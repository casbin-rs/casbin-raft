use std::collections::HashMap;
use std::fmt::Write;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use prost::Message;
use raft::prelude::*;
use raft::{Config, RawNode};
use slog::Logger;
use tokio::sync::mpsc::*;
use tokio::time::*;
use tonic::transport::ClientTlsConfig;
use tonic::Request;

use casbin_raft_proto::{self, InternalRaftMessage, PolicyRequestType, RaftRequest};
use casbin_raft_types::*;

use crate::network::{create_client, RpcClient};
use crate::{MemStorage, Storage};

pub struct CasbinRaft<D>
where
    D: DispatcherHandle,
{
    pub id: u64,
    pub node: RawNode<MemStorage>,
    pub logger: Logger,
    pub mailbox_sender: Sender<casbin_raft_proto::Message>,
    pub mailbox_recv: Receiver<casbin_raft_proto::Message>,
    pub conf_sender: Sender<ConfChange>,
    pub conf_recv: Receiver<ConfChange>,
    pub peers: HashMap<u64, RpcClient>,
    pub heartbeat: usize,
    pub dispatcher: Arc<D>,
}

impl<D> CasbinRaft<D>
where
    D: DispatcherHandle,
{
    pub fn new(
        id: u64,
        cfg: Config,
        logger: Logger,
        peers: HashMap<u64, RpcClient>,
        mailbox_sender: Sender<casbin_raft_proto::Message>,
        mailbox_recv: Receiver<casbin_raft_proto::Message>,
        dispatcher: Arc<D>,
    ) -> Result<Self, crate::StorageError> {
        cfg.validate()?;

        let storage = MemStorage::new();
        let node = RawNode::new(&cfg, storage, &logger)?;
        let (conf_sender, conf_recv) = channel(1024);

        Ok(Self {
            id,
            node,
            logger: logger.clone(),
            mailbox_sender,
            mailbox_recv,
            conf_sender,
            conf_recv,
            heartbeat: cfg.heartbeat_tick,
            peers,
            dispatcher,
        })
    }

    pub fn tick(&mut self) -> bool {
        self.node.tick()
    }

    pub fn propose_conf_change(
        &mut self,
        context: Vec<u8>,
        cc: ConfChange,
    ) -> Result<(), raft::Error> {
        Ok(self.node.propose_conf_change(context, cc)?)
    }

    pub fn become_leader(&mut self) {
        self.node.raft.raft_log.committed = 0;
        self.node.raft.become_candidate();
        self.node.raft.become_leader();
    }

    fn set_hard_state(&mut self, commit: u64, term: u64) -> Result<(), raft::Error> {
        self.node.raft.mut_store().set_hard_state(commit, term);
        Ok(())
    }

    #[allow(irrefutable_let_patterns)]
    pub async fn run(
        mut self,
        client_tls_config: Option<ClientTlsConfig>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        while let _ = interval(Duration::from_millis(self.heartbeat as u64))
            .tick()
            .await
        {
            let msg = match timeout(Duration::from_millis(100), self.mailbox_recv.recv())
                .await
            {
                Ok(Some(msg)) => Some(msg),
                Ok(None) => None,
                Err(_) => None,
            };

            if let Some(msg) = msg {
                slog::info!(self.logger, "Inbound raft message: {:?}", msg);
                self.node.step(msg.into())?;
            }

            match timeout(Duration::from_millis(100), self.conf_recv.recv()).await {
                Ok(Some(cc)) => {
                    let ccc = cc.clone();
                    let state = self.node.apply_conf_change(&cc)?;

                    self.node.mut_store().set_conf_state(state);
                    let mut p = self.peers.clone();
                    let logger = self.logger.clone();
                    let client_tls_config = client_tls_config.clone();
                    tokio::spawn(async move {
                        let mut uri = String::new();
                        for a in ccc.context[..].iter() {
                            //println!(" N: {:x?}", a);
                            //signature_string.push(a);
                            write!(uri, "{:02x}", a).unwrap();
                        }
                        let client: RpcClient = create_client(
                            uri.clone(),
                            Some(logger.clone()),
                            client_tls_config,
                        )
                        .await
                        .unwrap();
                        p.insert(ccc.node_id, client);
                        slog::info!(
                            logger,
                            "Added client: {:?} - {:?}",
                            ccc.node_id,
                            &uri
                        );
                    });
                }
                Ok(None) => (),
                Err(_) => (),
            };

            if self.node.has_ready() {
                slog::info!(self.logger, "I'm ready!");
                self.ready().await?;
            }
            self.node.tick();
        }

        Ok(())
    }

    pub async fn ready(
        &mut self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        let mut ready = self.node.ready();

        let is_leader = self.node.raft.leader_id == self.node.raft.id;
        slog::info!(
            self.logger,
            "Leader ID: {}, Node ID: {}",
            self.node.raft.leader_id,
            self.node.raft.id
        );
        slog::info!(self.logger, "Am I leader?: {}", is_leader);

        if !Snapshot::is_empty(ready.snapshot()) {
            let snap = ready.snapshot().clone();
            slog::info!(self.logger, "Got a snap: {:?}", snap);
            self.node.mut_store().apply_snapshot(snap)?;
        }

        if !ready.entries().is_empty() {
            let entries = ready
                .entries()
                .iter()
                .cloned()
                .filter(|e| !e.get_data().is_empty())
                .collect::<Vec<Entry>>();
            slog::info!(self.logger, "Entries?: {}", entries.len());
            self.node.mut_store().append(&entries)?;
        }

        if let Some(hs) = ready.hs() {
            slog::info!(self.logger, "HS?: {:?}", hs);
            self.node.mut_store().set_hard_state(hs.commit, hs.term);
            // self.node.mut_store().state.hard_state = (*hs).clone();
            // self.node.mut_store().commit()?;
        }

        for mut msg in ready.messages.drain(..) {
            slog::info!(self.logger, "LOGMSG==={:?}", msg);
            let to = msg.to;
            msg.from = self.id;

            msg.log_term = self.node.store().hard_state().term;
            msg.commit = self.node.store().hard_state().commit;
            if let Some(client) = self.peers.get(&to) {
                let mut msg_bytes = vec![];
                msg.encode(&mut msg_bytes).unwrap();
                let req = Request::new(RaftRequest {
                    tpe: 0,
                    message: msg_bytes,
                });
                let req = client.clone().raft(req).await?;
                slog::info!(self.logger, "RESP={:?}", req);
            }
            self.append_entries(&msg.entries).await?;
        }

        if let Some(committed_entries) = ready.committed_entries.take() {
            for entry in committed_entries.clone() {
                slog::info!(self.logger, "Committing: {:?}", entry);
                if entry.data.is_empty() {
                    // From new elected leaders.
                    continue;
                }

                let mut internal_raft_message = InternalRaftMessage::default();
                internal_raft_message
                    .merge(Bytes::from(entry.data.clone()))
                    .unwrap();

                if let Err(error) = self.apply(internal_raft_message).await {
                    slog::error!(self.logger, "Unable to apply entry. {:?}", error);
                    // TODO: return an error to the user
                }
            }

            if let Some(entry) = committed_entries.last() {
                self.set_hard_state(entry.index, entry.term)?;
            }
        }

        self.node.advance(ready);
        Ok(())
    }

    pub fn propose(
        &mut self,
        ctx: Vec<u8>,
        entry: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Ok(self.node.propose(ctx, entry)?)
    }

    pub async fn send(
        &mut self,
        msg: casbin_raft_proto::Message,
    ) -> Result<(), crate::StorageError> {
        slog::info!(self.logger, "SEND = {:?}", msg);
        self.mailbox_sender.send(msg).await.unwrap();
        Ok(())
    }

    pub async fn append_entries(
        &mut self,
        entries: &[Entry],
    ) -> Result<(), crate::StorageError> {
        for entry in entries {
            if entry.data.is_empty() {
                continue;
            }
            slog::info!(self.logger, "APPEND={:?}", entry);

            match EntryType::from_i32(entry.entry_type) {
                Some(EntryType::EntryConfChange) => {
                    let mut cc = ConfChange::default();
                    cc.merge(Bytes::from(entry.data.clone()))?;

                    let cs = self.node.apply_conf_change(&cc)?;
                    self.node.mut_store().set_conf_state(cs);
                }
                Some(EntryType::EntryNormal) => {
                    let mut e = Entry::default();

                    e.merge(Bytes::from(entry.data.clone()))?;

                    self.node.mut_store().append(&[e])?;
                }
                Some(EntryType::EntryConfChangeV2) => panic!("Conf2"),
                None => panic!(":-("),
            }
        }
        Ok(())
    }

    pub async fn apply(
        &mut self,
        request: InternalRaftMessage,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let dispatcher = Arc::clone(&self.dispatcher);
        let mut enf = dispatcher.get_dispatcher()?;
        if let Some(inner) = request.policy {
            let op = PolicyRequestType::from_i32(inner.op);
            match op {
                Some(PolicyRequestType::AddPolicy) => {
                    let params = inner.params;
                    enf.add_policy(params).await?;
                }
                Some(PolicyRequestType::RemovePolicy) => {
                    let params = inner.params;
                    enf.remove_policy(params).await?;
                }
                Some(PolicyRequestType::AddPolicies) => {
                    let paramss = inner.paramss.into_iter().map(|x| x.param).collect();
                    enf.add_policies(paramss).await?;
                }
                Some(PolicyRequestType::RemovePolicies) => {
                    let paramss = inner.paramss.into_iter().map(|x| x.param).collect();
                    enf.remove_policies(paramss).await?;
                }
                Some(PolicyRequestType::RemoveFilteredPolicy) => {
                    let field_index = inner.field_index as usize;
                    let field_values = inner.field_values;
                    enf.remove_filtered_policy(field_index, field_values)
                        .await?;
                }
                Some(PolicyRequestType::ClearPolicy) => {
                    enf.clear_policy();
                }
                None => panic!(":-("),
            }
        }

        Ok(())
    }
}
