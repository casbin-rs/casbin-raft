use std::convert::TryFrom;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use casbin::prelude::{CoreApi, Enforcer, MgmtApi};
use dashmap::DashMap;
use http::Uri;
use prost::Message;
use raft::prelude::*;
use raft::{Config, RawNode};
use slog::Logger;
use tokio::sync::mpsc::*;
use tokio::sync::RwLock;
use tokio::time::*;
use tonic::Request;

use crate::cluster::{self, InternalRaftMessage, PolicyRequestType, RaftRequest};
use crate::network::{create_client, RpcClient};
use crate::storage::{MemStorage, Storage};

pub struct CasbinRaft {
    pub id: u64,
    pub node: RawNode<MemStorage>,
    pub logger: Logger,
    pub mailbox_sender: Sender<cluster::Message>,
    pub mailbox_recv: Receiver<cluster::Message>,
    pub conf_sender: Sender<ConfChange>,
    pub conf_recv: Receiver<ConfChange>,
    pub peers: Arc<DashMap<u64, RpcClient>>,
    pub heartbeat: usize,
    pub enforcer: Arc<RwLock<Enforcer>>,
}

impl CasbinRaft {
    pub fn new(
        id: u64,
        cfg: Config,
        logger: Logger,
        peers: Arc<DashMap<u64, RpcClient>>,
        mailbox_sender: Sender<cluster::Message>,
        mailbox_recv: Receiver<cluster::Message>,
        enforcer: Arc<RwLock<Enforcer>>,
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
            enforcer,
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
                    let p = self.peers.clone();
                    let logger = self.logger.clone();
                    tokio::spawn(async move {
                        let uri = Uri::try_from(&ccc.context[..]).unwrap();
                        let client: RpcClient =
                            create_client(uri.clone(), Some(logger.clone()))
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

                if let Err(error) = self.apply(internal_raft_message) {
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
        msg: cluster::Message,
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

    pub fn apply(
        &mut self,
        request: InternalRaftMessage,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(policy_request) = request.policy {
            let op = PolicyRequestType::from_i32(policy_request.op);
            match op {
                Some(PolicyRequestType::AddPolicy) => {
                    let cloned_enforcer = self.enforcer.clone();
                    let policy = policy_request.params;
                    Box::pin(async move {
                        let mut lock = cloned_enforcer.write().await;
                        lock.add_policy(policy).await.unwrap();
                    });
                }
                Some(PolicyRequestType::RemovePolicy) => {
                    let cloned_enforcer = self.enforcer.clone();
                    let policy = policy_request.params;
                    Box::pin(async move {
                        let mut lock = cloned_enforcer.write().await;
                        lock.remove_policy(policy).await.unwrap();
                    });
                }
                Some(PolicyRequestType::AddPolicies) => {
                    let cloned_enforcer = self.enforcer.clone();
                    let policy = policy_request
                        .paramss
                        .into_iter()
                        .map(|x| x.param)
                        .collect();
                    Box::pin(async move {
                        let mut lock = cloned_enforcer.write().await;
                        lock.add_policies(policy).await.unwrap();
                    });
                }
                Some(PolicyRequestType::RemovePolicies) => {
                    let cloned_enforcer = self.enforcer.clone();
                    let policy = policy_request
                        .paramss
                        .into_iter()
                        .map(|x| x.param)
                        .collect();
                    Box::pin(async move {
                        let mut lock = cloned_enforcer.write().await;
                        lock.remove_policies(policy).await.unwrap();
                    });
                }
                Some(PolicyRequestType::RemoveFilteredPolicy) => {
                    let cloned_enforcer = self.enforcer.clone();
                    let field_index = policy_request.field_index;
                    let field_values = policy_request.field_values;
                    Box::pin(async move {
                        let mut lock = cloned_enforcer.write().await;
                        lock.remove_filtered_policy(field_index as usize, field_values)
                            .await
                            .unwrap();
                    });
                }
                Some(PolicyRequestType::ClearPolicy) => {
                    let cloned_enforcer = self.enforcer.clone();
                    Box::pin(async move {
                        let mut lock = cloned_enforcer.write().await;
                        lock.clear_policy().await.unwrap();
                    });
                }
                None => panic!(":-("),
            }
        }

        Ok(())
    }
}
