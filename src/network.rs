use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use http::Uri;
use slog::Logger;

use casbin::prelude::{CoreApi, Enforcer, MgmtApi, TryIntoAdapter, TryIntoModel};

use prost::Message;

use tokio::sync::mpsc::*;
use tokio::sync::RwLock;

use tonic::transport::{self, Channel, Server};
use tonic::{Code, Request, Response, Status};

use crate::cluster;
use crate::cluster::*;
use crate::error::Error;

pub type RpcClient = client::CasbinServiceClient<Channel>;

pub async fn create_from_managed<M: TryIntoModel, A: TryIntoAdapter>(
    m: M,
    a: A,
) -> Result<Enforcer, Error> {
    Enforcer::new(m, a).await.map_err(Error::CasbinError)
}

pub async fn create_client(
    uri: Uri,
    logger: Option<Logger>,
) -> Result<RpcClient, transport::Error> {
    if let Some(log) = logger {
        slog::info!(log, "Creating Client to: {:?}", uri);
    }
    client::CasbinServiceClient::connect(uri.to_string())
        .await
        .map_err(Into::into)
}

pub struct RpcServer {
    logger: Logger,
    raft_chan: Sender<cluster::Message>,
    raft_conf: Sender<raft::prelude::ConfChange>,
    enforcer: Arc<RwLock<Enforcer>>,
}

impl RpcServer {
    pub async fn serve(
        addr: SocketAddr,
        logger: Logger,
        raft_chan: Sender<cluster::Message>,
        raft_conf: Sender<raft::prelude::ConfChange>,
        enforcer: Arc<RwLock<Enforcer>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        let service = server::CasbinServiceServer::new(RpcServer {
            enforcer,
            logger: logger.clone(),
            raft_chan,
            raft_conf,
        });

        Ok(Server::builder().add_service(service).serve(addr).await?)
    }

    pub fn ok_result() -> ResultReply {
        Self::create_result(0, "".into())
    }

    pub fn create_result(code: i32, message: String) -> ResultReply {
        ResultReply { code, message }
    }

    pub fn error_response<T>(code: Code, msg: String) -> Result<Response<T>, Status> {
        let status = Status::new(code, msg);
        Err(status)
    }
}

#[async_trait::async_trait]
impl server::CasbinService for RpcServer {
    async fn ping(
        &self,
        _: Request<PingRequest>,
    ) -> Result<Response<PingReply>, Status> {
        Ok(Response::new(PingReply {
            status: "OK".into(),
        }))
    }

    async fn raft(
        &self,
        request: Request<RaftRequest>,
    ) -> Result<Response<RaftReply>, Status> {
        let RaftRequest { message, .. } = request.into_inner();
        let msg: cluster::Message = Message::decode(Bytes::from(message)).unwrap();
        slog::debug!(self.logger, "MSG = {:?}", msg);
        let mut chan = self.raft_chan.clone();
        if let Err(err) = chan.send(msg).await {
            panic!("Send Error: {:?}", err);
        }
        let response = Response::new(RaftReply { code: 0 });
        Ok(response)
    }

    async fn join(
        &self,
        request: Request<JoinRequest>,
    ) -> Result<Response<ResultReply>, Status> {
        let JoinRequest { host, id } = request.into_inner();
        let conf = raft::prelude::ConfChange {
            id,
            change_type: 0,
            node_id: id,
            context: host.as_bytes().to_vec(),
        };
        slog::debug!(self.logger, "CONF = {:?}", conf);
        let mut chan = self.raft_conf.clone();
        if let Err(err) = chan.send(conf).await {
            panic!("Send Error: {:?}", err);
        }

        let response = Response::new(ResultReply::default());
        Ok(response)
    }

    async fn add_policies(
        &self,
        request: Request<PolicyRequest>,
    ) -> Result<Response<Empty>, Status> {
        let cloned_enforcer = self.enforcer.clone();
        let cloned_request = request.into_inner();
        let p_type = "p".to_string();
        let policy = cloned_request
            .paramss
            .into_iter()
            .map(|x| x.param)
            .collect();
        Box::pin(async move {
            let mut lock = cloned_enforcer.write().await;
            lock.add_named_policies(&p_type, policy).await.unwrap();
        });
        let reply = Empty {};
        Ok(Response::new(reply))
    }

    async fn remove_policies(
        &self,
        request: Request<PolicyRequest>,
    ) -> Result<Response<Empty>, Status> {
        let cloned_enforcer = self.enforcer.clone();
        let policy = request
            .into_inner()
            .paramss
            .into_iter()
            .map(|x| x.param)
            .collect();
        Box::pin(async move {
            let mut lock = cloned_enforcer.write().await;
            lock.remove_policies(policy).await.unwrap();
        });
        let reply = Empty {};
        Ok(Response::new(reply))
    }

    async fn remove_filtered_policy(
        &self,
        request: Request<PolicyRequest>,
    ) -> Result<Response<Empty>, Status> {
        let cloned_enforcer = self.enforcer.clone();
        let policy_request = request.into_inner();
        let field_index = policy_request.field_index;
        let field_values = policy_request.field_values;
        Box::pin(async move {
            let mut lock = cloned_enforcer.write().await;
            lock.remove_filtered_policy(field_index as usize, field_values)
                .await
                .unwrap();
        });
        let reply = Empty {};
        Ok(Response::new(reply))
    }

    async fn clear_policy(
        &self,
        _request: Request<PolicyRequest>,
    ) -> Result<Response<Empty>, Status> {
        let cloned_enforcer = self.enforcer.clone();
        Box::pin(async move {
            let mut lock = cloned_enforcer.write().await;
            lock.clear_policy();
        });
        let reply = Empty {};
        Ok(Response::new(reply))
    }
}
