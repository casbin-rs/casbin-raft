use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use slog::Logger;

use prost::Message;

use tokio::sync::mpsc::*;
use tonic::transport::{self, Channel, ClientTlsConfig, Server, ServerTlsConfig};
use tonic::{Code, Request, Response, Status};

use casbin_raft_proto::*;
use casbin_raft_types::*;

pub type RpcClient = client::CasbinServiceClient<Channel>;

pub async fn create_client(
    uri: String,
    logger: Option<Logger>,
    tls_config: Option<ClientTlsConfig>,
) -> Result<RpcClient, transport::Error> {
    if let Some(log) = logger {
        slog::info!(log, "Creating Client to: {:?}", uri);
    }

    if let Some(tls) = tls_config {
        let channel = Channel::from_shared(uri)
            .unwrap()
            .tls_config(tls)?
            .connect()
            .await?;

        let rpc_client = client::CasbinServiceClient::new(channel);
        return Ok(rpc_client);
    }

    client::CasbinServiceClient::connect(uri.to_string())
        .await
        .map_err(Into::into)
}

pub struct RpcServer<D>
where
    D: DispatcherHandle,
{
    logger: Logger,
    raft_chan: Sender<casbin_raft_proto::Message>,
    raft_conf: Sender<raft::prelude::ConfChange>,
    dispatcher: Arc<D>,
}

impl<D> RpcServer<D>
where
    D: DispatcherHandle,
{
    pub async fn serve(
        addr: SocketAddr,
        logger: Logger,
        server_tls_config: Option<ServerTlsConfig>,
        raft_chan: Sender<casbin_raft_proto::Message>,
        raft_conf: Sender<raft::prelude::ConfChange>,
        dispatcher: Arc<D>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        let service = server::CasbinServiceServer::new(RpcServer {
            dispatcher,
            logger: logger.clone(),
            raft_chan,
            raft_conf,
        });

        if let Some(tls) = server_tls_config {
            return Ok(Server::builder()
                .tls_config(tls)?
                .add_service(service)
                .serve(addr)
                .await?);
        }

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
impl<D> server::CasbinService for RpcServer<D>
where
    D: DispatcherHandle,
{
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
        let msg: casbin_raft_proto::Message =
            Message::decode(Bytes::from(message)).unwrap();
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

    async fn add_policy(
        &self,
        request: Request<PolicyRequest>,
    ) -> Result<Response<Empty>, Status> {
        info!(self.logger, "Request From: {:?}", request);
        let inner = request.into_inner();
        let dispatcher = Arc::clone(&self.dispatcher);
        if let Ok(mut enf) = dispatcher.get_dispatcher() {
            let params = inner.params;
            enf.add_policy(params).await.unwrap();
            Ok(Response::new(Empty {}))
        } else {
            Self::error_response(Code::NotFound, "Could not find enforcer".into())
        }
    }

    async fn remove_policy(
        &self,
        request: Request<PolicyRequest>,
    ) -> Result<Response<Empty>, Status> {
        info!(self.logger, "Request From: {:?}", request);
        let inner = request.into_inner();
        let dispatcher = Arc::clone(&self.dispatcher);
        if let Ok(mut enf) = dispatcher.get_dispatcher() {
            let params = inner.params;
            enf.remove_policy(params).await.unwrap();
            Ok(Response::new(Empty {}))
        } else {
            Self::error_response(Code::NotFound, "Could not find enforcer".into())
        }
    }

    async fn add_policies(
        &self,
        request: Request<PolicyRequest>,
    ) -> Result<Response<Empty>, Status> {
        info!(self.logger, "Request From: {:?}", request);
        let inner = request.into_inner();
        let dispatcher = Arc::clone(&self.dispatcher);
        if let Ok(mut enf) = dispatcher.get_dispatcher() {
            let paramss = inner.paramss.into_iter().map(|x| x.param).collect();
            enf.add_policies(paramss).await.unwrap();
            Ok(Response::new(Empty {}))
        } else {
            Self::error_response(Code::NotFound, "Could not find enforcer".into())
        }
    }

    async fn remove_policies(
        &self,
        request: Request<PolicyRequest>,
    ) -> Result<Response<Empty>, Status> {
        info!(self.logger, "Request From: {:?}", request);
        let inner = request.into_inner();
        let dispatcher = Arc::clone(&self.dispatcher);
        if let Ok(mut enf) = dispatcher.get_dispatcher() {
            let paramss = inner.paramss.into_iter().map(|x| x.param).collect();
            enf.remove_policies(paramss).await.unwrap();
            Ok(Response::new(Empty {}))
        } else {
            Self::error_response(Code::NotFound, "Could not find enforcer".into())
        }
    }

    async fn remove_filtered_policy(
        &self,
        request: Request<PolicyRequest>,
    ) -> Result<Response<Empty>, Status> {
        info!(self.logger, "Request From: {:?}", request);
        let inner = request.into_inner();
        let dispatcher = Arc::clone(&self.dispatcher);
        if let Ok(mut enf) = dispatcher.get_dispatcher() {
            let field_index = inner.field_index as usize;
            let field_values = inner.field_values;
            enf.remove_filtered_policy(field_index, field_values)
                .await
                .unwrap();
            Ok(Response::new(Empty {}))
        } else {
            Self::error_response(Code::NotFound, "Could not find enforcer".into())
        }
    }

    async fn clear_policy(
        &self,
        request: Request<PolicyRequest>,
    ) -> Result<Response<Empty>, Status> {
        info!(self.logger, "Request From: {:?}", request);
        let dispatcher = Arc::clone(&self.dispatcher);
        if let Ok(mut enf) = dispatcher.get_dispatcher() {
            enf.clear_policy();
            Ok(Response::new(Empty {}))
        } else {
            Self::error_response(Code::NotFound, "Could not find enforcer".into())
        }
    }
}
