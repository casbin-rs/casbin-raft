use casbin::prelude::Enforcer;

type Result<T> = std::result::Result<T, crate::error::Error>;

/// Determines whether or not the enforcer is local to this machine or if the handle has to go to another
/// node in order to get it's data.
#[derive(Debug)]
pub enum EnforcerLocation {
    /// This enforcer is in local storage on this node
    LOCAL,
    /// Casbin-Raft has to make a request to another server for this enforcer
    REMOTE,
}

/// Defines an interface on how operations are done on enforcers inside Casbin-Raft
#[async_trait::async_trait]
pub trait Dispatcher: Clone {
    async fn add_policy(&mut self, params: Vec<String>) -> Result<bool>;
    async fn remove_policy(&mut self, params: Vec<String>) -> Result<bool>;
    async fn add_policies(&mut self, paramss: Vec<Vec<String>>) -> Result<bool>;
    async fn remove_policies(&mut self, paramss: Vec<Vec<String>>) -> Result<bool>;
    async fn remove_filtered_policy(
        &mut self,
        field_index: usize,
        field_values: Vec<String>,
    ) -> Result<bool>;
    fn clear_policy(&mut self);
    fn set_enforcer(&self, enforcer: Enforcer) -> Result<()>;
}

/// Defines the interface for obtaining a handle from a dispatcher to an enforcer
#[async_trait::async_trait]
pub trait DispatcherHandle: Send + Sync + 'static {
    type Local: Dispatcher + Send + Sync;
    type Remote: Dispatcher + Send + Sync;

    /// Return a handle to a single enforcer
    fn get_enforcer(&self) -> Result<Self::Local>;
    /// Determine if an enforcer exists locally
    fn exists(&self) -> bool;
    /// Return a handle to a single remote enforcer
    async fn get_remote_enforcer(&self, id: u64) -> Result<Self::Remote>;
    /// Determine if an enforcer exists remotely
    async fn remote_exists(&self, id: u64) -> bool;

    fn raft_id(&self) -> u64;
}
