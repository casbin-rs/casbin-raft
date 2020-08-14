use casbin::Error as CasbinErrors;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use thiserror::Error;
use tonic::{Code, Status};

#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorResponse {
    /// The human-readable message given back
    pub message: String,
}

impl ErrorResponse {
    pub fn new<M: ToString>(message: M) -> Self {
        Self {
            message: message.to_string(),
        }
    }
}

/// Casbin-Raft's base error types
#[derive(Debug, Error)]
pub enum Error {
    /// IO error that deals with anything related to reading from disk or network communications
    #[error("IO Error: {0}")]
    IOError(String),
    /// Any error that related to Casbin's model, adapter and so all.
    #[error(transparent)]
    CasbinError(#[from] CasbinErrors),
    /// This should never occur and is a bug that should be reported
    #[error("Failed to find known executor")]
    SpawnError,
    /// oOoOOoOOOoOOo Spooooky ghosts, maybe, we don't know.
    #[error("An unknown error occurred")]
    UnknownError,
    /// This should never occur and is a bug that should be reported
    #[error("Thread pool is poisoned")]
    PoisonedError,
    /// An error occured in Casbin-Raft's internal RPC communications
    #[error("An RPC error occurred: '{0}'")]
    RPCError(String),
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::IOError(e.to_string())
    }
}

impl From<slog::Error> for Error {
    fn from(e: slog::Error) -> Self {
        Error::IOError(e.to_string())
    }
}

impl From<Error> for tonic::Status {
    fn from(e: Error) -> Self {
        Status::new(Code::Internal, format!("{:?}", e))
    }
}

impl From<tonic::Status> for Error {
    fn from(s: tonic::Status) -> Self {
        Error::RPCError(s.to_string())
    }
}

impl From<tonic::transport::Error> for Error {
    fn from(e: tonic::transport::Error) -> Self {
        Error::RPCError(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn is_send<T: Send>() -> bool {
        true
    }

    fn is_sync<T: Sync>() -> bool {
        true
    }

    #[test]
    fn test_send_sync() {
        assert!(is_send::<Error>());
        assert!(is_sync::<Error>());
    }
}
