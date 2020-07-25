use casbin::Error as CasbinErrors;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use thiserror::Error;

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
#[derive(Debug, Error, Serialize, Deserialize)]
pub enum Error {
    /// IO error that deals with anything related to reading from disk or network communications
    #[error("IO Error: {0}")]
    IOError(String),
    /// Any error that related to Casbin's model, adapter and so all.
    #[error("CasbinError: {0}")]
    CasbinError(String),
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

impl From<CasbinErrors> for Error {
    fn from(e: CasbinErrors) -> Self {
        match e {
            CasbinErrors::IoError(e) => Error::IOError(e.to_string()),
            CasbinErrors::ModelError(e) => Error::CasbinError(e.to_string()),
            CasbinErrors::PolicyError(e) => Error::CasbinError(e.to_string()),
            CasbinErrors::RbacError(e) => Error::CasbinError(e.to_string()),
            CasbinErrors::RhaiError(e) => Error::CasbinError(e.to_string()),
            CasbinErrors::RhaiParseError(e) => Error::CasbinError(e.to_string()),
            CasbinErrors::RequestError(e) => Error::CasbinError(e.to_string()),
            CasbinErrors::AdapterError(e) => Error::CasbinError(e.to_string()),
        }
    }
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
