#[macro_use]
extern crate slog;

pub mod cluster;
pub mod error;
pub mod logger;
pub mod network;
pub mod node;
pub mod storage;

pub type StorageError = Box<dyn std::error::Error + Send + Sync + 'static>;
