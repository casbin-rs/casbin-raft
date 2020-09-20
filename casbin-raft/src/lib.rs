#[macro_use]
extern crate slog;

pub mod network;
pub mod node;

pub type StorageError = Box<dyn std::error::Error + Send + Sync + 'static>;

use raft::prelude::*;
pub use raft::storage::MemStorage;

pub trait Storage: raft::storage::Storage {
    fn append(&self, entries: &[Entry]) -> Result<(), StorageError>;
    fn apply_snapshot(&self, snapshot: Snapshot) -> Result<(), StorageError>;
    fn set_conf_state(&self, cs: ConfState);
    fn set_hard_state(&self, commit: u64, term: u64);
    fn hard_state(&self) -> HardState;
}

impl Storage for MemStorage {
    fn append(&self, entries: &[Entry]) -> Result<(), StorageError> {
        self.wl().append(entries)?;
        Ok(())
    }

    fn apply_snapshot(&self, snapshot: Snapshot) -> Result<(), StorageError> {
        self.wl().apply_snapshot(snapshot)?;
        Ok(())
    }

    fn set_conf_state(&self, cs: ConfState) {
        self.wl().set_conf_state(cs);
    }

    fn set_hard_state(&self, commit: u64, term: u64) {
        let mut me = self.wl();
        me.mut_hard_state().commit = commit;
        me.mut_hard_state().term = term;
    }

    fn hard_state(&self) -> HardState {
        let mut me = self.wl();
        me.mut_hard_state().clone()
    }
}

#[cfg(test)]
mod test {
    use crate::Snapshot;

    use super::MemStorage;

    fn new_snapshot(index: u64, term: u64, voters: Vec<u64>) -> Snapshot {
        let mut s = Snapshot::default();
        s.mut_metadata().index = index;
        s.mut_metadata().term = term;
        s.mut_metadata().mut_conf_state().voters = voters;
        s
    }

    #[test]
    fn test_storage_apply_snapshot() {
        let nodes = vec![1, 2, 3];
        let storage = MemStorage::new();

        // Apply snapshot successfully
        let snap = new_snapshot(4, 4, nodes.clone());
        assert!(storage.wl().apply_snapshot(snap).is_ok());

        // Apply snapshot fails due to StorageError::SnapshotOutOfDate
        let snap = new_snapshot(3, 3, nodes);
        assert!(storage.wl().apply_snapshot(snap).is_err());
    }
}
