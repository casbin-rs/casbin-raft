use raft::prelude::*;
pub use raft::storage::MemStorage;

pub trait Storage: raft::storage::Storage {
    fn append(&self, entries: &[Entry]) -> Result<(), crate::Error>;
    fn apply_snapshot(&self, snapshot: Snapshot) -> Result<(), crate::Error>;
    fn set_conf_state(&self, cs: ConfState);
    fn set_hard_state(&self, commit: u64, term: u64);
    fn hard_state(&self) -> HardState;
}

impl Storage for MemStorage {
    fn append(&self, entries: &[Entry]) -> Result<(), crate::Error> {
        self.wl().append(entries)?;
        Ok(())
    }

    fn apply_snapshot(&self, snapshot: Snapshot) -> Result<(), crate::Error> {
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
