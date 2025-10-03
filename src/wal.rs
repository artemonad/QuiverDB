//! WAL module: split into writer (append/group-commit) and replay (startup recovery).
//!
//! Public API remains the same:
//! - Wal, WalGroupCfg, write_wal_file_header
//! - wal_replay_if_any

mod writer;
mod replay;

pub use writer::{Wal, WalGroupCfg, write_wal_file_header};
pub use replay::wal_replay_if_any;