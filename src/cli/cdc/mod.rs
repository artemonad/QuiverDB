// src/cli/cdc/mod.rs

mod ship;
mod tail;
mod apply;
mod record;
mod replay;
mod last_lsn;

pub use ship::cmd_wal_ship_ext;
pub use tail::cmd_wal_tail;
pub use apply::{wal_apply_from_stream, cmd_wal_apply};
pub use record::cmd_cdc_record;
pub use replay::cmd_cdc_replay;
pub use last_lsn::cmd_cdc_last_lsn;