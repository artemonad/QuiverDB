#![recursion_limit = "256"]

use anyhow::Result;

mod cli;
mod cmd_batch;
mod cmd_bloom;
mod cmd_checkpoint;
mod cmd_compact;
mod cmd_del;
mod cmd_doctor;
mod cmd_exists;
mod cmd_get;
mod cmd_init;
mod cmd_maint;
mod cmd_put;
mod cmd_scan;
mod cmd_status;
mod cmd_sweep;
mod cmd_tde; // TDE rotate command
mod cmd_vacuum;
mod util;
// NEW: CDC modules
mod cmd_cdc_apply;
mod cmd_cdc_ship;
// NEW: Snapshots (2.2)
mod cmd_snapshot;
// NEW: Snapshot restore (persisted)
mod cmd_snapshot_restore;

fn main() {
    if let Err(e) = run() {
        eprintln!("error: {:#}", e);
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = cli::Cli::parse();
    match cli.cmd {
        cli::Cmd::Init {
            path,
            page_size,
            buckets,
        } => cmd_init::exec(path, page_size, buckets),

        cli::Cmd::Put {
            path,
            key,
            value,
            value_file,
        } => cmd_put::exec(path, key, value, value_file),

        cli::Cmd::Get { path, key, out } => cmd_get::exec(path, key, out),

        cli::Cmd::Exists { path, key } => cmd_exists::exec(path, key),

        cli::Cmd::Del { path, key } => cmd_del::exec(path, key),

        cli::Cmd::Batch {
            path,
            ops_file,
            ops_json,
        } => cmd_batch::exec(path, ops_file, ops_json),

        cli::Cmd::Scan {
            path,
            prefix,
            json,
            stream,
        } => cmd_scan::exec(path, prefix, json, stream),

        // Status supports --json flag
        cli::Cmd::Status { path, json } => cmd_status::exec_with_json(path, json),

        cli::Cmd::Sweep { path } => cmd_sweep::exec(path),

        cli::Cmd::Doctor { path, json } => cmd_doctor::exec(path, json),

        cli::Cmd::Checkpoint { path } => cmd_checkpoint::exec(path),

        cli::Cmd::Compact { path, bucket, json } => cmd_compact::exec(path, bucket, json),

        cli::Cmd::Vacuum { path, json } => cmd_vacuum::exec(path, json),

        cli::Cmd::Bloom {
            path,
            bucket,
            bpb,
            k,
        } => cmd_bloom::exec(path, bucket, bpb, k),

        // TDE rotate
        cli::Cmd::TdeRotate { path, kid } => cmd_tde::exec_rotate(path, kid),

        // Auto maintenance
        cli::Cmd::AutoMaint {
            path,
            max_buckets,
            sweep,
            json,
        } => cmd_maint::exec(path, max_buckets, sweep, json),

        // NEW: CDC commands wiring
        cli::Cmd::CdcApply { path, from } => cmd_cdc_apply::exec(path, from),

        cli::Cmd::CdcShip {
            path,
            to,
            since_lsn,
        } => cmd_cdc_ship::exec(path, to, since_lsn),

        // NEW: Snapshots (2.2)
        cli::Cmd::SnapshotCreate {
            path,
            message,
            label,
            parent,
        } => cmd_snapshot::exec_create(path, message, label, parent),

        cli::Cmd::SnapshotList { path, json } => cmd_snapshot::exec_list(path, json),

        cli::Cmd::SnapshotInspect { path, id, json } => cmd_snapshot::exec_inspect(path, id, json),

        // NEW: Snapshot restore
        cli::Cmd::SnapshotRestore {
            path,
            src,
            id,
            verify,
        } => cmd_snapshot_restore::exec(path, src, id, verify),

        // NEW: Snapshot delete
        cli::Cmd::SnapshotDelete { path, id } => cmd_snapshot::exec_delete(path, id),
    }
}
