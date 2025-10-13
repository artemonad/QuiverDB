use anyhow::Result;

mod cli;
mod util;
mod cmd_init;
mod cmd_put;
mod cmd_get;
mod cmd_exists;
mod cmd_del;
mod cmd_batch;
mod cmd_scan;
mod cmd_status;
mod cmd_sweep;
mod cmd_doctor;
mod cmd_checkpoint;
mod cmd_compact;
mod cmd_vacuum;
mod cmd_bloom;
mod cmd_tde;    // TDE rotate command
mod cmd_maint;
// NEW: CDC modules
mod cmd_cdc_ship;
mod cmd_cdc_apply;

fn main() {
    if let Err(e) = run() {
        eprintln!("error: {:#}", e);
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = cli::Cli::parse();
    match cli.cmd {
        cli::Cmd::Init { path, page_size, buckets } =>
            cmd_init::exec(path, page_size, buckets),

        cli::Cmd::Put { path, key, value, value_file } =>
            cmd_put::exec(path, key, value, value_file),

        cli::Cmd::Get { path, key, out } =>
            cmd_get::exec(path, key, out),

        cli::Cmd::Exists { path, key } =>
            cmd_exists::exec(path, key),

        cli::Cmd::Del { path, key } =>
            cmd_del::exec(path, key),

        cli::Cmd::Batch { path, ops_file, ops_json } =>
            cmd_batch::exec(path, ops_file, ops_json),

        cli::Cmd::Scan { path, prefix, json, stream } =>
            cmd_scan::exec(path, prefix, json, stream),

        // Status supports --json flag
        cli::Cmd::Status { path, json } =>
            cmd_status::exec_with_json(path, json),

        cli::Cmd::Sweep { path } =>
            cmd_sweep::exec(path),

        cli::Cmd::Doctor { path, json } =>
            cmd_doctor::exec(path, json),

        cli::Cmd::Checkpoint { path } =>
            cmd_checkpoint::exec(path),

        cli::Cmd::Compact { path, bucket, json } =>
            cmd_compact::exec(path, bucket, json),

        cli::Cmd::Vacuum { path, json } =>
            cmd_vacuum::exec(path, json),

        cli::Cmd::Bloom { path, bucket, bpb, k } =>
            cmd_bloom::exec(path, bucket, bpb, k),

        // TDE rotate
        cli::Cmd::TdeRotate { path, kid } =>
            cmd_tde::exec_rotate(path, kid),

        // Auto maintenance
        cli::Cmd::AutoMaint { path, max_buckets, sweep, json } =>
            cmd_maint::exec(path, max_buckets, sweep, json),

        // NEW: CDC commands wiring
        cli::Cmd::CdcApply { path, from } =>
            cmd_cdc_apply::exec(path, from),

        cli::Cmd::CdcShip { path, to, since_lsn } =>
            cmd_cdc_ship::exec(path, to, since_lsn),
    }
}