use anyhow::{Context, Result};
use std::path::PathBuf;

use QuiverDB::db::Db;

/// CLI: auto-maintenance — компактация ограниченного числа бакетов + опциональный sweep сиротских OVERFLOW.
///
/// Примеры:
///   quiverdb auto-maint --path ./db --max-buckets 32 --sweep
///   quiverdb auto-maint --path ./db --max-buckets 16 --json
pub fn exec(path: PathBuf, max_buckets: u32, do_sweep: bool, json: bool) -> Result<()> {
    let mut db =
        Db::open(&path).with_context(|| format!("open writer DB at {}", path.display()))?;

    let sum = db
        .auto_maintenance(max_buckets, do_sweep)
        .with_context(|| "auto_maintenance")?;

    if json {
        println!(
            "{{\
                \"buckets_scanned\":{},\
                \"buckets_compacted\":{},\
                \"pages_written_sum\":{},\
                \"overflow_pages_freed\":{}\
            }}",
            sum.buckets_scanned,
            sum.buckets_compacted,
            sum.pages_written_sum,
            sum.overflow_pages_freed
        );
        return Ok(());
    }

    println!("Auto-maintenance:");
    println!("  buckets_scanned        = {}", sum.buckets_scanned);
    println!("  buckets_compacted      = {}", sum.buckets_compacted);
    println!("  pages_written_sum      = {}", sum.pages_written_sum);
    if do_sweep {
        println!("  overflow_pages_freed   = {}", sum.overflow_pages_freed);
    } else {
        println!("  overflow_pages_freed   = (sweep disabled)");
    }

    Ok(())
}
