use anyhow::{Context, Result};
use std::path::PathBuf;

use QuiverDB::db::vacuum::VacuumSummary;
use QuiverDB::db::Db;

/// CLI: vacuum — комбинированная операция обслуживания:
/// 1) Компактация всех бакетов (tail-wins без tombstone/expired)
/// 2) Очистка сиротских OVERFLOW-страниц
///
/// Требует writer (эксклюзивный lock). Вывод — текст/JSON.
pub fn exec(path: PathBuf, json: bool) -> Result<()> {
    let mut db =
        Db::open(&path).with_context(|| format!("open writer DB at {}", path.display()))?;

    let sum: VacuumSummary = db
        .vacuum_all()
        .with_context(|| "vacuum_all (compaction + sweep orphan overflow)")?;

    if json {
        println!(
            "{{\
                \"buckets_total\":{},\
                \"buckets_compacted\":{},\
                \"old_chain_len_sum\":{},\
                \"keys_kept_sum\":{},\
                \"keys_deleted_sum\":{},\
                \"pages_written_sum\":{},\
                \"overflow_pages_freed\":{}\
            }}",
            sum.compaction.buckets_total,
            sum.compaction.buckets_compacted,
            sum.compaction.old_chain_len_sum,
            sum.compaction.keys_kept_sum,
            sum.compaction.keys_deleted_sum,
            sum.compaction.pages_written_sum,
            sum.overflow_pages_freed
        );
        return Ok(());
    }

    println!("Vacuum summary:");
    println!("  buckets_total        = {}", sum.compaction.buckets_total);
    println!(
        "  buckets_compacted    = {}",
        sum.compaction.buckets_compacted
    );
    println!(
        "  old_chain_len_sum    = {}",
        sum.compaction.old_chain_len_sum
    );
    println!("  keys_kept_sum        = {}", sum.compaction.keys_kept_sum);
    println!(
        "  keys_deleted_sum     = {}",
        sum.compaction.keys_deleted_sum
    );
    println!(
        "  pages_written_sum    = {}",
        sum.compaction.pages_written_sum
    );
    println!("  overflow_pages_freed = {}", sum.overflow_pages_freed);

    Ok(())
}
