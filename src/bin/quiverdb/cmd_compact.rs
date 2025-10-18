use anyhow::{Context, Result};
use std::path::PathBuf;

use QuiverDB::db::compaction::{CompactBucketReport, CompactSummary};
use QuiverDB::db::Db;

/// CLI: compact
/// - Если указан --bucket, компактуем один бакет.
/// - Иначе — всю БД.
/// - --json управляет форматом вывода.
pub fn exec(path: PathBuf, bucket: Option<u32>, json: bool) -> Result<()> {
    // Компактация — операция записи: нужен writer (эксклюзивный lock).
    let mut db =
        Db::open(&path).with_context(|| format!("open writer DB at {}", path.display()))?;

    if let Some(b) = bucket {
        let rep = db
            .compact_bucket(b)
            .with_context(|| format!("compact bucket {}", b))?;
        print_bucket_report(&rep, json);
    } else {
        let sum = db
            .compact_all()
            .with_context(|| format!("compact all buckets at {}", path.display()))?;
        print_summary(&sum, json);
    }

    Ok(())
}

fn print_bucket_report(rep: &CompactBucketReport, json: bool) {
    if json {
        println!(
            "{{\
                \"bucket\":{},\
                \"old_chain_len\":{},\
                \"keys_kept\":{},\
                \"keys_deleted\":{},\
                \"pages_written\":{},\
                \"new_head\":{}\
            }}",
            rep.bucket,
            rep.old_chain_len,
            rep.keys_kept,
            rep.keys_deleted,
            rep.pages_written,
            rep.new_head
        );
        return;
    }

    println!("Compaction (bucket {}):", rep.bucket);
    println!("  old_chain_len  = {}", rep.old_chain_len);
    println!("  keys_kept      = {}", rep.keys_kept);
    println!("  keys_deleted   = {}", rep.keys_deleted);
    println!("  pages_written  = {}", rep.pages_written);
    println!("  new_head       = {}", rep.new_head);
}

fn print_summary(sum: &CompactSummary, json: bool) {
    if json {
        println!(
            "{{\
                \"buckets_total\":{},\
                \"buckets_compacted\":{},\
                \"old_chain_len_sum\":{},\
                \"keys_kept_sum\":{},\
                \"keys_deleted_sum\":{},\
                \"pages_written_sum\":{}\
            }}",
            sum.buckets_total,
            sum.buckets_compacted,
            sum.old_chain_len_sum,
            sum.keys_kept_sum,
            sum.keys_deleted_sum,
            sum.pages_written_sum
        );
        return;
    }

    println!("Compaction summary:");
    println!("  buckets_total      = {}", sum.buckets_total);
    println!("  buckets_compacted  = {}", sum.buckets_compacted);
    println!("  old_chain_len_sum  = {}", sum.old_chain_len_sum);
    println!("  keys_kept_sum      = {}", sum.keys_kept_sum);
    println!("  keys_deleted_sum   = {}", sum.keys_deleted_sum);
    println!("  pages_written_sum  = {}", sum.pages_written_sum);
}
