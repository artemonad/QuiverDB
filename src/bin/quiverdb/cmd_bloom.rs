use anyhow::{Context, Result};
use std::path::PathBuf;

use QuiverDB::bloom::BloomSidecar;
use QuiverDB::db::Db;

/// CLI: bloom rebuild
/// - Если указан --bucket, перестраивает только один бакет (частичный ребилд; Bloom не будет “свежим” для fast-path).
/// - Иначе перестраивает все бакеты и обновляет last_lsn (Bloom становится “свежим” для fast-path).
/// - Если bloom.bin отсутствует — создаётся с указанными параметрами bpb/k.
pub fn exec(
    path: PathBuf,
    bucket: Option<u32>,
    bytes_per_bucket: Option<u32>,
    k_hashes: Option<u32>,
) -> Result<()> {
    // Bloom — side-car: достаточно RO-открытия базы (shared lock).
    let db_ro = Db::open_ro(&path).with_context(|| format!("open RO DB at {}", path.display()))?;

    let bpb = bytes_per_bucket.unwrap_or(4096);
    let k = k_hashes.unwrap_or(6);

    // Важно: sidecar нужен mut для rebuild_*.
    let mut sidecar = BloomSidecar::open_or_create_for_db(&db_ro, bpb, k)
        .with_context(|| "open_or_create bloom.bin sidecar")?;

    match bucket {
        Some(b) => {
            // Частичный ребилд: не обновляем last_lsn в заголовке, иначе можно получить ложные отрицания на других бакетах.
            sidecar
                .rebuild_bucket(&db_ro, b)
                .with_context(|| format!("rebuild bloom for bucket {}", b))?;
            println!(
                "bloom: rebuilt bucket {} (bpb={}, k={}) — partial rebuild (fast-path disabled until full rebuild)",
                b, bpb, k
            );
        }
        None => {
            // Полный ребилд: после завершения заголовок получает current last_lsn, фильтр становится “свежим”.
            sidecar
                .rebuild_all(&db_ro)
                .with_context(|| "rebuild bloom for all buckets")?;
            println!("bloom: rebuilt all buckets (bpb={}, k={})", bpb, k);
        }
    }

    Ok(())
}
