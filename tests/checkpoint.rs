use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;

use QuiverDB::{init_db, Db, Directory};
use QuiverDB::consts::{WAL_FILE, WAL_HDR_SIZE};
use QuiverDB::wal::Wal;

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("qdbtest-checkpoint-{prefix}-{pid}-{t}-{id}"))
}

#[test]
fn checkpoint_truncate_to_header() -> Result<()> {
    // Deterministic WAL timing
    std::env::set_var("P1_WAL_COALESCE_MS", "0");
    // data_fsync оставляем по умолчанию (true), чтобы поведение соответствовало безопасному чекпоинту

    // Prepare DB
    let root = unique_root("basic");
    fs::create_dir_all(&root)?;
    init_db(&root, 4096)?;
    Directory::create(&root, 64)?;

    // 1) Write a record -> WAL should grow beyond header
    {
        let mut db = Db::open(&root)?;
        db.put(b"a", b"1")?;
        // drop -> clean_shutdown=true
    }
    let wal_path = root.join(WAL_FILE);
    let len1 = fs::metadata(&wal_path)?.len();
    assert!(
        len1 > WAL_HDR_SIZE as u64,
        "WAL must be larger than header after a write; got {}",
        len1
    );

    // 2) Manual checkpoint: truncate WAL to header
    {
        let mut wal = Wal::open_for_append(&root)?;
        wal.truncate_to_header()?;
    }
    let len2 = fs::metadata(&wal_path)?.len();
    assert_eq!(
        len2,
        WAL_HDR_SIZE as u64,
        "WAL must be truncated to header after checkpoint"
    );

    // 3) Ensure DB remains writable after checkpoint
    {
        let mut db = Db::open(&root)?;
        db.put(b"b", b"2")?;
    }
    let len3 = fs::metadata(&wal_path)?.len();
    assert!(
        len3 > WAL_HDR_SIZE as u64,
        "WAL must grow again after subsequent writes; got {}",
        len3
    );

    // Cleanup ENV for other tests
    std::env::remove_var("P1_WAL_COALESCE_MS");
    Ok(())
}