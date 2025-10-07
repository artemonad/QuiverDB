use std::fs;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use byteorder::{ByteOrder, LittleEndian};

use QuiverDB::{init_db, read_meta, Db, Directory};
use QuiverDB::snapshots::store::SnapStore;

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

// Global lock to serialize tests that mutate ENV/shared globals.
static TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("qdbtest-snapdir-{prefix}-{pid}-{t}-{id}"))
}

/// Read all hashes from <sidecar>/hashindex.bin
fn read_hashes_from_hashindex(sidecar_dir: &PathBuf) -> Result<Vec<u64>> {
    let path = sidecar_dir.join("hashindex.bin");
    let mut out = Vec::new();
    if !path.exists() {
        return Ok(out);
    }
    let mut f = OpenOptions::new().read(true).open(&path)?;
    let len = f.metadata()?.len();
    const REC: u64 = 8 + 8 + 8; // [page_id u64][hash u64][page_lsn u64]
    let mut pos = 0u64;
    let mut buf = vec![0u8; REC as usize];
    while pos + REC <= len {
        f.seek(SeekFrom::Start(pos))?;
        f.read_exact(&mut buf)?;
        let hash = LittleEndian::read_u64(&buf[8..16]);
        out.push(hash);
        pos += REC;
    }
    Ok(out)
}

#[test]
fn snapstore_respects_relative_dir_env() -> Result<()> {
    let _g = TEST_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();

    // Clean ENV to start from a known state
    std::env::remove_var("P1_SNAPSTORE_DIR");
    std::env::remove_var("P1_SNAP_DEDUP");
    std::env::remove_var("P1_SNAP_PERSIST");
    std::env::remove_var("P1_WAL_COALESCE_MS");

    // Deterministic WAL timing
    std::env::set_var("P1_WAL_COALESCE_MS", "0");
    // Enable Phase 2 features
    std::env::set_var("P1_SNAP_PERSIST", "1");
    std::env::set_var("P1_SNAP_DEDUP", "1");

    // Relative SnapStore dir under DB root
    let rel_dir = "snapdir_rel";
    std::env::set_var("P1_SNAPSTORE_DIR", rel_dir);

    // DB root
    let root = unique_root("rel");
    fs::create_dir_all(&root)?;
    init_db(&root, 4096)?;
    Directory::create(&root, 64)?;

    // Write initial value
    {
        let mut db = Db::open(&root)?;
        db.put(b"k", b"v0")?;
    }

    // Begin snapshot and update value to trigger freeze
    let mut db = Db::open(&root)?;
    let mut s = db.snapshot_begin()?;
    db.put(b"k", b"v1")?;

    // Sidecar path
    let sidecar = root.join(".snapshots").join(&s.id);
    assert!(sidecar.exists(), "sidecar must exist");

    // Open SnapStore and verify directory selection
    let ps = read_meta(&root)?.page_size;
    let ss = SnapStore::open(&root, ps)?;
    let actual_dir = ss.dir_path().to_path_buf();
    let expected_dir = root.join(rel_dir);
    assert!(
        actual_dir.exists(),
        "snapstore dir returned by SnapStore must exist: {}",
        actual_dir.display()
    );
    assert_eq!(
        actual_dir, expected_dir,
        "snapstore dir must equal <root>/<rel_dir>"
    );

    // Read hashes from sidecar and verify we can get payload from SnapStore
    let hashes = read_hashes_from_hashindex(&sidecar)?;
    assert!(!hashes.is_empty(), "hashindex.bin must contain at least one entry");

    let mut found = false;
    for h in hashes {
        if let Some(payload) = ss.get(h)? {
            assert_eq!(payload.len() as u32, ps, "payload size must match page_size");
            found = true;
            break;
        }
    }
    assert!(found, "at least one frozen frame must be readable from SnapStore");

    // Clean up snapshot
    db.snapshot_end(&mut s)?;

    // Cleanup ENV for other tests
    std::env::remove_var("P1_SNAPSTORE_DIR");
    std::env::remove_var("P1_SNAP_DEDUP");
    std::env::remove_var("P1_SNAP_PERSIST");
    std::env::remove_var("P1_WAL_COALESCE_MS");
    Ok(())
}

#[test]
fn snapstore_respects_absolute_dir_env() -> Result<()> {
    let _g = TEST_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();

    // Clean ENV to start from a known state
    std::env::remove_var("P1_SNAPSTORE_DIR");
    std::env::remove_var("P1_SNAP_DEDUP");
    std::env::remove_var("P1_SNAP_PERSIST");
    std::env::remove_var("P1_WAL_COALESCE_MS");

    // Deterministic WAL timing
    std::env::set_var("P1_WAL_COALESCE_MS", "0");
    // Enable Phase 2 features
    std::env::set_var("P1_SNAP_PERSIST", "1");
    std::env::set_var("P1_SNAP_DEDUP", "1");

    // DB root
    let root = unique_root("abs");
    fs::create_dir_all(&root)?;
    init_db(&root, 4096)?;
    Directory::create(&root, 64)?;

    // Absolute SnapStore dir (unique per DB root)
    let abs_snap_dir = root.join("snapdir_abs");
    std::env::set_var("P1_SNAPSTORE_DIR", abs_snap_dir.display().to_string());

    // Write initial value
    {
        let mut db = Db::open(&root)?;
        db.put(b"ka", b"v0")?;
    }

    // Begin snapshot and update value to trigger freeze
    let mut db = Db::open(&root)?;
    let mut s = db.snapshot_begin()?;
    db.put(b"ka", b"v1")?;

    // Sidecar path
    let sidecar = root.join(".snapshots").join(&s.id);
    assert!(sidecar.exists(), "sidecar must exist");

    // The absolute SnapStore dir should exist
    assert!(
        abs_snap_dir.exists(),
        "absolute SnapStore dir must exist: {}",
        abs_snap_dir.display()
    );

    // Read hashes from sidecar and verify we can get payload from SnapStore
    let hashes = read_hashes_from_hashindex(&sidecar)?;
    assert!(!hashes.is_empty(), "hashindex.bin must contain at least one entry");

    let ps = read_meta(&root)?.page_size;
    let ss = SnapStore::open(&root, ps)?;
    let mut found = false;
    for h in hashes {
        if let Some(payload) = ss.get(h)? {
            assert_eq!(payload.len() as u32, ps, "payload size must match page_size");
            found = true;
            break;
        }
    }
    assert!(found, "at least one frozen frame must be readable from SnapStore");

    // Run compact (smoke)
    let mut ss2 = SnapStore::open(&root, ps)?;
    let _ = ss2.compact()?;

    // Clean up snapshot
    db.snapshot_end(&mut s)?;

    // Cleanup ENV for other tests
    std::env::remove_var("P1_SNAPSTORE_DIR");
    std::env::remove_var("P1_SNAP_DEDUP");
    std::env::remove_var("P1_SNAP_PERSIST");
    std::env::remove_var("P1_WAL_COALESCE_MS");
    Ok(())
}