// tests/ro_readers.rs
//
// Проверяем семантику RO-читателей:
// - open_ro не меняет clean_shutdown;
// - shared-lock читателя блокирует эксклюзивный lock (try) для writer;
// - open_ro не делает WAL replay; writer делает replay и усечёт WAL.
//
// Запуск:
//   cargo test --test ro_readers -- --nocapture

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;

use QuiverDB::{init_db, read_meta, set_clean_shutdown, Db, Directory};
use QuiverDB::consts::{DATA_SEG_EXT, DATA_SEG_PREFIX, WAL_FILE, WAL_HDR_SIZE};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let base = std::env::temp_dir();
    base.join(format!("qdbtest-ro-{prefix}-{pid}-{t}-{id}"))
}

fn seg1_path(root: &PathBuf) -> PathBuf {
    root.join(format!("{}{:06}.{}", DATA_SEG_PREFIX, 1, DATA_SEG_EXT))
}

#[test]
fn ro_does_not_toggle_clean_flag() -> Result<()> {
    let root = unique_root("clean-flag");
    fs::create_dir_all(&root)?;
    init_db(&root, 4096)?;
    Directory::create(&root, 8)?;

    let m0 = read_meta(&root)?;
    assert!(m0.clean_shutdown, "initial meta must be clean");

    {
        // RO не должен менять clean_shutdown
        let _db = Db::open_ro(&root)?;
        let m1 = read_meta(&root)?;
        assert!(
            m1.clean_shutdown,
            "clean_shutdown must remain true while RO is open"
        );
    }

    // После drop RO — тоже без изменений
    let m2 = read_meta(&root)?;
    assert!(m2.clean_shutdown, "clean_shutdown must remain true after RO drop");
    Ok(())
}

#[test]
fn ro_shared_lock_blocks_exclusive_try() -> Result<()> {
    // Откроем RO (shared-lock)
    let root = unique_root("lock");
    fs::create_dir_all(&root)?;
    init_db(&root, 4096)?;
    Directory::create(&root, 8)?;

    let _db_ro = Db::open_ro(&root)?;

    // Попробуем взять эксклюзивный lock — должно упасть (try_* возвращает Err)
    let res = QuiverDB::try_acquire_exclusive_lock(&root);
    assert!(
        res.is_err(),
        "try_acquire_exclusive_lock must fail while RO (shared) is held"
    );

    Ok(())
}

#[test]
fn ro_does_not_replay_wal_writer_does() -> Result<()> {
    // Сценарий:
    // 1) Writer пишет ключ (создаёт страницу 0), drop -> clean=true.
    // 2) Симулируем крэш до записи data: удаляем seg1 и помечаем clean=false.
    // 3) RO-открытие не делает replay: страница не восстановится, WAL не усечён.
    // 4) Writer-открытие делает replay: страница восстановится, WAL усечён.

    std::env::set_var("P1_WAL_COALESCE_MS", "0");

    let root = unique_root("replay-ro");
    fs::create_dir_all(&root)?;
    init_db(&root, 4096)?;
    Directory::create(&root, 16)?;

    // 1) Writer: put
    {
        let mut db = Db::open(&root)?;
        db.put(b"foo", b"bar")?;
        // drop -> clean_shutdown=true
    }

    // Убедимся, что WAL есть и не пустой
    let wal_path = root.join(WAL_FILE);
    let wal_len1 = fs::metadata(&wal_path)?.len();
    assert!(
        wal_len1 >= WAL_HDR_SIZE as u64,
        "wal must exist and be >= header"
    );

    // 2) Симулируем потерю сегмента 1 и пометим unclean shutdown
    let seg1 = seg1_path(&root);
    if seg1.exists() {
        fs::remove_file(&seg1)?;
    }
    set_clean_shutdown(&root, false)?;

    // 3) RO-открытие: не делает replay
    {
        let db_ro = Db::open_ro(&root)?;
        // Попытка чтения может вернуть Err (нет сегмента) — это ожидаемо.
        let res = db_ro.get(b"foo");
        assert!(
            res.is_err() || res.unwrap().is_none(),
            "RO must not restore data; get(foo) should fail or be None"
        );

        // WAL не должен быть усечён в RO-пути
        let wal_len_ro = fs::metadata(&wal_path)?.len();
        assert_eq!(
            wal_len_ro, wal_len1,
            "RO must not truncate WAL (no replay in RO)"
        );
    }

    // 4) Writer-открытие: выполнит wal_replay_if_any, восстановит страницу, усечёт WAL
    {
        let db = Db::open(&root)?;
        let v = db.get(b"foo")?.expect("foo must exist after writer replay");
        assert_eq!(v, b"bar");
    }

    // WAL усечён
    let wal_len2 = fs::metadata(&wal_path)?.len();
    assert_eq!(wal_len2, WAL_HDR_SIZE as u64, "WAL must be truncated by writer replay");

    Ok(())
}