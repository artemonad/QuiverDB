use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use QuiverDB::{init_db, read_meta, set_clean_shutdown, Db, Directory};
use QuiverDB::consts::{DATA_SEG_EXT, DATA_SEG_PREFIX, WAL_FILE, WAL_HDR_SIZE};

// Генератор уникальных временных директорий для тестов
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let base = std::env::temp_dir();
    base.join(format!("p1test-{prefix}-{pid}-{t}-{id}"))
}

// Утилита: путь к первому сегменту
fn seg1_path(root: &PathBuf) -> PathBuf {
    root.join(format!("{}{:06}.{}", DATA_SEG_PREFIX, 1, DATA_SEG_EXT))
}

#[test]
fn init_and_meta_dir() {
    let root = unique_root("init");
    fs::create_dir_all(&root).expect("create root dir");
    init_db(&root, 4096).expect("init_db");

    // meta v3 и стабильный hash_kind присутствуют
    let meta = read_meta(&root).expect("read_meta");
    assert_eq!(meta.version, 3);
    assert_eq!(meta.page_size, 4096);
    assert!(meta.clean_shutdown);
    // last_lsn на старте 0
    assert_eq!(meta.last_lsn, 0);

    // создаём каталог и проверяем открытие
    let dir = Directory::create(&root, 64).expect("dir create");
    assert_eq!(dir.bucket_count, 64);
    let dir2 = Directory::open(&root).expect("dir open");
    assert_eq!(dir2.bucket_count, 64);

    // должны существовать wal и первый сегмент
    let wal = root.join(WAL_FILE);
    assert!(wal.exists(), "wal file should exist");
    let seg1 = seg1_path(&root);
    assert!(seg1.exists(), "segment 1 should exist");
}

#[test]
fn db_put_get_del_roundtrip() {
    let root = unique_root("kv");
    fs::create_dir_all(&root).expect("create root dir");
    init_db(&root, 4096).expect("init_db");
    Directory::create(&root, 128).expect("dir create");

    {
        let mut db = Db::open(&root).expect("db open");
        db.put(b"alpha", b"1").expect("put alpha");
        db.put(b"beta", b"2").expect("put beta");

        let v1 = db.get(b"alpha").expect("get alpha").expect("alpha exists");
        assert_eq!(v1, b"1");

        let v2 = db.get(b"beta").expect("get beta").expect("beta exists");
        assert_eq!(v2, b"2");

        let existed = db.del(b"alpha").expect("del alpha");
        assert!(existed, "alpha must be deleted");

        let miss = db.get(b"alpha").expect("get alpha after del");
        assert!(miss.is_none(), "alpha must be gone");
    }

    // повторное открытие и проверка beta
    {
        let db = Db::open(&root).expect("db reopen");
        let v2 = db.get(b"beta").expect("get beta").expect("beta exists");
        assert_eq!(v2, b"2");
    }
}

#[test]
fn wal_replay_recovers_missing_segment() {
    let root = unique_root("replay");
    fs::create_dir_all(&root).expect("create root dir");
    init_db(&root, 4096).expect("init_db");
    Directory::create(&root, 16).expect("dir create");

    // 1) Пишем запись (это создаст страницу 0, запишет WAL)
    {
        let mut db = Db::open(&root).expect("db open");
        db.put(b"foo", b"bar").expect("put foo");
        // Db::drop выставит clean_shutdown=true
    }

    // 2) Симулируем потерю сегмента (крэш до flush data, но WAL уже есть)
    let seg1 = seg1_path(&root);
    if seg1.exists() {
        fs::remove_file(&seg1).expect("remove seg1");
    }

    // Убедимся, что WAL содержит хоть что-то больше заголовка
    let wal = root.join(WAL_FILE);
    let wal_len = fs::metadata(&wal).expect("wal meta").len();
    assert!(
        wal_len >= WAL_HDR_SIZE as u64,
        "wal must exist and be at least header-sized"
    );

    // 3) Помечаем как 'нечистое завершение' перед открытием — реплей должен сработать
    set_clean_shutdown(&root, false).expect("force unclean shutdown flag");

    // 4) Открытие БД должно выполнить wal_replay и восстановить страницу
    {
        let db = Db::open(&root).expect("db reopen with replay");
        let v = db.get(b"foo").expect("get foo").expect("foo must exist");
        assert_eq!(v, b"bar");
    }

    // 5) WAL должен быть усечён до заголовка после успешного реплея
    let wal_len2 = fs::metadata(&wal).expect("wal meta2").len();
    assert_eq!(wal_len2, WAL_HDR_SIZE as u64, "wal must be truncated");
}

#[test]
fn exclusive_lock_prevents_second_writer() {
    use QuiverDB::{acquire_exclusive_lock, try_acquire_exclusive_lock};

    let root = unique_root("lock");
    fs::create_dir_all(&root).expect("create root dir");
    init_db(&root, 4096).expect("init_db");

    let g1 = acquire_exclusive_lock(&root).expect("acquire first lock");
    let second = try_acquire_exclusive_lock(&root);
    assert!(
        second.is_err(),
        "second try_acquire_exclusive_lock must fail while first is held"
    );
    drop(g1);

    // после освобождения — должно получиться
    let g2 = try_acquire_exclusive_lock(&root).expect("acquire after drop");
    drop(g2);
}

#[test]
fn clean_shutdown_flag_toggles_on_open_drop() {
    let root = unique_root("clean-flag");
    fs::create_dir_all(&root).expect("create root dir");
    init_db(&root, 4096).expect("init_db");
    Directory::create(&root, 8).expect("dir create");

    // До открытия writer'а — clean
    let m0 = read_meta(&root).expect("read meta initial");
    assert!(m0.clean_shutdown);

    // Во время жизни Db (после open) — dirty
    {
        let _db = Db::open(&root).expect("db open");
        let m1 = read_meta(&root).expect("read meta during db");
        assert!(!m1.clean_shutdown, "should be marked unclean while DB is open");
    }

    // После drop — вновь clean
    let m2 = read_meta(&root).expect("read meta after drop");
    assert!(m2.clean_shutdown, "should be clean after DB is dropped");
}