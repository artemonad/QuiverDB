use std::fs;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use QuiverDB::{init_db, read_meta};
use QuiverDB::consts::{
    DATA_SEG_EXT, DATA_SEG_PREFIX, PAGE_HDR_V2_SIZE, WAL_FILE, WAL_HDR_SIZE,
};
use QuiverDB::dir::Directory;
use QuiverDB::db::Db;
use QuiverDB::meta::set_clean_shutdown;
use QuiverDB::pager::Pager;
use QuiverDB::page_rh::{
    rh_compact_inplace, rh_header_read, rh_header_write, rh_kv_delete_inplace, rh_kv_insert,
    rh_kv_lookup, rh_page_init, rh_page_update_crc,
};
use QuiverDB::wal::{Wal, wal_replay_if_any};

// ---------- helpers ----------

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let base = std::env::temp_dir();
    base.join(format!("qdbtest-{prefix}-{pid}-{t}-{id}"))
}

fn seg_path(root: &PathBuf, seg_no: u64) -> PathBuf {
    root.join(format!("{}{:06}.{}", DATA_SEG_PREFIX, seg_no, DATA_SEG_EXT))
}

// ---------- tests ----------

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
    assert_eq!(meta.last_lsn, 0);

    // создаём каталог и проверяем открытие
    let dir = Directory::create(&root, 64).expect("dir create");
    assert_eq!(dir.bucket_count, 64);
    let dir2 = Directory::open(&root).expect("dir open");
    assert_eq!(dir2.bucket_count, 64);

    // должны существовать wal и первый сегмент
    let wal = root.join(WAL_FILE);
    assert!(wal.exists(), "wal file should exist");
    let seg1 = seg_path(&root, 1);
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

    // 1) Пишем запись (создаст страницу 0, запишет WAL)
    {
        let mut db = Db::open(&root).expect("db open");
        db.put(b"foo", b"bar").expect("put foo");
        // Drop db -> clean_shutdown=true
    }

    // 2) Симулируем потерю сегмента
    let seg1 = seg_path(&root, 1);
    if seg1.exists() {
        fs::remove_file(&seg1).expect("remove seg1");
    }

    // Убедимся, что WAL больше заголовка
    let wal = root.join(WAL_FILE);
    let wal_len = fs::metadata(&wal).expect("wal meta").len();
    assert!(wal_len >= WAL_HDR_SIZE as u64);

    // 3) Форсим «нечистый старт» и реплей
    set_clean_shutdown(&root, false).expect("unclean flag");
    let db = Db::open(&root).expect("db reopen with replay");
    let v = db.get(b"foo").expect("get foo").expect("foo exists");
    assert_eq!(v, b"bar");

    // 4) WAL должен быть усечён
    let wal_len2 = fs::metadata(&wal).expect("wal meta2").len();
    assert_eq!(wal_len2, WAL_HDR_SIZE as u64);
}

#[test]
fn clean_shutdown_flag_toggles_on_open_drop() {
    let root = unique_root("clean-flag");
    fs::create_dir_all(&root).expect("create root dir");
    init_db(&root, 4096).expect("init_db");
    Directory::create(&root, 8).expect("dir create");

    // До writer'а — clean
    let m0 = read_meta(&root).expect("meta0");
    assert!(m0.clean_shutdown);

    // Во время жизни Db — dirty
    {
        let _db = Db::open(&root).expect("db open");
        let m1 = read_meta(&root).expect("meta1");
        assert!(!m1.clean_shutdown);
    }

    // После drop — clean
    let m2 = read_meta(&root).expect("meta2");
    assert!(m2.clean_shutdown);
}

#[test]
fn wal_replay_lsn_gating_v2_preserves_latest() {
    let root = unique_root("lsn-gate-v2");
    fs::create_dir_all(&root).expect("create root dir");
    init_db(&root, 4096).expect("init_db");

    let mut pager = Pager::open(&root).expect("pager open");
    let page_id = 0u64;
    pager.ensure_allocated(page_id).expect("ensure alloc");
    let ps = pager.meta.page_size as usize;

    // 1) Инициализируем v2 (LSN=1)
    let mut buf = vec![0u8; ps];
    rh_page_init(&mut buf, page_id).expect("rh init");
    pager.commit_page(page_id, &mut buf).expect("commit v2 init");

    // 2) Вставляем новое значение (LSN=2)
    let meta = read_meta(&root).expect("read meta");
    let k = b"foo";
    let v_new = b"bar2";
    rh_kv_insert(&mut buf, meta.hash_kind, k, v_new).expect("insert v2");
    pager.commit_page(page_id, &mut buf).expect("commit v2 put");

    // На диске — новое значение
    let mut cur = vec![0u8; ps];
    pager.read_page(page_id, &mut cur).expect("read page");
    let got = rh_kv_lookup(&cur, meta.hash_kind, k)
        .expect("lookup ok")
        .expect("key exists");
    assert_eq!(got, v_new);

    // 3) Запишем старую запись WAL (LSN=1) с пустой страницей
    let mut payload_old = vec![0u8; ps];
    rh_page_init(&mut payload_old, page_id).expect("rh init payload");
    {
        let mut h = rh_header_read(&payload_old).expect("hdr read");
        h.lsn = 1;
        rh_header_write(&mut payload_old, &h).expect("hdr write");
        rh_page_update_crc(&mut payload_old).expect("crc update");
    }
    let mut wal = Wal::open_for_append(&root).expect("open wal");
    wal.append_page_image(1, page_id, &payload_old)
        .expect("append old rec");
    wal.fsync().expect("wal fsync");

    // 4) Реплей не должен откатить страницу
    set_clean_shutdown(&root, false).expect("unclean");
    wal_replay_if_any(&root).expect("replay");

    let mut cur2 = vec![0u8; ps];
    let pager2 = Pager::open(&root).expect("pager reopen");
    pager2.read_page(page_id, &mut cur2).expect("read page 2");
    let got2 = rh_kv_lookup(&cur2, meta.hash_kind, k)
        .expect("lookup ok")
        .expect("key exists");
    assert_eq!(got2, v_new);

    // WAL усечён
    let wal_path = root.join(WAL_FILE);
    let len = fs::metadata(&wal_path).expect("wal meta").len();
    assert_eq!(len, WAL_HDR_SIZE as u64);
}

#[test]
fn crc_mismatch_detected_on_read_v2() {
    let root = unique_root("crc-mismatch-v2");
    fs::create_dir_all(&root).expect("create root dir");
    init_db(&root, 4096).expect("init_db");

    let mut pager = Pager::open(&root).expect("pager open");
    let page_id = 0u64;
    pager.ensure_allocated(page_id).expect("ensure alloc");
    let ps = pager.meta.page_size as usize;

    // Инициализируем страницу и записываем (CRC установлен)
    let mut buf = vec![0u8; ps];
    rh_page_init(&mut buf, page_id).expect("v2 init");
    pager.commit_page(page_id, &mut buf).expect("commit v2");

    // Портим один байт за пределами заголовка
    let seg1 = seg_path(&root, 1);
    let mut f = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&seg1)
        .expect("open seg1");
    let off_in_seg = 0u64 + (PAGE_HDR_V2_SIZE as u64);
    f.seek(SeekFrom::Start(off_in_seg)).expect("seek");
    let mut b = [0u8; 1];
    f.read_exact(&mut b).expect("read one");
    b[0] ^= 0xFF;
    f.seek(SeekFrom::Start(off_in_seg)).expect("seek2");
    f.write_all(&b).expect("write one");
    f.sync_all().expect("sync seg");

    // Чтение должно упасть по CRC mismatch
    let mut buf2 = vec![0u8; ps];
    let err = pager.read_page(page_id, &mut buf2).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("CRC"), "expected CRC error, got: {msg}");
}

#[test]
fn rh_resize_lookup_delete_compact() {
    let page_size = 4096usize;
    let mut buf = vec![0u8; page_size];
    let page_id = 42u64; // seed only
    rh_page_init(&mut buf, page_id).expect("init v2");

    // Вставляем много ключей — таблица должна увеличиваться
    let kind = read_meta(&unique_root("dummy")) // fake path can't read; use default
        .err()
        .map(|_| QuiverDB::hash::HASH_KIND_DEFAULT)
        .unwrap();
    let mut inserted = Vec::new();

    for i in 0..200usize {
        let k = format!("k{:05}", i);
        let v = format!("v{:03}", i % 100);
        let ok = rh_kv_insert(&mut buf, kind, k.as_bytes(), v.as_bytes()).expect("insert");
        if ok {
            inserted.push((k, v));
        } else {
            break;
        }
    }

    assert!(
        inserted.len() >= 64,
        "expected to insert at least 64 records, got {}",
        inserted.len()
    );

    // Выборочные lookup'ы
    for (k, v) in inserted.iter().step_by(7).take(10) {
        let got = rh_kv_lookup(&buf, kind, k.as_bytes())
            .expect("lookup ok")
            .expect("must exist");
        assert_eq!(got, v.as_bytes(), "lookup must match");
    }

    // Удалим каждую вторую, затем compact
    for (idx, (k, _v)) in inserted.iter().enumerate() {
        if idx % 2 == 1 {
            let existed = rh_kv_delete_inplace(&mut buf, kind, k.as_bytes()).expect("delete");
            assert!(existed, "deleted key must exist");
        }
    }

    rh_compact_inplace(&mut buf, kind).expect("compact");

    for (idx, (k, v)) in inserted.iter().enumerate() {
        let got = rh_kv_lookup(&buf, kind, k.as_bytes()).expect("lookup ok");
        if idx % 2 == 1 {
            assert!(got.is_none(), "deleted key must be gone");
        } else {
            assert_eq!(got.unwrap(), v.as_bytes(), "kept key must match");
        }
    }
}