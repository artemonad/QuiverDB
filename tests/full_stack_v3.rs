use std::fs;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use QuiverDB::{init_db, read_meta};
use QuiverDB::consts::{DATA_SEG_EXT, DATA_SEG_PREFIX, PAGE_HDR_V2_SIZE, WAL_FILE, WAL_HDR_SIZE};
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
    base.join(format!("p1test-{prefix}-{pid}-{t}-{id}"))
}

fn seg_path(root: &PathBuf, seg_no: u64) -> PathBuf {
    root.join(format!("{}{:06}.{}", DATA_SEG_PREFIX, seg_no, DATA_SEG_EXT))
}

// ---------- tests ----------

#[test]
fn wal_replay_lsn_gating_v2_preserves_latest() {
    // Инициализация
    let root = unique_root("lsn-gate-v2");
    fs::create_dir_all(&root).expect("create root dir");
    init_db(&root, 4096).expect("init_db");

    let mut pager = Pager::open(&root).expect("pager open");
    let page_id = 0u64;
    pager.ensure_allocated(page_id).expect("ensure alloc");
    let ps = pager.meta.page_size as usize;

    // 1) Форматируем страницу v2 и коммитим (LSN=1)
    let mut buf = vec![0u8; ps];
    rh_page_init(&mut buf, page_id).expect("rh init");
    pager.commit_page(page_id, &mut buf).expect("commit v2 init");

    // 2) Вставляем ключ и коммитим (LSN=2)
    let meta = read_meta(&root).expect("read meta");
    let k = b"foo";
    let v_new = b"bar2";
    rh_kv_insert(&mut buf, meta.hash_kind, k, v_new).expect("insert v2");
    pager.commit_page(page_id, &mut buf).expect("commit v2 put");

    // Проверка: значение на диске действительно новое
    let mut cur = vec![0u8; ps];
    pager.read_page(page_id, &mut cur).expect("read page");
    let got = rh_kv_lookup(&cur, meta.hash_kind, k)
        .expect("lookup ok")
        .expect("key exists");
    assert_eq!(got, v_new, "value must be the latest before replay");

    // 3) Сформируем старую запись WAL с меньшим LSN (1) и пустой страницей как payload.
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
        .expect("append old wal rec");
    wal.fsync().expect("wal fsync");

    // 4) Помечаем незавершённый запуск и запускаем реплей — старая запись не должна примениться.
    set_clean_shutdown(&root, false).expect("mark unclean");
    wal_replay_if_any(&root).expect("replay");

    // 5) Проверяем, что на странице осталось новое значение, а WAL усечён до заголовка.
    let mut cur2 = vec![0u8; ps];
    let pager2 = Pager::open(&root).expect("pager reopen");
    pager2.read_page(page_id, &mut cur2).expect("read page after replay");
    let got2 = rh_kv_lookup(&cur2, meta.hash_kind, k)
        .expect("lookup ok")
        .expect("key exists");
    assert_eq!(
        got2, v_new,
        "replay must NOT overwrite newer page with older WAL record"
    );

    let wal_path = root.join(WAL_FILE);
    let len = fs::metadata(&wal_path).expect("wal meta").len();
    assert_eq!(len, WAL_HDR_SIZE as u64, "wal must be truncated to header");
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

    // Инициализируем страницу v2 и коммитим (CRC записан)
    let mut buf = vec![0u8; ps];
    rh_page_init(&mut buf, page_id).expect("v2 init");
    pager.commit_page(page_id, &mut buf).expect("commit v2");

    // Портим один байт данных страницы (не в поле CRC) прямо в сегменте
    let seg1 = seg_path(&root, 1);
    let mut f = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&seg1)
        .expect("open seg1");
    // Смещение: начало страницы + байт за пределами заголовка (чтобы точно сломать CRC)
    let off_in_seg = 0u64 + (PAGE_HDR_V2_SIZE as u64);
    f.seek(SeekFrom::Start(off_in_seg)).expect("seek");
    let mut b = [0u8; 1];
    f.read_exact(&mut b).expect("read one");
    b[0] ^= 0xFF;
    f.seek(SeekFrom::Start(off_in_seg)).expect("seek2");
    f.write_all(&b).expect("write one");
    f.sync_all().expect("sync seg");

    // Теперь чтение страницы должно провалиться по CRC mismatch.
    let mut buf2 = vec![0u8; ps];
    let err = pager.read_page(page_id, &mut buf2).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("CRC mismatch") || msg.contains("CRC"),
        "expected CRC error, got: {msg}"
    );
}

#[test]
fn rh_resize_lookup_delete_compact() {
    let page_size = 4096usize;
    let mut buf = vec![0u8; page_size];
    let page_id = 42u64; // любой id, только для seed
    rh_page_init(&mut buf, page_id).expect("init v2");

    // Будем вставлять ключи, чтобы принудить рост таблицы.
    let kind = QuiverDB::hash::HASH_KIND_DEFAULT;
    let mut inserted = Vec::new();

    for i in 0..200usize {
        let k = format!("k{:05}", i);
        let v = format!("v{:03}", i % 100);
        let ok = rh_kv_insert(&mut buf, kind, k.as_bytes(), v.as_bytes()).expect("insert");
        if ok {
            inserted.push((k, v));
        } else {
            // Страница переполнилась — ок, хватит.
            break;
        }
    }

    // Должны уметь вставить хотя бы несколько десятков записей.
    assert!(
        inserted.len() >= 64,
        "expected to insert at least 64 records, got {}",
        inserted.len()
    );

    // Проверим несколько выборочных lookup'ов.
    for (k, v) in inserted.iter().step_by(7).take(10) {
        let got = rh_kv_lookup(&buf, kind, k.as_bytes())
            .expect("lookup ok")
            .expect("must exist");
        assert_eq!(got, v.as_bytes(), "lookup must return inserted value");
    }

    // Удалим половину ключей (каждый второй)
    for (idx, (k, _v)) in inserted.iter().enumerate() {
        if idx % 2 == 1 {
            let existed = rh_kv_delete_inplace(&mut buf, kind, k.as_bytes()).expect("delete");
            assert!(existed, "deleted key must exist");
        }
    }

    // Компактим страницу и проверяем, что оставшиеся ключи на месте.
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