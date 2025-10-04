// tests/metrics_and_wal_v0_9.rs
//
// Покрываем:
// - Metrics: snapshot/reset и базовые инкременты после операций.
// - WAL replay с неизвестным типом записи (должен игнорироваться, не ломая реплей).
// - P1_DATA_FSYNC=0: WAL не трюнкатится при коммите (truncate делается только на replay).
//
// Запуск:
//   cargo test --test metrics_and_wal_v0_9 -- --nocapture

use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use byteorder::{ByteOrder, LittleEndian};
use crc32fast::Hasher as Crc32;

use QuiverDB::{init_db, read_meta, set_clean_shutdown, Db, Directory};
use QuiverDB::consts::{
    WAL_FILE, WAL_HDR_SIZE, WAL_MAGIC, WAL_REC_HDR_SIZE, WAL_REC_OFF_CRC32, WAL_REC_OFF_LEN,
    WAL_REC_OFF_LSN, WAL_REC_OFF_PAGE_ID, WAL_REC_PAGE_IMAGE,
};
use QuiverDB::page_rh::{rh_page_init, rh_header_read, rh_header_write, rh_page_update_crc};
use QuiverDB::pager::Pager;

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let base = std::env::temp_dir();
    base.join(format!("qdbtest-metrics-{prefix}-{pid}-{t}-{id}"))
}

// -------- Test 1: metrics snapshot/reset и базовые инкременты --------

#[test]
fn metrics_snapshot_and_reset() -> Result<()> {
    std::env::set_var("P1_WAL_COALESCE_MS", "0");

    // Сброс метрик
    QuiverDB::metrics::reset();

    let root = unique_root("m1");
    fs::create_dir_all(&root)?;
    init_db(&root, 4096)?;
    Directory::create(&root, 64)?;

    {
        let mut db = Db::open(&root)?;
        db.put(b"a", b"1")?;
        db.put(b"b", b"2")?;
        let _ = db.get(b"a")?;
        let _ = db.del(b"b")?;
        // drop -> clean_shutdown=true
    }

    let m = QuiverDB::metrics::snapshot();
    assert!(m.wal_appends_total >= 2, "wal_appends_total should increase");
    assert!(m.wal_fsync_calls >= 1, "wal_fsync_calls should be >= 1");
    // cache по умолчанию выключен — не проверяем hit/miss значения

    // reset
    QuiverDB::metrics::reset();
    let z = QuiverDB::metrics::snapshot();
    assert_eq!(z.wal_appends_total, 0);
    assert_eq!(z.wal_fsync_calls, 0);
    assert_eq!(z.page_cache_hits, 0);
    assert_eq!(z.page_cache_misses, 0);
    Ok(())
}

// -------- helpers для WAL кадров --------

fn append_wal_record(
    wal_path: &PathBuf,
    rec_type: u8,
    lsn: u64,
    page_id: u64,
    payload: &[u8],
) -> Result<()> {
    let mut f = OpenOptions::new().read(true).write(true).open(wal_path)?;
    f.seek(SeekFrom::End(0))?;

    let mut hdr = vec![0u8; WAL_REC_HDR_SIZE];
    hdr[0] = rec_type;
    hdr[1] = 0;
    LittleEndian::write_u16(&mut hdr[2..4], 0);
    LittleEndian::write_u64(&mut hdr[WAL_REC_OFF_LSN..WAL_REC_OFF_LSN + 8], lsn);
    LittleEndian::write_u64(&mut hdr[WAL_REC_OFF_PAGE_ID..WAL_REC_OFF_PAGE_ID + 8], page_id);
    LittleEndian::write_u32(&mut hdr[WAL_REC_OFF_LEN..WAL_REC_OFF_LEN + 4], payload.len() as u32);

    // CRC over header (except crc) + payload
    let mut hasher = Crc32::new();
    hasher.update(&hdr[..WAL_REC_OFF_CRC32]);
    hasher.update(payload);
    let crc = hasher.finalize();
    LittleEndian::write_u32(&mut hdr[WAL_REC_OFF_CRC32..WAL_REC_OFF_CRC32 + 4], crc);

    f.write_all(&hdr)?;
    f.write_all(payload)?;
    f.sync_all()?;
    Ok(())
}

// -------- Test 2: WAL replay с неизвестным типом записи --------

#[test]
fn wal_replay_ignores_unknown_record_and_applies_valid() -> Result<()> {
    // Подготовка БД
    let root = unique_root("wal-unknown");
    fs::create_dir_all(&root)?;
    init_db(&root, 4096)?;
    Directory::create(&root, 8)?;

    // Получим page_size
    let ps = read_meta(&root)?.page_size as usize;

    // Подготовим payload v2 страниц: p0 с LSN=10, p1 с LSN=11
    let mut p0 = vec![0u8; ps];
    rh_page_init(&mut p0, 0)?;
    {
        let mut h = rh_header_read(&p0)?;
        h.lsn = 10;
        rh_header_write(&mut p0, &h)?;
        rh_page_update_crc(&mut p0)?;
    }

    let mut p1 = vec![0u8; ps];
    rh_page_init(&mut p1, 1)?;
    {
        let mut h = rh_header_read(&p1)?;
        h.lsn = 11;
        rh_header_write(&mut p1, &h)?;
        rh_page_update_crc(&mut p1)?;
    }

    // Откроем WAL, убедимся что header присутствует
    let wal_path = root.join(WAL_FILE);
    {
        let mut f = OpenOptions::new().read(true).open(&wal_path)?;
        let mut hdr16 = vec![0u8; WAL_HDR_SIZE];
        f.read_exact(&mut hdr16)?;
        assert_eq!(&hdr16[..8], WAL_MAGIC, "bad WAL magic");
    }

    // Добросим три кадра: [page_image p0][unknown rec_type=99][page_image p1]
    append_wal_record(&wal_path, WAL_REC_PAGE_IMAGE, 10, 0, &p0)?;
    // unknown record: пустой payload
    append_wal_record(&wal_path, 99, 999, 0, &[])?;
    append_wal_record(&wal_path, WAL_REC_PAGE_IMAGE, 11, 1, &p1)?;

    // Форсим unclean shutdown, чтобы сработал replay
    set_clean_shutdown(&root, false)?;

    // Writer open -> wal_replay_if_any -> применит p0 и p1, проигнорирует unknown
    {
        let _db = Db::open(&root)?;
    }

    // Проверим, что можно читать обе страницы
    let pager = Pager::open(&root)?;
    let mut buf = vec![0u8; ps];

    // page 0
    pager.read_page(0, &mut buf)?;
    assert_eq!(&buf[..4], b"P1PG");
    // page 1
    pager.read_page(1, &mut buf)?;
    assert_eq!(&buf[..4], b"P1PG");

    Ok(())
}

// -------- Test 3: P1_DATA_FSYNC=0 — WAL не трюнкатится при commit --------

#[test]
fn data_fsync_off_keeps_wal_untruncated_until_replay() -> Result<()> {
    std::env::set_var("P1_WAL_COALESCE_MS", "0");
    std::env::set_var("P1_DATA_FSYNC", "0"); // отключаем fsync данных

    let root = unique_root("fsync0");
    fs::create_dir_all(&root)?;
    init_db(&root, 4096)?;
    Directory::create(&root, 8)?;

    let wal_path = root.join(WAL_FILE);

    // Длина WAL после init — ровно header
    let len0 = fs::metadata(&wal_path)?.len();
    assert_eq!(len0, WAL_HDR_SIZE as u64);

    // Пишем одну запись через writer
    {
        let mut db = Db::open(&root)?;
        db.put(b"x", b"y")?;
        // drop -> clean=true
    }

    // При data_fsync=0, commit_page не вызывает wal.maybe_truncate(), поэтому WAL > header
    let len1 = fs::metadata(&wal_path)?.len();
    assert!(
        len1 > WAL_HDR_SIZE as u64,
        "WAL should not be truncated when P1_DATA_FSYNC=0 (len1={len1})"
    );

    // Теперь сделаем unclean shutdown и writer-open -> wal_replay_if_any усечёт WAL до header
    set_clean_shutdown(&root, false)?;
    {
        let _db = Db::open(&root)?;
    }
    let len2 = fs::metadata(&wal_path)?.len();
    assert_eq!(
        len2,
        WAL_HDR_SIZE as u64,
        "WAL should be truncated to header after replay"
    );

    // Вернём env, чтобы не влиять на другие тесты (best-effort)
    std::env::remove_var("P1_DATA_FSYNC");
    Ok(())
}