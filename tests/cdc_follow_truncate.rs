// tests/cdc_follow_truncate.rs
//
// Как запустить только этот тест:
//   cargo test --test cdc_follow_truncate -- --nocapture
//
// Или все тесты:
//   cargo test -- --nocapture

use std::fs;
use std::fs::OpenOptions;
use std::io::{Cursor, Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use byteorder::{ByteOrder, LittleEndian};
use crc32fast::Hasher as Crc32;

use QuiverDB::{init_db, read_meta, Db, Directory};
use QuiverDB::cli::wal_apply_from_stream;
use QuiverDB::consts::{
    WAL_FILE, WAL_HDR_SIZE, WAL_MAGIC, WAL_REC_HDR_SIZE, WAL_REC_OFF_CRC32, WAL_REC_OFF_LEN,
    WAL_REC_OFF_LSN,
};
use QuiverDB::pager::Pager;
use QuiverDB::page_rh::{rh_kv_lookup, rh_page_is_kv};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let base = std::env::temp_dir();
    base.join(format!("qdbtest-cdc-{prefix}-{pid}-{t}-{id}"))
}

// Прочитать WAL и вернуть (16-байтовый заголовок, вектор кадров),
// каждый кадр = header(28B) + payload(page image).
// Если CRC в заголовке пуст/битый, пересчитываем и исправляем локально.
fn parse_wal(root: &PathBuf) -> Result<(Vec<u8>, Vec<Vec<u8>>)> {
    let wal_path = root.join(WAL_FILE);
    let mut f = OpenOptions::new().read(true).open(&wal_path)?;
    let mut hdr16 = vec![0u8; WAL_HDR_SIZE];
    f.read_exact(&mut hdr16)?;
    assert_eq!(&hdr16[..8], WAL_MAGIC, "bad WAL magic");

    let mut frames: Vec<Vec<u8>> = Vec::new();
    let len = f.metadata()?.len();
    let mut pos = WAL_HDR_SIZE as u64;

    while pos + (WAL_REC_HDR_SIZE as u64) <= len {
        f.seek(SeekFrom::Start(pos))?;
        let mut hdr = vec![0u8; WAL_REC_HDR_SIZE];
        if f.read_exact(&mut hdr).is_err() {
            break;
        }
        let payload_len =
            LittleEndian::read_u32(&hdr[WAL_REC_OFF_LEN..WAL_REC_OFF_LEN + 4]) as usize;
        let crc_expected =
            LittleEndian::read_u32(&hdr[WAL_REC_OFF_CRC32..WAL_REC_OFF_CRC32 + 4]);
        let rec_total = WAL_REC_HDR_SIZE as u64 + (payload_len as u64);
        if pos + rec_total > len {
            break;
        }
        let mut payload = vec![0u8; payload_len];
        f.read_exact(&mut payload)?;

        // Пересчитаем CRC и починим локальную копию, если нужно.
        let mut hasher = Crc32::new();
        hasher.update(&hdr[..WAL_REC_OFF_CRC32]);
        hasher.update(&payload);
        let crc_actual = hasher.finalize();
        if crc_expected != crc_actual {
            LittleEndian::write_u32(&mut hdr[WAL_REC_OFF_CRC32..WAL_REC_OFF_CRC32 + 4], crc_actual);
        }

        // keep full record bytes (hdr+payload)
        let mut full = hdr;
        full.extend_from_slice(&payload);
        frames.push(full);

        pos += rec_total;
    }

    Ok((hdr16, frames))
}

#[test]
fn cdc_apply_ignores_midstream_header_and_applies_all_records() -> Result<()> {
    // Для детерминированности
    std::env::set_var("P1_WAL_COALESCE_MS", "0");

    // ----- Лидер: одна сессия, 10 ключей -----
    let leader = unique_root("leader");
    fs::create_dir_all(&leader)?;
    init_db(&leader, 4096)?;
    Directory::create(&leader, 64)?;

    {
        let mut db = Db::open(&leader)?;
        for i in 0..10 {
            let k = format!("k{i:03}");
            let v = format!("v{i:03}");
            db.put(k.as_bytes(), v.as_bytes())?;
        }
        // drop -> clean_shutdown=true
    }

    // Снимем WAL целиком
    let (wal_hdr, wal_frames) = parse_wal(&leader)?;
    assert!(
        !wal_frames.is_empty(),
        "WAL must contain records after puts"
    );

    // Разделим на две части и соберём поток: [header][часть1][header][часть2]
    let mid = wal_frames.len() / 2;
    let (part1, part2) = wal_frames.split_at(mid.max(1)); // защитимся от 0

    let mut stream_bytes: Vec<u8> = Vec::new();
    stream_bytes.extend_from_slice(&wal_hdr);
    for rec in part1 {
        stream_bytes.extend_from_slice(rec);
    }
    stream_bytes.extend_from_slice(&wal_hdr); // mid-stream header
    for rec in part2 {
        stream_bytes.extend_from_slice(rec);
    }

    // ----- Фолловер: применяем поток -----
    let follower = unique_root("follower");
    fs::create_dir_all(&follower)?;
    init_db(&follower, 4096)?; // dir не обязателен

    {
        let cursor = Cursor::new(stream_bytes);
        wal_apply_from_stream(&follower, cursor)?;
    }

    // ----- Проверка: все ключи присутствуют на страницах фолловера -----
    let meta_f = read_meta(&follower)?;
    let pager = Pager::open(&follower)?;
    let ps = pager.meta.page_size as usize;

    let mut page_buf = vec![0u8; ps];

    for i in 0..10 {
        let key = format!("k{i:03}");
        let val = format!("v{i:03}");
        let mut found = false;

        for pid in 0..pager.meta.next_page_id {
            pager.read_page(pid, &mut page_buf)?;
            if !rh_page_is_kv(&page_buf) {
                continue;
            }
            if let Some(v) = rh_kv_lookup(&page_buf, meta_f.hash_kind, key.as_bytes())? {
                assert_eq!(
                    v,
                    val.as_bytes(),
                    "value mismatch for key {} on follower (page {})",
                    key,
                    pid
                );
                found = true;
                break;
            }
        }
        assert!(
            found,
            "key {} must be present on follower after apply",
            key
        );
    }

    // Дополнительно: LSN монотонный в исходном WAL
    let mut lsns = Vec::new();
    for rec in &wal_frames {
        let lsn = LittleEndian::read_u64(&rec[WAL_REC_OFF_LSN..WAL_REC_OFF_LSN + 8]);
        lsns.push(lsn);
    }
    for w in lsns.windows(2) {
        assert!(w[0] < w[1], "LSN must be strictly increasing: {:?}", lsns);
    }

    Ok(())
}