// tests/cdc_partial_tail.rs
//
// Запуск только этого теста:
//   cargo test --test cdc_partial_tail -- --nocapture
//
// Все тесты:
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

use QuiverDB::{init_db, read_meta};
use QuiverDB::cli::wal_apply_from_stream;
use QuiverDB::consts::{
    WAL_FILE, WAL_HDR_SIZE, WAL_MAGIC, WAL_REC_HDR_SIZE, WAL_REC_OFF_CRC32, WAL_REC_OFF_LEN,
};
use QuiverDB::pager::Pager;
use QuiverDB::page_rh::{
    rh_header_read, rh_header_write, rh_kv_insert, rh_kv_lookup, rh_page_init, rh_page_is_kv,
    rh_page_update_crc,
};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let base = std::env::temp_dir();
    base.join(format!("qdbtest-partial-{prefix}-{pid}-{t}-{id}"))
}

// Прочитать WAL: вернуть (16-байтовый header, вектор кадров (hdr28+payload))
// При несоответствии CRC – пересчитать и поправить локально, чтобы поток был валиден до точки обрыва.
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

        // CRC check/fix (локально)
        let mut hasher = Crc32::new();
        hasher.update(&hdr[..WAL_REC_OFF_CRC32]);
        hasher.update(&payload);
        let crc_actual = hasher.finalize();
        if crc_expected != crc_actual {
            LittleEndian::write_u32(&mut hdr[WAL_REC_OFF_CRC32..WAL_REC_OFF_CRC32 + 4], crc_actual);
        }

        let mut full = hdr;
        full.extend_from_slice(&payload);
        frames.push(full);

        pos += rec_total;
    }

    Ok((hdr16, frames))
}

// Подготовить WAL лидера: одна страница v2, последовательные коммиты:
// init -> +k1 -> +k2 -> +k3
fn make_leader_wal() -> Result<(PathBuf, Vec<u8>, Vec<Vec<u8>>)> {
    std::env::set_var("P1_WAL_COALESCE_MS", "0");

    let root = unique_root("leader");
    fs::create_dir_all(&root)?;
    init_db(&root, 4096)?;

    // Работаем напрямую через Pager, чтобы все записи падали в одну страницу.
    let mut pager = Pager::open(&root)?;
    let page_id = 0u64;
    pager.ensure_allocated(page_id)?;
    let ps = pager.meta.page_size as usize;

    let meta = read_meta(&root)?;
    let kind = meta.hash_kind;

    let mut buf = vec![0u8; ps];

    // 1) init v2
    rh_page_init(&mut buf, page_id)?;
    pager.commit_page(page_id, &mut buf)?;

    // 2) +k1
    rh_kv_insert(&mut buf, kind, b"k1", b"v1")?;
    if &buf[..4] == QuiverDB::consts::PAGE_MAGIC {
        rh_page_update_crc(&mut buf)?;
    }
    pager.commit_page(page_id, &mut buf)?;

    // 3) +k2
    rh_kv_insert(&mut buf, kind, b"k2", b"v2")?;
    if &buf[..4] == QuiverDB::consts::PAGE_MAGIC {
        rh_page_update_crc(&mut buf)?;
    }
    pager.commit_page(page_id, &mut buf)?;

    // 4) +k3
    rh_kv_insert(&mut buf, kind, b"k3", b"v3")?;
    if &buf[..4] == QuiverDB::consts::PAGE_MAGIC {
        // LSN впишется в commit_page, но CRC посчитаем свежий (не обязательно, просто аккуратность)
        let h = rh_header_read(&buf)?;
        rh_header_write(&mut buf, &h)?;
        rh_page_update_crc(&mut buf)?;
    }
    pager.commit_page(page_id, &mut buf)?;

    // Снимем WAL
    let (hdr16, frames) = parse_wal(&root)?;
    assert!(frames.len() >= 4, "expected at least 4 frames (init + 3 puts)");
    Ok((root, hdr16, frames))
}

#[test]
fn apply_stops_gracefully_on_partial_payload_tail() -> Result<()> {
    let (_leader_path, hdr16, frames) = make_leader_wal()?;

    // Сконструируем поток: [header][3 полных записи][4-я запись: заголовок + кусочек payload]
    let mut stream = Vec::new();
    stream.extend_from_slice(&hdr16);

    // Возьмём первые 3 полных кадра
    for rec in frames.iter().take(3) {
        stream.extend_from_slice(rec);
    }

    // 4-й кадр: заголовок полностью + 16 байт payload (неполный хвост)
    let last = &frames[3];
    assert!(last.len() > WAL_REC_HDR_SIZE + 16);
    stream.extend_from_slice(&last[..WAL_REC_HDR_SIZE + 16]);

    // Фолловер: применяем
    let follower = unique_root("follower-payload");
    fs::create_dir_all(&follower)?;
    init_db(&follower, 4096)?;

    {
        let cursor = Cursor::new(stream);
        wal_apply_from_stream(&follower, cursor)?;
    }

    // Проверим состояние страницы: должны видеть k1 и k2, а k3 — нет (так как кадр обрезан)
    let meta_f = read_meta(&follower)?;
    let pager_f = Pager::open(&follower)?;
    let ps = pager_f.meta.page_size as usize;
    let mut page = vec![0u8; ps];

    pager_f.read_page(0, &mut page)?;
    assert!(rh_page_is_kv(&page), "page 0 must be v2 KV after apply");

    let v1 = rh_kv_lookup(&page, meta_f.hash_kind, b"k1")?.expect("k1 must exist");
    assert_eq!(v1, b"v1");
    let v2 = rh_kv_lookup(&page, meta_f.hash_kind, b"k2")?.expect("k2 must exist");
    assert_eq!(v2, b"v2");
    let v3 = rh_kv_lookup(&page, meta_f.hash_kind, b"k3")?;
    assert!(v3.is_none(), "k3 must be absent (partial tail)");

    Ok(())
}

#[test]
fn apply_stops_gracefully_on_partial_header_tail() -> Result<()> {
    let (_leader_path, hdr16, frames) = make_leader_wal()?;

    // Поток: [header][3 полных записи][4-я запись: только первые 8 байт заголовка]
    let mut stream = Vec::new();
    stream.extend_from_slice(&hdr16);
    for rec in frames.iter().take(3) {
        stream.extend_from_slice(rec);
    }
    let last = &frames[3];
    // первые 8 байт заголовка (не WAL_MAGIC) => wal_apply попытается дочитать tail заголовка и увидит EOF
    stream.extend_from_slice(&last[..8]);

    // Фолловер
    let follower = unique_root("follower-header");
    fs::create_dir_all(&follower)?;
    init_db(&follower, 4096)?;

    {
        let cursor = Cursor::new(stream);
        wal_apply_from_stream(&follower, cursor)?;
    }

    // Проверка: k1 и k2 есть, k3 нет
    let meta_f = read_meta(&follower)?;
    let pager_f = Pager::open(&follower)?;
    let ps = pager_f.meta.page_size as usize;
    let mut page = vec![0u8; ps];

    pager_f.read_page(0, &mut page)?;
    assert!(rh_page_is_kv(&page));

    let v1 = rh_kv_lookup(&page, meta_f.hash_kind, b"k1")?.expect("k1 must exist");
    assert_eq!(v1, b"v1");
    let v2 = rh_kv_lookup(&page, meta_f.hash_kind, b"k2")?.expect("k2 must exist");
    assert_eq!(v2, b"v2");
    let v3 = rh_kv_lookup(&page, meta_f.hash_kind, b"k3")?;
    assert!(v3.is_none(), "k3 must be absent (partial header)");

    Ok(())
}