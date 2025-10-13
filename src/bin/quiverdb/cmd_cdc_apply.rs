use anyhow::{anyhow, Context, Result};
use byteorder::{ByteOrder, LittleEndian};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};
use std::net::TcpStream;
use std::path::PathBuf;

use QuiverDB::db::Db;
use QuiverDB::meta::set_last_lsn;
use QuiverDB::page::{
    PAGE_MAGIC, OFF_TYPE, KV_OFF_LSN, OVF_OFF_LSN,
    PAGE_TYPE_KV_RH3, PAGE_TYPE_OVERFLOW3,
    KV_HDR_MIN,
};
use QuiverDB::wal::{
    WAL_HDR_SIZE, WAL_MAGIC,
    WAL_REC_HDR_SIZE,
    WAL_REC_OFF_TYPE, WAL_REC_OFF_FLAGS, WAL_REC_OFF_RESERVED,
    WAL_REC_OFF_LSN, WAL_REC_OFF_PAGE_ID, WAL_REC_OFF_LEN, WAL_REC_OFF_CRC32,
    WAL_REC_PAGE_IMAGE, WAL_REC_HEADS_UPDATE,
    crc32c_of_parts,
};
use QuiverDB::wal::reader::read_next_record;
// защищённый CDC транспорт (фрейминг + HMAC-PSK) + TLS
use QuiverDB::wal::net::{load_psk_from_env, read_next_framed_psk, IoStream, open_tls_psk_stream};
// NEW: общие helpers для персистентного состояния
use QuiverDB::wal::state::{load_last_heads_lsn, store_last_heads_lsn, load_last_seq, store_last_seq};

/// CDC apply: применить WAL‑поток в целевую БД.
///
/// Поддерживаемые источники:
///   - file://<path>        — бинарный WAL файл (WAL header + кадры, P2WAL001)
///   - tcp+psk://host:port  — поток кадров WAL (фреймы HMAC‑PSK) по TCP
///   - tls+psk://host:port  — поток кадров WAL (фреймы HMAC‑PSK) по TLS (native-tls)
///
/// Новое:
/// - HEADS_UPDATE гейтится по LSN (персистентный last_heads_lsn в <root>/.heads_lsn.bin).
/// - Для tcp/tls+psk добавлена проверка монотонности seq (персистентный <root>/.cdc_seq.bin):
///   кадры с seq <= last_seq пропускаются; при P1_CDC_SEQ_STRICT=1 — out‑of‑order (seq < last_seq) → ошибка.
///
/// Примеры:
///   quiverdb cdc-apply --path ./follower --from file://./wal-stream.bin
///   quiverdb cdc-apply --path ./follower --from tcp+psk://127.0.0.1:9099
///   quiverdb cdc-apply --path ./follower --from tls+psk://127.0.0.1:9443
pub fn exec(path: PathBuf, from: String) -> Result<()> {
    if let Some(src_path) = from.strip_prefix("file://") {
        return apply_from_file(path, PathBuf::from(src_path));
    }
    if let Some(addr) = from.strip_prefix("tcp+psk://") {
        return apply_from_psk(path, addr, false);
    }
    if let Some(addr) = from.strip_prefix("tls+psk://") {
        return apply_from_psk(path, addr, true);
    }
    Err(anyhow!(
        "unsupported source '{}': use file://<path>, tcp+psk://host:port or tls+psk://host:port",
        from
    ))
}

// -------------------- file:// source --------------------

fn apply_from_file(path: PathBuf, src: PathBuf) -> Result<()> {
    // Откроем source-файл (WAL‑стрим)
    let mut f = OpenOptions::new()
        .read(true)
        .open(&src)
        .with_context(|| format!("open source {}", src.display()))?;

    // Проверим заголовок WAL stream
    if f.metadata()?.len() < WAL_HDR_SIZE as u64 {
        return Err(anyhow!("source too small (< WAL header): {}", src.display()));
    }
    let mut hdr = [0u8; WAL_HDR_SIZE];
    f.seek(SeekFrom::Start(0))?;
    Read::read_exact(&mut f, &mut hdr)?;
    if &hdr[..8] != WAL_MAGIC {
        return Err(anyhow!("bad WAL magic in source {}", src.display()));
    }

    // Writer DB
    let mut db = Db::open(&path)
        .with_context(|| format!("open writer DB at {}", path.display()))?;

    let ps = db.pager.meta.page_size as usize;

    let mut pos = WAL_HDR_SIZE as u64;
    let file_len = f.metadata()?.len();

    let mut frames = 0u64;
    let mut bytes = 0u64;
    let mut max_lsn = db.pager.meta.last_lsn;

    // Персистентный last_heads_lsn для HEADS_UPDATE LSN-гейтинга
    let mut last_heads_lsn = load_last_heads_lsn(&path).unwrap_or(0);

    while let Some((rec, next_pos)) = read_next_record(&mut f, pos, file_len)? {
        let frame_len = next_pos - pos;
        bytes += frame_len;
        frames += 1;

        if rec.lsn > max_lsn {
            max_lsn = rec.lsn;
        }

        match rec.rec_type {
            WAL_REC_PAGE_IMAGE => {
                // LSN‑гейтинг до ensure_allocated, если страница уже существует
                let mut apply = true;
                if rec.page_id < db.pager.meta.next_page_id {
                    let mut cur = vec![0u8; ps];
                    if db.pager.read_page(rec.page_id, &mut cur).is_ok() {
                        if let (Some(nl), Some(cl)) = (v3_page_lsn(&rec.payload), v3_page_lsn(&cur)) {
                            if cl >= nl {
                                apply = false;
                            }
                        }
                    }
                }
                if apply {
                    db.pager.ensure_allocated(rec.page_id)?;
                    db.pager.write_page_raw(rec.page_id, &rec.payload)?;
                }
            }
            WAL_REC_HEADS_UPDATE => {
                // LSN-гейтинг: применяем только если rec.lsn > last_heads_lsn
                if rec.lsn > last_heads_lsn {
                    let updates = parse_heads_updates(&rec.payload);
                    if !updates.is_empty() {
                        db.set_dir_heads_bulk(&updates)?;
                        last_heads_lsn = rec.lsn;
                        let _ = store_last_heads_lsn(&path, last_heads_lsn);
                    }
                }
            }
            _ => {}
        }

        pos = next_pos;
    }

    // best-effort: обновим meta.last_lsn
    let _ = set_last_lsn(&path, max_lsn);

    println!(
        "cdc-apply[file]: applied {} frames ({} bytes) to {}, last_lsn={}, last_heads_lsn={}",
        frames, bytes, path.display(), max_lsn, last_heads_lsn
    );

    Ok(())
}

// -------------------- tcp/tls+psk:// source --------------------

fn apply_from_psk(path: PathBuf, addr: &str, use_tls: bool) -> Result<()> {
    // Writer DB
    let mut db = Db::open(&path)
        .with_context(|| format!("open writer DB at {}", path.display()))?;
    let ps = db.pager.meta.page_size as usize;

    // Транспорт
    let mut stream = if use_tls {
        open_tls_psk_stream(addr)?
    } else {
        let sock = TcpStream::connect(addr)
            .with_context(|| format!("connect tcp+psk source {}", addr))?;
        let _ = sock.set_nodelay(true);
        IoStream::Plain(sock)
    };

    // PSK
    let psk = load_psk_from_env()?;

    let mut frames = 0u64;
    let mut bytes = 0u64;
    let mut max_lsn = db.pager.meta.last_lsn;

    // Максимальный размер payload: 28 байт заголовок + сама страница (<=1MiB) + запас
    let max_len = std::cmp::max(2 * 1024 * 1024, WAL_REC_HDR_SIZE + ps);

    // Персистентные маркеры консистентности
    let mut last_heads_lsn = load_last_heads_lsn(&path).unwrap_or(0);
    let mut last_seq = load_last_seq(&path).unwrap_or(0);

    while let Some((seq, payload)) = read_next_framed_psk(&mut stream, &psk, max_len)? {
        // Проверка монотонности seq (персистентная)
        if seq <= last_seq {
            if seq < last_seq && seq_strict() {
                return Err(anyhow!(
                    "out-of-order CDC frame: seq {} < last_seq {} (strict mode)",
                    seq, last_seq
                ));
            }
            // дубликат/повтор — пропустим
            continue;
        }

        // Кадр должен содержать минимум заголовок WAL
        if payload.len() < WAL_REC_HDR_SIZE {
            return Err(anyhow!("short WAL frame ({} < hdr)", payload.len()));
        }

        // Разобрать заголовок
        let rec_type = payload[WAL_REC_OFF_TYPE];
        let _flags = payload[WAL_REC_OFF_FLAGS];
        let _reserved = LittleEndian::read_u16(&payload[WAL_REC_OFF_RESERVED..WAL_REC_OFF_RESERVED + 2]);
        let lsn = LittleEndian::read_u64(&payload[WAL_REC_OFF_LSN..WAL_REC_OFF_LSN + 8]);
        let page_id = LittleEndian::read_u64(&payload[WAL_REC_OFF_PAGE_ID..WAL_REC_OFF_PAGE_ID + 8]);
        let len = LittleEndian::read_u32(&payload[WAL_REC_OFF_LEN..WAL_REC_OFF_LEN + 4]) as usize;
        let stored_crc = LittleEndian::read_u32(&payload[WAL_REC_OFF_CRC32..WAL_REC_OFF_CRC32 + 4]);

        // Согласованность длины
        if payload.len() != WAL_REC_HDR_SIZE + len {
            return Err(anyhow!(
                "bad WAL frame len: payload {} vs header.len {}",
                payload.len(),
                len
            ));
        }

        // Проверка CRC заголовка/полезной нагрузки (WAL invariant)
        let calc_crc = crc32c_of_parts(&payload[..WAL_REC_OFF_CRC32], &payload[WAL_REC_HDR_SIZE..]);
        if calc_crc != stored_crc {
            return Err(anyhow!(
                "WAL CRC mismatch (stored={}, calc={})",
                stored_crc,
                calc_crc
            ));
        }

        // Статистика
        frames += 1;
        bytes += payload.len() as u64;
        if lsn > max_lsn {
            max_lsn = lsn;
        }

        match rec_type {
            t if t == WAL_REC_PAGE_IMAGE => {
                let page_bytes = &payload[WAL_REC_HDR_SIZE..];

                // LSN‑гейтинг до ensure_allocated
                let mut apply = true;
                if page_id < db.pager.meta.next_page_id {
                    let mut cur = vec![0u8; ps];
                    if db.pager.read_page(page_id, &mut cur).is_ok() {
                        if let (Some(nl), Some(cl)) = (v3_page_lsn(page_bytes), v3_page_lsn(&cur)) {
                            if cl >= nl {
                                apply = false;
                            }
                        }
                    }
                }

                if apply {
                    db.pager.ensure_allocated(page_id)?;
                    db.pager.write_page_raw(page_id, page_bytes)?;
                }
            }

            t if t == WAL_REC_HEADS_UPDATE => {
                // LSN-гейтинг: применяем только если lsn > last_heads_lsn
                if lsn > last_heads_lsn {
                    let updates = parse_heads_updates(&payload[WAL_REC_HDR_SIZE..]);
                    if !updates.is_empty() {
                        db.set_dir_heads_bulk(&updates)?;
                        last_heads_lsn = lsn;
                        let _ = store_last_heads_lsn(&path, last_heads_lsn);
                    }
                }
            }

            _ => { /* BEGIN/COMMIT/TRUNCATE/PAGE_DELTA — игнорируем */ }
        }

        // Персистентно обновим last_seq после успешной обработки кадра
        last_seq = seq;
        let _ = store_last_seq(&path, last_seq);
    }

    // best-effort: обновим meta.last_lsn
    let _ = set_last_lsn(&path, max_lsn);

    println!(
        "cdc-apply[{}+psk]: applied {} frames ({} bytes) to {}, last_lsn={}, last_heads_lsn={}, last_seq={}",
        if use_tls { "tls" } else { "tcp" },
        frames, bytes, path.display(), max_lsn, last_heads_lsn, last_seq
    );

    Ok(())
}

// -------- helpers --------

fn v3_page_lsn(buf: &[u8]) -> Option<u64> {
    // Требуется минимальный заголовок (64 байта для v3 KV/OVF).
    if buf.len() < KV_HDR_MIN {
        return None;
    }
    if &buf[..4] != PAGE_MAGIC {
        return None;
    }
    let ptype = LittleEndian::read_u16(&buf[OFF_TYPE..OFF_TYPE + 2]);
    match ptype {
        t if t == PAGE_TYPE_KV_RH3 => Some(LittleEndian::read_u64(&buf[KV_OFF_LSN..KV_OFF_LSN + 8])),
        t if t == PAGE_TYPE_OVERFLOW3 => Some(LittleEndian::read_u64(&buf[OVF_OFF_LSN..OVF_OFF_LSN + 8])),
        _ => None,
    }
}

fn seq_strict() -> bool {
    std::env::var("P1_CDC_SEQ_STRICT")
        .ok()
        .map(|s| s.to_ascii_lowercase())
        .map(|s| s == "1" || s == "true" || s == "yes" || s == "on")
        .unwrap_or(false)
}

// -------- payload parser --------

#[inline]
fn parse_heads_updates(buf: &[u8]) -> Vec<(u32, u64)> {
    if buf.len() % 12 != 0 || buf.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(buf.len() / 12);
    let mut off = 0usize;
    while off + 12 <= buf.len() {
        let b = LittleEndian::read_u32(&buf[off..off + 4]);
        let pid = LittleEndian::read_u64(&buf[off + 4..off + 12]);
        out.push((b, pid));
        off += 12;
    }
    out
}