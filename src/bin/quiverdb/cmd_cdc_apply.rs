use anyhow::{anyhow, Context, Result};
use byteorder::{ByteOrder, LittleEndian};
use std::fs::OpenOptions;
use std::net::TcpStream;
use std::path::PathBuf;

use QuiverDB::db::Db;
use QuiverDB::meta::set_last_lsn;
use QuiverDB::page::{
    KV_HDR_MIN, KV_OFF_LSN, OFF_TYPE, OVF_OFF_LSN, PAGE_MAGIC, PAGE_TYPE_KV_RH3,
    PAGE_TYPE_OVERFLOW3,
};
use QuiverDB::wal::{
    crc32c_of_parts, wal_header_read_stream_id, WAL_HDR_SIZE, WAL_MAGIC, WAL_REC_HDR_SIZE,
    WAL_REC_HEADS_UPDATE, WAL_REC_OFF_CRC32, WAL_REC_OFF_FLAGS, WAL_REC_OFF_LEN, WAL_REC_OFF_LSN,
    WAL_REC_OFF_PAGE_ID, WAL_REC_OFF_RESERVED, WAL_REC_OFF_TYPE, WAL_REC_PAGE_IMAGE,
};
// NEW: stateful reader
use QuiverDB::wal::reader::WalStreamReader;
// CDC транспорт
use QuiverDB::wal::net::{load_psk_from_env, open_tls_psk_stream, read_next_framed_psk, IoStream};
// NEW: персистентные маркеры
use QuiverDB::wal::state::{
    load_last_heads_lsn, load_last_seq, load_stream_id, store_last_heads_lsn, store_last_seq,
    store_stream_id,
};

/// CDC apply: применить WAL‑поток в целевую БД.
///
/// Поддерживаемые источники:
///   - file://<path>        — бинарный WAL файл (WAL header + кадры, P2WAL001)
///   - tcp+psk://host:port  — поток WAL кадров (первый кадр — hello с WAL header) по TCP
///   - tls+psk://host:port  — поток WAL кадров (первый кадр — hello с WAL header) по TLS
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

// -------------------- helpers (env) --------------------

#[inline]
fn env_bool(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .map(|s| s.trim().to_ascii_lowercase())
        .map(|s| s == "1" || s == "true" || s == "yes" || s == "on")
        .unwrap_or(false)
}

// -------------------- file:// source --------------------

fn apply_from_file(path: PathBuf, src: PathBuf) -> Result<()> {
    // Откроем source-файл (WAL‑стрим)
    let mut f = OpenOptions::new()
        .read(true)
        .open(&src)
        .with_context(|| format!("open source {}", src.display()))?;

    // Проверим заголовок WAL stream и извлечём stream_id
    if f.metadata()?.len() < WAL_HDR_SIZE as u64 {
        return Err(anyhow!(
            "source too small (< WAL header): {}",
            src.display()
        ));
    }
    let stream_id = wal_header_read_stream_id(&mut f)
        .with_context(|| format!("read WAL header (stream_id) from {}", src.display()))?;

    // Валидация/фиксация stream_id на стороне follower’а
    verify_and_store_stream_id(&path, stream_id)?;

    // Writer DB
    let mut db =
        Db::open(&path).with_context(|| format!("open writer DB at {}", path.display()))?;

    let ps = db.pager.meta.page_size as usize;

    let mut pos = WAL_HDR_SIZE as u64;
    let file_len = f.metadata()?.len();

    let mut frames = 0u64;
    let mut bytes = 0u64;
    let mut max_lsn = db.pager.meta.last_lsn;

    // Персистентный last_heads_lsn для HEADS_UPDATE LSN-гейтинга
    let mut last_heads_lsn = load_last_heads_lsn(&path).unwrap_or(0);

    // NEW: stateful WAL reader
    let mut rdr = WalStreamReader::new();

    let heads_strict = env_bool("P1_CDC_HEADS_STRICT");

    while let Some((rec, next_pos)) = rdr.read_next(&mut f, pos, file_len)? {
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
                        if let (Some(nl), Some(cl)) = (v3_page_lsn(&rec.payload), v3_page_lsn(&cur))
                        {
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
                // Проверим корректность payload длины
                if rec.payload.len() == 0 || rec.payload.len() % 12 != 0 {
                    if heads_strict {
                        return Err(anyhow!(
                            "invalid HEADS_UPDATE payload length={} (strict mode)",
                            rec.payload.len()
                        ));
                    } else {
                        eprintln!(
                            "[WARN] HEADS_UPDATE payload length {} is invalid; skipping",
                            rec.payload.len()
                        );
                    }
                } else if rec.lsn > last_heads_lsn {
                    let updates = parse_heads_updates(&rec.payload);
                    if !updates.is_empty() {
                        db.set_dir_heads_bulk(&updates)?;
                        last_heads_lsn = rec.lsn;
                        let _ = store_last_heads_lsn(&path, last_heads_lsn);
                    } else {
                        eprintln!("[WARN] HEADS_UPDATE parsed empty updates; skipping");
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
        "cdc-apply[file]: applied {} frames ({} bytes) to {}, last_lsn={}, last_heads_lsn={}, stream_id={}",
        frames, bytes, path.display(), max_lsn, last_heads_lsn, stream_id
    );

    Ok(())
}

// -------------------- tcp/tls+psk:// source --------------------

fn apply_from_psk(path: PathBuf, addr: &str, use_tls: bool) -> Result<()> {
    // Writer DB
    let mut db =
        Db::open(&path).with_context(|| format!("open writer DB at {}", path.display()))?;
    let ps = db.pager.meta.page_size as usize;

    // Транспорт
    let mut stream = if use_tls {
        open_tls_psk_stream(addr)?
    } else {
        let sock =
            TcpStream::connect(addr).with_context(|| format!("connect tcp+psk source {}", addr))?;
        let _ = sock.set_nodelay(true);
        IoStream::Plain(sock)
    };

    // PSK
    let psk = load_psk_from_env()?;

    let mut frames = 0u64;
    let mut bytes = 0u64;
    let mut max_lsn = db.pager.meta.last_lsn;

    // Безопасный предел размера фрейма
    let heads_bytes = (db.dir.bucket_count as usize).saturating_mul(12);
    let mut max_len = WAL_REC_HDR_SIZE + std::cmp::max(ps, heads_bytes) + 64 * 1024; // +64 KiB запас
    let hard_cap = 32 * 1024 * 1024; // 32 MiB — верхний предел фрейма
    if max_len > hard_cap {
        max_len = hard_cap;
    }

    // Персистентные маркеры
    let mut last_heads_lsn = load_last_heads_lsn(&path).unwrap_or(0);
    let mut last_seq = load_last_seq(&path).unwrap_or(0);

    // STRICT/compat флаги
    let seq_strict = env_bool("P1_CDC_SEQ_STRICT");
    let allow_no_hello = env_bool("P1_CDC_ALLOW_NO_HELLO");
    let heads_strict = env_bool("P1_CDC_HEADS_STRICT");

    // 0) Ожидаем HELLO кадр (WAL header = MAGIC + stream_id).
    let hello = read_next_framed_psk(&mut stream, &psk, WAL_HDR_SIZE)?;
    let mut stream_id: u64 = 0;
    if let Some((seq0, payload0)) = hello {
        if payload0.len() == WAL_HDR_SIZE && &payload0[..8] == WAL_MAGIC {
            // HELLO: wal header
            stream_id = LittleEndian::read_u64(&payload0[8..16]);
            verify_and_store_stream_id(&path, stream_id)?;
            // Персистентно обновим last_seq
            if seq0 > last_seq {
                last_seq = seq0;
                let _ = store_last_seq(&path, last_seq);
            }
        } else {
            // Нет HELLO — по умолчанию ошибка; fallback только если разрешён ENV
            if !allow_no_hello {
                return Err(anyhow!(
                    "missing HELLO (WAL header) on PSK stream from {}; enable P1_CDC_ALLOW_NO_HELLO=1 to allow fallback",
                    addr
                ));
            } else {
                eprintln!(
                    "[WARN] no HELLO on PSK stream (addr={}); continuing without stream_id (anti-mix disabled)",
                    addr
                );
                // Обработаем payload0 как обычный WAL кадр
                // Прежде проверим seq
                if seq0 <= last_seq {
                    if seq_strict {
                        return Err(anyhow!(
                            "cdc seq regression on first frame (no-hello): {} <= {}",
                            seq0,
                            last_seq
                        ));
                    } else {
                        eprintln!(
                            "[WARN] cdc seq regression on first frame (no-hello): {} <= {}; skipping",
                            seq0, last_seq
                        );
                    }
                } else {
                    handle_wal_frame_payload(
                        &mut db,
                        &path,
                        &payload0,
                        &mut frames,
                        &mut bytes,
                        &mut max_lsn,
                        &mut last_heads_lsn,
                        ps,
                        heads_strict,
                    )?;
                    last_seq = seq0;
                    let _ = store_last_seq(&path, last_seq);
                }
            }
        }
    } else {
        // Пустой поток — нечего применять
        println!(
            "cdc-apply[{}+psk]: empty stream from {}, last_lsn={}, last_heads_lsn={}",
            if use_tls { "tls" } else { "tcp" },
            addr,
            max_lsn,
            last_heads_lsn
        );
        return Ok(());
    }

    // Основной цикл
    while let Some((seq, payload)) = read_next_framed_psk(&mut stream, &psk, max_len)? {
        // Проверка монотонности seq (персистентная)
        if seq <= last_seq {
            if seq_strict {
                return Err(anyhow!("cdc seq regression: {} <= {}", seq, last_seq));
            } else {
                eprintln!(
                    "[WARN] cdc seq regression: {} <= {} (non-strict mode); skipping frame",
                    seq, last_seq
                );
                continue;
            }
        }

        handle_wal_frame_payload(
            &mut db,
            &path,
            &payload,
            &mut frames,
            &mut bytes,
            &mut max_lsn,
            &mut last_heads_lsn,
            ps,
            heads_strict,
        )?;

        // Персистентно обновим last_seq после успешной обработки кадра
        last_seq = seq;
        let _ = store_last_seq(&path, last_seq);
    }

    // best-effort: обновим meta.last_lsn
    let _ = set_last_lsn(&path, max_lsn);

    println!(
        "cdc-apply[{}+psk]: applied {} frames ({} bytes) to {}, last_lsn={}, last_heads_lsn={}, last_seq={}, stream_id={}",
        if use_tls { "tls" } else { "tcp" },
        frames, bytes, path.display(), max_lsn, last_heads_lsn, last_seq, stream_id
    );

    Ok(())
}

// -------- helpers --------

fn handle_wal_frame_payload(
    db: &mut Db,
    root: &PathBuf,
    payload: &[u8],
    frames: &mut u64,
    bytes: &mut u64,
    max_lsn: &mut u64,
    last_heads_lsn: &mut u64,
    ps: usize,
    heads_strict: bool,
) -> Result<()> {
    // Кадр должен содержать минимум заголовок WAL
    if payload.len() < WAL_REC_HDR_SIZE {
        return Err(anyhow!("short WAL frame ({} < hdr)", payload.len()));
    }

    // Разобрать заголовок
    let rec_type = payload[WAL_REC_OFF_TYPE];
    let _flags = payload[WAL_REC_OFF_FLAGS];
    let _reserved =
        LittleEndian::read_u16(&payload[WAL_REC_OFF_RESERVED..WAL_REC_OFF_RESERVED + 2]);
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
    *frames += 1;
    *bytes += payload.len() as u64;
    if lsn > *max_lsn {
        *max_lsn = lsn;
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
            let pl = &payload[WAL_REC_HDR_SIZE..];
            if pl.len() == 0 || pl.len() % 12 != 0 {
                if heads_strict {
                    return Err(anyhow!(
                        "invalid HEADS_UPDATE payload length={} (strict mode)",
                        pl.len()
                    ));
                } else {
                    eprintln!(
                        "[WARN] HEADS_UPDATE payload length {} is invalid; skipping",
                        pl.len()
                    );
                    return Ok(());
                }
            }

            // LSN-гейтинг: применяем только если lsn > last_heads_lsn
            if lsn > *last_heads_lsn {
                let updates = parse_heads_updates(pl);
                if !updates.is_empty() {
                    db.set_dir_heads_bulk(&updates)?;
                    *last_heads_lsn = lsn;
                    let _ = store_last_heads_lsn(root, *last_heads_lsn);
                } else {
                    eprintln!("[WARN] HEADS_UPDATE parsed empty updates; skipping");
                }
            }
        }

        _ => { /* BEGIN/COMMIT/TRUNCATE/PAGE_DELTA — игнорируем */ }
    }

    Ok(())
}

fn v3_page_lsn(buf: &[u8]) -> Option<u64> {
    if buf.len() < KV_HDR_MIN {
        return None;
    }
    if &buf[..4] != PAGE_MAGIC {
        return None;
    }
    let ptype = LittleEndian::read_u16(&buf[OFF_TYPE..OFF_TYPE + 2]);
    match ptype {
        t if t == PAGE_TYPE_KV_RH3 => {
            Some(LittleEndian::read_u64(&buf[KV_OFF_LSN..KV_OFF_LSN + 8]))
        }
        t if t == PAGE_TYPE_OVERFLOW3 => {
            Some(LittleEndian::read_u64(&buf[OVF_OFF_LSN..OVF_OFF_LSN + 8]))
        }
        _ => None,
    }
}

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

fn verify_and_store_stream_id(root: &PathBuf, incoming: u64) -> Result<()> {
    if incoming == 0 {
        return Err(anyhow!("incoming WAL stream_id is zero (invalid)"));
    }
    let local = load_stream_id(root)?;
    if local == 0 {
        // Запомним первый валидный stream_id
        store_stream_id(root, incoming)?;
        Ok(())
    } else if local == incoming {
        Ok(())
    } else {
        Err(anyhow!(
            "WAL stream source mismatch: local stream_id={} vs incoming={}",
            local,
            incoming
        ))
    }
}
