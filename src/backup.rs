//! Incremental/Full backup (Phase 1 + Phase 2 dedup best-effort).
//!
//! Идея: делаем бэкап "как есть" на момент снапшота S (snapshot_lsn = S).
//! Для каждой страницы page_id:
//! - если live_page_lsn <= S — берём live;
//! - иначе — берём frozen-копию из sidecar данного снапшота.
//!
//! Фильтр по since_lsn (инкрементальный): берём только страницы с lsn ∈ (since_lsn, S].
//!
//! Формат на вывод:
//! - pages.bin: кадры страниц (как freeze.bin):
//!   [page_id u64][page_lsn u64][page_len u32][crc32 u32] + payload(page_size)
//! - dir.bin: копия файла каталога <root>/dir (если присутствует)
//! - manifest.json: минимальная сводка (snapshot_lsn, since_lsn, page_size, pages_emitted, bytes_emitted, dir_present, dir_bytes)
//!
//! Phase 2 (подготовка, без ломки форматов):
//! - Если включён дедуп (cfg.snap_dedup = true), для каждого выбранного кадра снова кладём payload в SnapStore
//!   (content-addressed, best-effort). Это не влияет на pages.bin и restore; чтение из SnapStore будет
//!   добавлено позже вместе с persisted snapshot registry/GC.

use anyhow::{anyhow, Context, Result};
use byteorder::{ByteOrder, LittleEndian};
use crc32fast::Hasher as Crc32;
use log::{debug, info, warn};
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use crate::db::Db;
use crate::meta::{set_clean_shutdown, set_last_lsn};
use crate::pager::Pager;
use crate::snapshots::SnapshotHandle;
use crate::util::v2_page_lsn;
use crate::metrics::{record_backup_page_emitted, record_restore_page_written};

// Phase 2: SnapStore (content-addressed dedup best-effort on backup)
use crate::snapshots::store::SnapStore;

/// Бэкап в директорию out_dir (создаётся при необходимости).
/// Если since_lsn=None — full backup (все страницы как на S).
/// Если since_lsn=Some(N) — incremental (только страницы с lsn ∈ (since_lsn, S]).
///
/// Выход:
/// - out_dir/pages.bin      — кадры страниц (header+payload)
/// - out_dir/dir.bin        — копия каталога (если присутствует)
/// - out_dir/manifest.json  — сводка
pub fn backup_to_dir(
    db: &Db,
    snap: &SnapshotHandle,
    out_dir: &Path,
    since_lsn: Option<u64>,
) -> Result<()> {
    let ps = db.pager.meta.page_size as usize;
    let snapshot_lsn = snap.lsn;

    info!(
        "backup: start, root={}, out={}, snapshot_lsn={}, since_lsn={}",
        db.root.display(),
        out_dir.display(),
        snapshot_lsn,
        since_lsn.map(|v| v.to_string()).unwrap_or_else(|| "null".to_string())
    );

    fs::create_dir_all(out_dir)
        .with_context(|| format!("create backup dir {}", out_dir.display()))?;

    let pages_path = out_dir.join("pages.bin");
    let mut pages_file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&pages_path)
        .with_context(|| format!("open {}", pages_path.display()))?;

    let mut pages_emitted: u64 = 0;
    let mut bytes_emitted: u64 = 0;

    // Для ускорения: заранее поднимем индекс из sidecar (page_id -> offset).
    // Будем обновлять его по мере необходимости (retry) при промахах.
    let mut freeze_index = build_freeze_index(snap)?;

    // Phase 2: лениво откроем SnapStore, если включён дедуп в конфиге.
    let mut snapstore: Option<SnapStore> = if db.cfg.snap_dedup {
        match SnapStore::open(&db.root, db.pager.meta.page_size) {
            Ok(ss) => Some(ss),
            Err(e) => {
                // Не валим бэкап — просто логируем предупреждение.
                warn!("backup: snapstore open failed (dedup disabled for this run): {e}");
                None
            }
        }
    } else {
        None
    };

    // Пройдём все page_id
    let total_pages = db.pager.meta.next_page_id;
    debug!(
        "backup: scanning {} page(s), page_size={}, root={}",
        total_pages, ps, db.root.display()
    );

    for pid in 0..total_pages {
        // Выбор источника payload для этой страницы с учётом snapshot_lsn и retry на заморозку
        let payload_with_lsn = match choose_payload_with_retry(db, snap, &mut freeze_index, pid, ps, snapshot_lsn) {
            Ok(opt) => opt,
            Err(e) => {
                // best-effort: пропустим страницу при ошибке чтения (не валим весь бэкап)
                warn!("backup: skip page {} due to error: {}", pid, e);
                None
            }
        };

        let (payload, lsn_used) = match payload_with_lsn {
            Some(v) => v,
            None => continue,
        };

        // Инкрементальный фильтр
        if let Some(since) = since_lsn {
            if !(lsn_used > since && lsn_used <= snapshot_lsn) {
                continue;
            }
        }

        // Phase 2: best-effort дедуп — сохранить кадр в SnapStore.
        if let Some(ss) = snapstore.as_mut() {
            let _ = ss.put(&payload);
        }

        // Header: [page_id u64][page_lsn u64][page_len u32][crc32 u32]
        let mut hdr = vec![0u8; 8 + 8 + 4 + 4];
        LittleEndian::write_u64(&mut hdr[0..8], pid);
        LittleEndian::write_u64(&mut hdr[8..16], lsn_used);
        LittleEndian::write_u32(&mut hdr[16..20], payload.len() as u32);

        let mut hasher = Crc32::new();
        hasher.update(&hdr[0..20]);
        hasher.update(&payload);
        let crc = hasher.finalize();
        LittleEndian::write_u32(&mut hdr[20..24], crc);

        pages_file.write_all(&hdr)?;
        pages_file.write_all(&payload)?;

        // Метрики backup
        record_backup_page_emitted(payload.len());

        pages_emitted += 1;
        bytes_emitted += (hdr.len() + payload.len()) as u64;
    }

    // Копируем каталог (dir) если есть
    let dir_src = db.root.join("dir");
    let dir_dst = out_dir.join("dir.bin");
    let (dir_present, dir_bytes) = if dir_src.exists() {
        let bytes = fs::read(&dir_src).with_context(|| format!("read {}", dir_src.display()))?;
        fs::write(&dir_dst, &bytes)
            .with_context(|| format!("write {}", dir_dst.display()))?;
        (true, bytes.len() as u64)
    } else {
        (false, 0)
    };

    // Запишем manifest.json
    let manifest_path = out_dir.join("manifest.json");
    let manifest = format!(
        "{{\"snapshot_lsn\":{},\"since_lsn\":{},\"page_size\":{},\"pages_emitted\":{},\"bytes_emitted\":{},\"dir_present\":{},\"dir_bytes\":{}}}\n",
        snapshot_lsn,
        since_lsn.map(|v| v.to_string()).unwrap_or_else(|| "null".to_string()),
        ps,
        pages_emitted,
        bytes_emitted,
        if dir_present { "true" } else { "false" },
        dir_bytes
    );
    fs::write(&manifest_path, manifest)
        .with_context(|| format!("write manifest {}", manifest_path.display()))?;

    info!(
        "backup: done pages={}, bytes={}, dir_present={}, dir_bytes={}, out={}",
        pages_emitted, bytes_emitted, dir_present, dir_bytes, out_dir.display()
    );

    Ok(())
}

/// Восстановление БД из директории бэкапа (pages.bin + manifest.json + dir.bin).
/// Предполагается пустой/новый путь dst_root. Создаёт БД, заливает страницы, копирует каталог.
pub fn restore_from_dir(dst_root: &Path, backup_dir: &Path) -> Result<()> {
    info!(
        "restore: start, dst={}, from={}",
        dst_root.display(),
        backup_dir.display()
    );

    // Прочитаем manifest.json (минимальный парсинг page_size)
    let manifest_path = backup_dir.join("manifest.json");
    let manifest = fs::read_to_string(&manifest_path)
        .with_context(|| format!("read manifest {}", manifest_path.display()))?;
    let page_size = parse_manifest_u64(&manifest, "\"page_size\":")
        .ok_or_else(|| anyhow!("manifest: page_size not found"))? as u32;

    // Создадим/инициализируем БД (meta/seg1/WAL/free)
    if !dst_root.exists() {
        fs::create_dir_all(dst_root)
            .with_context(|| format!("create dst root {}", dst_root.display()))?;
    }
    // init_db может вернуть ошибку, если meta уже существует. Запустим только если нет meta.
    if !dst_root.join("meta").exists() {
        crate::init_db(dst_root, page_size)?;
    }

    // Если есть dir.bin — положим его как dst_root/dir
    let dir_bin = backup_dir.join("dir.bin");
    if dir_bin.exists() {
        let bytes = fs::read(&dir_bin)
            .with_context(|| format!("read {}", dir_bin.display()))?;
        let dst_dir = dst_root.join("dir");
        fs::write(&dst_dir, &bytes)
            .with_context(|| format!("write {}", dst_dir.display()))?;
    }

    // Пройдём pages.bin и запишем страницы
    let pages_path = backup_dir.join("pages.bin");
    let mut f = OpenOptions::new()
        .read(true)
        .open(&pages_path)
        .with_context(|| format!("open {}", pages_path.display()))?;

    let mut pager = Pager::open(dst_root)?;
    let ps = pager.meta.page_size as usize;
    let mut max_lsn: u64 = 0;

    let mut pages_written: u64 = 0;
    let mut bytes_written: u64 = 0;

    let mut pos = 0u64;
    let len = f.metadata()?.len();

    while pos + 24 <= len {
        // Header: [page_id u64][page_lsn u64][page_len u32][crc32 u32]
        let mut hdr = [0u8; 24];
        f.seek(SeekFrom::Start(pos))?;
        f.read_exact(&mut hdr)?;
        let page_id = LittleEndian::read_u64(&hdr[0..8]);
        let page_lsn = LittleEndian::read_u64(&hdr[8..16]);
        let page_len = LittleEndian::read_u32(&hdr[16..20]) as usize;
        let crc_expected = LittleEndian::read_u32(&hdr[20..24]);

        let rec_total = 24u64 + page_len as u64;
        if pos + rec_total > len {
            debug!(
                "restore: partial tail at off={}, need {} bytes, stop",
                pos, rec_total
            );
            break; // неполный хвост — выходим
        }

        let mut payload = vec![0u8; page_len];
        f.read_exact(&mut payload)?;

        // CRC verify
        let mut hasher = Crc32::new();
        hasher.update(&hdr[0..20]);
        hasher.update(&payload);
        let crc_actual = hasher.finalize();
        if crc_actual != crc_expected {
            return Err(anyhow!("pages.bin CRC mismatch at off {}", pos));
        }

        if page_len != ps {
            return Err(anyhow!(
                "pages.bin frame len {} != page_size {}",
                page_len,
                ps
            ));
        }

        // ensure + write
        pager.ensure_allocated(page_id)?;
        pager.write_page_raw(page_id, &payload)?;

        // Метрики restore
        record_restore_page_written(payload.len());

        pages_written += 1;
        bytes_written += (24 + payload.len()) as u64;

        if page_lsn > max_lsn {
            max_lsn = page_lsn;
        }

        pos += rec_total;
    }

    if max_lsn > 0 {
        set_last_lsn(dst_root, max_lsn)?;
    }
    set_clean_shutdown(dst_root, true)?;

    info!(
        "restore: done pages={}, bytes={}, last_lsn={}, dst={}",
        pages_written, bytes_written, max_lsn, dst_root.display()
    );

    Ok(())
}

/// Сканирует index.bin снапшота и строит карту page_id -> offset (последний по встрече).
fn build_freeze_index(snap: &SnapshotHandle) -> Result<std::collections::HashMap<u64, u64>> {
    let mut map = std::collections::HashMap::new();
    let idx_path = snap_index_path(snap);
    if !idx_path.exists() {
        return Ok(map);
    }
    let mut f = OpenOptions::new()
        .read(true)
        .open(&idx_path)
        .with_context(|| format!("open {}", idx_path.display()))?;
    let mut pos = 0u64;
    let len = f.metadata()?.len();

    // Каждая запись index: [page_id u64][offset u64][page_lsn u64] = 24 байта
    const REC: u64 = 8 + 8 + 8;
    let mut buf = vec![0u8; REC as usize];
    while pos + REC <= len {
        f.seek(SeekFrom::Start(pos))?;
        f.read_exact(&mut buf)?;
        let page_id = LittleEndian::read_u64(&buf[0..8]);
        let off = LittleEndian::read_u64(&buf[8..16]);
        // let _lsn = LittleEndian::read_u64(&buf[16..24]); // не используем
        map.insert(page_id, off);
        pos += REC;
    }
    Ok(map)
}

/// Прочитать frozen-страницу из freeze.bin по смещению из индекса.
fn read_frozen_page(
    freeze_index: &std::collections::HashMap<u64, u64>,
    snap: &SnapshotHandle,
    page_id: u64,
    page_size: usize,
) -> Result<Option<Vec<u8>>> {
    let off = match freeze_index.get(&page_id) {
        Some(o) => *o,
        None => return Ok(None),
    };
    let path = snap_freeze_path(snap);
    let mut f = OpenOptions::new()
        .read(true)
        .open(&path)
        .with_context(|| format!("open {}", path.display()))?;

    // Header: [page_id u64][page_lsn u64][page_len u32][crc32 u32]
    let mut hdr = [0u8; 8 + 8 + 4 + 4];
    f.seek(SeekFrom::Start(off))?;
    f.read_exact(&mut hdr)?;
    let pid = LittleEndian::read_u64(&hdr[0..8]);
    if pid != page_id {
        return Err(anyhow!(
            "freeze frame page_id mismatch at off={}, expected={}, got={}",
            off,
            page_id,
            pid
        ));
    }
    let page_len = LittleEndian::read_u32(&hdr[16..20]) as usize;
    if page_len != page_size {
        return Err(anyhow!(
            "freeze frame len={} != page_size {}",
            page_len,
            page_size
        ));
    }
    let crc_expected = LittleEndian::read_u32(&hdr[20..24]);

    // Payload
    let mut payload = vec![0u8; page_len];
    f.read_exact(&mut payload)?;

    // CRC verify
    let mut hasher = Crc32::new();
    hasher.update(&hdr[0..20]);
    hasher.update(&payload);
    let crc_actual = hasher.finalize();
    if crc_actual != crc_expected {
        return Err(anyhow!("freeze frame CRC mismatch for page {}", page_id));
    }

    Ok(Some(payload))
}

fn snap_dir(snap: &SnapshotHandle) -> &Path {
    &snap.freeze_dir
}
fn snap_index_path(snap: &SnapshotHandle) -> PathBuf {
    snap_dir(snap).join("index.bin")
}
fn snap_freeze_path(snap: &SnapshotHandle) -> PathBuf {
    snap_dir(snap).join("freeze.bin")
}

/// Простейший парсер положительного u64 для ключа вида "  ... "page_size": 4096, ..."
fn parse_manifest_u64(s: &str, key: &str) -> Option<u64> {
    let pos = s.find(key)?;
    let tail = &s[pos + key.len()..];
    let mut num = String::new();
    for ch in tail.chars() {
        if ch.is_ascii_digit() {
            num.push(ch);
        } else if !num.is_empty() {
            break;
        }
    }
    num.parse::<u64>().ok()
}

/// Выбор payload для страницы с учётом snapshot_lsn:
/// - live v2 с lsn ≤ snapshot_lsn → live
/// - live v2 с lsn > snapshot_lsn → frozen (retry с пересбором индекса)
/// - live non-v2 → live
/// - live read error → fallback frozen (retry)
fn choose_payload_with_retry(
    db: &Db,
    snap: &SnapshotHandle,
    freeze_index: &mut std::collections::HashMap<u64, u64>,
    page_id: u64,
    page_size: usize,
    snapshot_lsn: u64,
) -> Result<Option<(Vec<u8>, u64)>> {
    let mut live_buf = vec![0u8; page_size];

    // Попробуем читать live
    match db.pager.read_page(page_id, &mut live_buf) {
        Ok(()) => {
            match v2_page_lsn(&live_buf) {
                Some(live_lsn) => {
                    if live_lsn <= snapshot_lsn {
                        // живой снимок подходит
                        return Ok(Some((live_buf, live_lsn)));
                    }
                    // нужен frozen; попробуем с retry
                    if let Some(bytes) = get_frozen_or_retry(snap, freeze_index, page_id, page_size)? {
                        let lsn_frozen = v2_page_lsn(&bytes).unwrap_or(snapshot_lsn);
                        return Ok(Some((bytes, lsn_frozen)));
                    }
                    // не нашли — best-effort: пропустим эту страницу
                    Ok(None)
                }
                None => {
                    // не v2 — берём live как есть
                    Ok(Some((live_buf, snapshot_lsn)))
                }
            }
        }
        Err(_) => {
            // live не читается — fallback на frozen с retry
            if let Some(bytes) = get_frozen_or_retry(snap, freeze_index, page_id, page_size)? {
                let lsn_frozen = v2_page_lsn(&bytes).unwrap_or(snapshot_lsn);
                Ok(Some((bytes, lsn_frozen)))
            } else {
                Ok(None)
            }
        }
    }
}

/// Поиск frozen-страницы с несколькими попытками:
/// 1) проверить текущий index,
/// 2) пересобрать index и проверять снова, с небольшими задержками.
fn get_frozen_or_retry(
    snap: &SnapshotHandle,
    freeze_index: &mut std::collections::HashMap<u64, u64>,
    page_id: u64,
    page_size: usize,
) -> Result<Option<Vec<u8>>> {
    const RETRIES: usize = 3;
    const DELAY_MS: u64 = 5;

    for attempt in 0..=RETRIES {
        if let Some(bytes) = read_frozen_page(freeze_index, snap, page_id, page_size)? {
            return Ok(Some(bytes));
        }
        if attempt < RETRIES {
            *freeze_index = build_freeze_index(snap)?;
            thread::sleep(Duration::from_millis(DELAY_MS));
        }
    }
    Ok(None)
}