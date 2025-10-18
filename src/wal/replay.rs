//! wal/replay — универсальный реплей WAL v2 (P2WAL001) с LSN‑гейтингом в вызывающем слое.
//!
//! Обновления:
//! - Переход на stateful WalStreamReader (src/wal/reader.rs) вместо глобальной функции.
//!   Это убирает утечку состояния mid‑header между потоками/стримами и повышает безопасность.
//!
//! Поведение неизменно:
//! - Чтение кадров WAL выполняется через WalStreamReader::read_next()
//!   (проверка CRC/partial tails — внутри reader).
//! - HEADS_UPDATE гейтится по LSN: применяется только если wal_lsn > last_heads_lsn,
//!   маркер хранится в <root>/.heads_lsn.bin (см. wal::state).
//! - Этот модуль содержит только wal_replay_if_any(..).
//!   Метод Pager::wal_replay_with_pager находится в src/pager/replay.rs.

use anyhow::{anyhow, Context, Result};
use byteorder::{ByteOrder, LittleEndian};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use super::{
    wal_path, write_wal_file_header, WAL_HDR_SIZE, WAL_MAGIC, WAL_REC_BEGIN, WAL_REC_COMMIT,
    WAL_REC_HEADS_UPDATE, WAL_REC_PAGE_DELTA, WAL_REC_PAGE_IMAGE, WAL_REC_TRUNCATE,
};
// NEW: stateful stream reader
use super::reader::WalStreamReader;
// NEW: персистентное состояние для LSN-гейтинга HEADS_UPDATE
use super::state::{load_last_heads_lsn, store_last_heads_lsn};

/// Реплей WAL v2 c CRC32C и LSN‑гейтингом (внутри apply_page).
///
/// Правила:
/// - При clean_shutdown=true: truncate до заголовка и выход.
/// - При clean_shutdown=false: идём подряд, CRC обязательна, частичный хвост — нормальный EOF.
/// - BEGIN/COMMIT/PAGE_DELTA/неизвестные типы — игнор.
/// - PAGE_IMAGE -> apply_page(lsn, page_id, payload).
/// - HEADS_UPDATE -> применить set_heads_bulk к каталогу ТОЛЬКО если wal_lsn > last_heads_lsn; затем обновить last_heads_lsn.
/// - По завершении — truncate до заголовка, meta.last_lsn=max, clean_shutdown=true.
pub fn wal_replay_if_any<F>(root: &Path, mut apply_page: F) -> Result<()>
where
    F: FnMut(u64, u64, &[u8]) -> Result<()>,
{
    use crate::meta::{read_meta, set_clean_shutdown, set_last_lsn};

    let wal_path = wal_path(root);
    if !wal_path.exists() {
        return Ok(());
    }

    let mut f = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&wal_path)
        .with_context(|| format!("open wal {}", wal_path.display()))?;

    // Заголовок
    if f.metadata()?.len() < WAL_HDR_SIZE as u64 {
        // файл мал — запишем новый header
        write_wal_file_header(&mut f)?;
        f.sync_all()?;
        return Ok(());
    }
    let mut hdr16 = [0u8; WAL_HDR_SIZE];
    f.seek(SeekFrom::Start(0))?;
    f.read_exact(&mut hdr16)?;
    if &hdr16[..8] != WAL_MAGIC {
        return Err(anyhow!("bad WAL magic in {}", wal_path.display()));
    }

    // Быстрый путь при clean_shutdown
    let m = read_meta(root)?;
    if m.clean_shutdown {
        let len = f.metadata()?.len();
        if len > WAL_HDR_SIZE as u64 {
            f.set_len(WAL_HDR_SIZE as u64)?;
            f.sync_all()?;
        }
        return Ok(());
    }

    // Реплей
    let mut pos = WAL_HDR_SIZE as u64;
    let len = f.metadata()?.len();
    let mut max_lsn = m.last_lsn;

    // Каталог для HEADS_UPDATE откроем лениво
    let mut dir_lazy: Option<crate::dir::Directory> = None;

    // NEW: персистентный LSN-гейтинг для HEADS_UPDATE
    let mut last_heads_lsn = load_last_heads_lsn(root).unwrap_or(0);

    // NEW: stateful reader
    let mut reader = WalStreamReader::new();

    while let Some((rec, next_pos)) = reader.read_next(&mut f, pos, len)? {
        // Учёт max LSN
        if rec.lsn > max_lsn {
            max_lsn = rec.lsn;
        }

        match rec.rec_type {
            WAL_REC_PAGE_IMAGE => {
                // PAGE_IMAGE — делегируем вызывающему коду (LSN‑гейтинг снаружи)
                apply_page(rec.lsn, rec.page_id, &rec.payload)?;
            }
            WAL_REC_HEADS_UPDATE => {
                // LSN-гейтинг: применяем только если lsn > last_heads_lsn
                if rec.lsn > last_heads_lsn {
                    // payload = повторяющиеся [bucket u32][head_pid u64] (LE)
                    let payload_len = rec.payload.len();
                    if payload_len % 12 == 0 && payload_len > 0 {
                        // Откроем каталог (лениво)
                        if dir_lazy.is_none() {
                            dir_lazy = Some(crate::dir::Directory::open(root)?);
                        }
                        let dir = dir_lazy.as_ref().unwrap();

                        // Разберём payload в вектор апдейтов
                        let mut updates: Vec<(u32, u64)> = Vec::with_capacity(payload_len / 12);
                        let mut off = 0usize;
                        while off + 12 <= payload_len {
                            let b = LittleEndian::read_u32(&rec.payload[off..off + 4]);
                            let pid = LittleEndian::read_u64(&rec.payload[off + 4..off + 12]);
                            updates.push((b, pid));
                            off += 12;
                        }
                        if !updates.is_empty() {
                            // Отсортируем по bucket для стабильности (не обязательно)
                            updates.sort_by_key(|e| e.0);
                            dir.set_heads_bulk(&updates)?;
                            // Обновим персистентный маркер
                            last_heads_lsn = rec.lsn;
                            let _ = store_last_heads_lsn(root, last_heads_lsn);
                        }
                    }
                    // если длина некорректна — игнорируем (forward-compatible)
                } else {
                    // устаревший HEADS_UPDATE — пропускаем
                }
            }
            WAL_REC_TRUNCATE => {
                // ignore (маркер ротации в стриминге)
            }
            WAL_REC_BEGIN | WAL_REC_COMMIT | WAL_REC_PAGE_DELTA => {
                // В 2.0 PAGE_DELTA игнорируется; BEGIN/COMMIT — только маркеры.
            }
            _ => {
                // unknown — игнор
            }
        }

        pos = next_pos;
    }

    // Успешный реплей: усечём до заголовка и проставим meta
    f.set_len(WAL_HDR_SIZE as u64)?;
    f.sync_all()?;
    set_last_lsn(root, max_lsn)?;
    set_clean_shutdown(root, true)?;
    Ok(())
}
