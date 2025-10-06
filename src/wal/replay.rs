use anyhow::{anyhow, Context, Result};
use byteorder::{ByteOrder, LittleEndian};
use crc32fast::Hasher as Crc32;
use log::{debug, info, warn};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use super::writer::write_wal_file_header;

use crate::consts::{
    WAL_FILE, WAL_HDR_SIZE, WAL_MAGIC, WAL_REC_HDR_SIZE, WAL_REC_OFF_CRC32, WAL_REC_OFF_LEN,
    WAL_REC_OFF_LSN, WAL_REC_OFF_PAGE_ID, WAL_REC_PAGE_IMAGE,
};
use crate::meta::{read_meta, set_last_lsn};
use crate::pager::Pager;
use crate::util::v2_page_lsn;

/// Реплей WAL только если meta.clean_shutdown == false.
/// При clean_shutdown == true WAL просто усечётся до заголовка (быстрый старт).
/// Дополнительно: для v2-страниц применяем запись только при wal_lsn > page_lsn.
/// Поддерживаются v2-типы: RH и Overflow.
/// Unknown-типы кадров игнорируются (forward-совместимость).
pub fn wal_replay_if_any(root: &Path) -> Result<()> {
    let wal_path = root.join(WAL_FILE);
    if !wal_path.exists() {
        debug!("wal_replay_if_any: WAL file not found at {}, nothing to do", wal_path.display());
        return Ok(());
    }

    let mut f = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&wal_path)
        .with_context(|| format!("open wal {}", wal_path.display()))?;

    // Заголовок есть? Если нет — инициализируем.
    if f.metadata()?.len() < WAL_HDR_SIZE as u64 {
        debug!(
            "wal_replay_if_any: WAL too small (< header), writing fresh header to {}",
            wal_path.display()
        );
        write_wal_file_header(&mut f)?;
        f.sync_all()?;
        return Ok(());
    }

    let mut magic = [0u8; 8];
    f.seek(SeekFrom::Start(0))?;
    f.read_exact(&mut magic)?;
    if &magic != WAL_MAGIC {
        return Err(anyhow!("bad WAL magic in {}", wal_path.display()));
    }

    // Быстрый выход при чистом завершении: truncate до заголовка.
    let meta = read_meta(root)?;
    if meta.clean_shutdown {
        let len = f.metadata()?.len();
        if len > WAL_HDR_SIZE as u64 {
            debug!(
                "wal_replay_if_any: clean shutdown -> truncate WAL to header ({} -> {})",
                len,
                WAL_HDR_SIZE
            );
            f.set_len(WAL_HDR_SIZE as u64)?;
            f.sync_all()?;
        } else {
            debug!("wal_replay_if_any: clean shutdown, WAL already header-sized");
        }
        return Ok(());
    }

    // Нечистое завершение — реплеим.
    debug!(
        "wal_replay_if_any: unclean shutdown -> start replay from {}",
        wal_path.display()
    );
    let mut pager = Pager::open(root)?;
    let mut pos = WAL_HDR_SIZE as u64;
    let len = f.metadata()?.len();
    let mut applied = 0usize;
    let mut max_lsn: u64 = 0;

    while pos + (WAL_REC_HDR_SIZE as u64) <= len {
        f.seek(SeekFrom::Start(pos))?;
        let mut hdr = vec![0u8; WAL_REC_HDR_SIZE];
        if f.read_exact(&mut hdr).is_err() {
            // частичный хвост заголовка — нормальный конец
            debug!("wal_replay_if_any: partial header tail at off={}, stop", pos);
            break;
        }

        let rec_type = hdr[0];
        let payload_len =
            LittleEndian::read_u32(&hdr[WAL_REC_OFF_LEN..WAL_REC_OFF_LEN + 4]) as usize;
        let crc_expected =
            LittleEndian::read_u32(&hdr[WAL_REC_OFF_CRC32..WAL_REC_OFF_CRC32 + 4]);
        let rec_total = WAL_REC_HDR_SIZE as u64 + payload_len as u64;

        if pos + rec_total > len {
            // хвост неполон — выходим
            debug!(
                "wal_replay_if_any: partial record tail at off={}, need {} bytes, stop",
                pos, rec_total
            );
            break;
        }

        let mut payload = vec![0u8; payload_len];
        f.read_exact(&mut payload)?;

        // Verify CRC
        let mut hasher = Crc32::new();
        hasher.update(&hdr[..WAL_REC_OFF_CRC32]);
        hasher.update(&payload);
        let crc_actual = hasher.finalize();
        if crc_actual != crc_expected {
            // возможно, запись ещё дописывается — выходим
            warn!(
                "wal_replay_if_any: CRC mismatch at off={}, expected={}, actual={}, stop",
                pos, crc_expected, crc_actual
            );
            break;
        }

        let wal_lsn = LittleEndian::read_u64(&hdr[WAL_REC_OFF_LSN..WAL_REC_OFF_LSN + 8]);
        if wal_lsn > max_lsn {
            max_lsn = wal_lsn;
        }

        match rec_type {
            WAL_REC_PAGE_IMAGE => {
                let page_id =
                    LittleEndian::read_u64(&hdr[WAL_REC_OFF_PAGE_ID..WAL_REC_OFF_PAGE_ID + 8]);

                // Оптимизация/корректность: LSN-гейтинг ДО ensure_allocated
                // (избегаем лишних аллокаций, если кадр устарел).
                let new_lsn_opt = v2_page_lsn(&payload);
                let mut apply = true;

                if page_id < pager.meta.next_page_id {
                    let mut cur = vec![0u8; pager.meta.page_size as usize];
                    match pager.read_page(page_id, &mut cur) {
                        Ok(()) => {
                            if let (Some(new_lsn), Some(cur_lsn)) = (new_lsn_opt, v2_page_lsn(&cur)) {
                                if cur_lsn >= new_lsn {
                                    apply = false;
                                    debug!(
                                        "wal_replay_if_any: skip page {} (cur_lsn {} >= wal_lsn {})",
                                        page_id, cur_lsn, new_lsn
                                    );
                                }
                            }
                        }
                        Err(_) => {
                            // Не смогли прочитать текущую — применим запись (восстановление)
                            apply = true;
                        }
                    }
                } else {
                    // Страница логически не аллоцирована — применяем, аллоцируем при необходимости
                    apply = true;
                }

                if apply {
                    pager.ensure_allocated(page_id)?;
                    pager.write_page_raw(page_id, &payload)?;
                    applied += 1;
                }
            }
            // Unknown или будущие типы — игнорируем (forward-совместимость).
            _ => {
                debug!("wal_replay_if_any: skip unknown record type {}", rec_type);
            }
        }

        pos += rec_total;
    }

    if applied > 0 {
        info!("WAL replay: applied {} record(s)", applied);
    } else {
        debug!("WAL replay: nothing to apply");
    }

    // Truncate WAL to header после успешного прохода.
    f.set_len(WAL_HDR_SIZE as u64)?;
    f.sync_all()?;
    debug!("wal_replay_if_any: WAL truncated to header");

    // Update last_lsn в meta (best-effort).
    if max_lsn > 0 {
        let _ = set_last_lsn(root, max_lsn);
        debug!("wal_replay_if_any: set last_lsn={}", max_lsn);
    }
    Ok(())
}