use crate::consts::{
    PAGE_MAGIC, WAL_FILE, WAL_HDR_SIZE, WAL_MAGIC, WAL_REC_HDR_SIZE, WAL_REC_OFF_CRC32,
    WAL_REC_OFF_LEN, WAL_REC_OFF_LSN, WAL_REC_OFF_PAGE_ID, WAL_REC_PAGE_IMAGE,
};
use crate::meta::{read_meta, set_last_lsn};
use crate::pager::Pager;
use crate::page_rh::rh_header_read;
use anyhow::{anyhow, Context, Result};
use byteorder::{ByteOrder, LittleEndian};
use crc32fast::Hasher as Crc32;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

// Нужен для инициализации пустого WAL (хедер) при первом старте.
use super::writer::write_wal_file_header;

/// Реплей WAL только если meta.clean_shutdown == false.
/// При clean_shutdown == true WAL просто усечётся до заголовка (быстрый старт).
/// Дополнительно: для v2-страниц применяем запись только при wal_lsn > page_lsn.
pub fn wal_replay_if_any(root: &Path) -> Result<()> {
    let wal_path = root.join(WAL_FILE);
    if !wal_path.exists() {
        return Ok(());
    }
    let mut f = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&wal_path)
        .with_context(|| format!("open wal {}", wal_path.display()))?;

    // Заголовок есть?
    if f.metadata()?.len() < WAL_HDR_SIZE as u64 {
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

    // Если завершение было чистым — реплей не нужен.
    let meta = read_meta(root)?;
    if meta.clean_shutdown {
        // Поддержим быстрый старт и компактный WAL
        let len = f.metadata()?.len();
        if len > WAL_HDR_SIZE as u64 {
            f.set_len(WAL_HDR_SIZE as u64)?;
            f.sync_all()?;
        }
        return Ok(());
    }

    // Нечистое завершение — реплеим.
    let mut pager = Pager::open(root)?;
    let mut pos = WAL_HDR_SIZE as u64;
    let len = f.metadata()?.len();
    let mut applied = 0usize;
    let mut max_lsn: u64 = 0;

    while pos + (WAL_REC_HDR_SIZE as u64) <= len {
        f.seek(SeekFrom::Start(pos))?;
        let mut hdr = vec![0u8; WAL_REC_HDR_SIZE];
        if f.read_exact(&mut hdr).is_err() {
            break;
        }
        let rec_type = hdr[0];
        let payload_len =
            LittleEndian::read_u32(&hdr[WAL_REC_OFF_LEN..WAL_REC_OFF_LEN + 4]) as usize;
        let crc_expected =
            LittleEndian::read_u32(&hdr[WAL_REC_OFF_CRC32..WAL_REC_OFF_CRC32 + 4]);
        let rec_total = WAL_REC_HDR_SIZE as u64 + payload_len as u64;
        if pos + rec_total > len {
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

                // Убедимся, что файл/сегмент физически есть и достаточного размера.
                pager.ensure_allocated(page_id)?;

                // По умолчанию применяем запись; можем отменить при v2 и wal_lsn <= page_lsn.
                let mut apply = true;

                // Гейтинг для v2-страниц: сравним LSN текущей страницы и новой.
                if payload.len() >= 8 && &payload[..4] == PAGE_MAGIC {
                    let ver = LittleEndian::read_u16(&payload[4..6]);
                    if ver >= 2 {
                        // Прочитаем текущую страницу, если она есть и корректна (CRC check внутри read_page).
                        let mut cur = vec![0u8; pager.meta.page_size as usize];
                        if page_id < pager.meta.next_page_id {
                            if pager.read_page(page_id, &mut cur).is_ok() {
                                if &cur[..4] == PAGE_MAGIC {
                                    let cur_ver = LittleEndian::read_u16(&cur[4..6]);
                                    if cur_ver >= 2 {
                                        if let (Ok(h_cur), Ok(h_new)) =
                                            (rh_header_read(&cur), rh_header_read(&payload))
                                        {
                                            // Применяем только если новая запись «свежее».
                                            if h_cur.lsn >= h_new.lsn {
                                                apply = false;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                if apply {
                    pager.write_page_raw(page_id, &payload)?;
                    applied += 1;
                }
            }
            _ => {
                // Unknown record type: stop replay to avoid misinterpreting tail
                break;
            }
        }
        pos += rec_total;
    }

    if applied > 0 {
        println!("WAL replay: applied {} record(s)", applied);
    }

    // Truncate WAL to header after successful scan.
    f.set_len(WAL_HDR_SIZE as u64)?;
    f.sync_all()?;

    // Update last_lsn in meta (best-effort).
    if max_lsn > 0 {
        let _ = set_last_lsn(root, max_lsn);
    }
    Ok(())
}