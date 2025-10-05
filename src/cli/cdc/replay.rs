use anyhow::{anyhow, Context, Result};
use byteorder::{ByteOrder, LittleEndian};
use crc32fast::Hasher as Crc32;
use std::fs::OpenOptions;
use std::io::Read;
use std::path::{Path, PathBuf};

use crate::consts::{
    WAL_HDR_SIZE, WAL_MAGIC, WAL_REC_HDR_SIZE, WAL_REC_OFF_CRC32, WAL_REC_OFF_LEN,
    WAL_REC_OFF_LSN, WAL_REC_OFF_PAGE_ID, WAL_REC_PAGE_IMAGE, WAL_REC_TRUNCATE,
};
use crate::meta::set_last_lsn;
use crate::pager::Pager;
use crate::util::v2_page_lsn;

/// CDC replay: применить поток WAL из файла (или stdin), с фильтром по LSN.
/// Если input_path=None — читаем stdin.
/// from_lsn: применяем только кадры с lsn > from_lsn (exclusive);
/// to_lsn: и lsn <= to_lsn (inclusive), если задан.
pub fn cmd_cdc_replay(
    path: PathBuf,
    input_path: Option<PathBuf>,
    from_lsn: Option<u64>,
    to_lsn: Option<u64>,
) -> Result<()> {
    // Источник (файл или stdin)
    let boxed_reader: Box<dyn Read> = if let Some(p) = input_path {
        let f = OpenOptions::new()
            .read(true)
            .open(&p)
            .with_context(|| format!("open input {}", p.display()))?;
        Box::new(f)
    } else {
        Box::new(std::io::stdin().lock())
    };

    // Применяем поток с фильтром LSN
    cdc_apply_with_lsn_filter(&path, boxed_reader, from_lsn, to_lsn)
}

/// Применение WAL-потока с фильтром по wal_lsn.
/// Правила:
/// - Требуется начальный 16-байтовый WAL header.
/// - Mid-stream header (после TRUNCATE) допускается и пропускается.
/// - Частичные хвосты (заголовок/пейлоад) — нормальный EOF.
/// - CRC в полной записи обязателен; mismatch => ошибка.
/// - Для v2-страниц действует LSN-гейтинг: new_lsn > cur_lsn.
/// Оптимизация: ensure_allocated вызывается только если запись действительно применяется.
pub fn cdc_apply_with_lsn_filter<R: Read>(
    path: &Path,
    mut inp: R,
    from_lsn: Option<u64>,
    to_lsn: Option<u64>,
) -> Result<()> {
    let mut pager = Pager::open(path)?;

    // Начальный WAL header обязателен
    let mut hdr16 = vec![0u8; WAL_HDR_SIZE];
    inp.read_exact(&mut hdr16)
        .context("read WAL stream header from input")?;
    if &hdr16[..8] != WAL_MAGIC {
        return Err(anyhow!("bad WAL stream header magic"));
    }

    let mut max_lsn: u64 = 0;

    loop {
        // Пробуем прочитать первые 8 байт (record start или WAL_MAGIC в середине)
        let mut first8 = [0u8; 8];
        match inp.read_exact(&mut first8) {
            Ok(()) => {}
            Err(e) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    break; // нормальный EOF
                }
                return Err(anyhow!("read from stream (first 8 bytes): {e}"));
            }
        }

        // Mid-stream WAL header (после TRUNCATE)
        if &first8 == WAL_MAGIC {
            let mut rest8 = [0u8; 8];
            match inp.read_exact(&mut rest8) {
                Ok(()) => {}
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        break; // оборванный заголовок — нормальный EOF
                    }
                    return Err(anyhow!("read WAL stream header (mid-stream): {e}"));
                }
            }
            continue;
        }

        // Дочитываем хвост заголовка записи
        let mut hdr = vec![0u8; WAL_REC_HDR_SIZE];
        hdr[..8].copy_from_slice(&first8);
        if let Err(e) = inp.read_exact(&mut hdr[8..]) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                break; // частичный заголовок — нормальный EOF
            }
            return Err(anyhow!("read WAL record header tail from stream: {e}"));
        }

        let rec_type = hdr[0];
        let payload_len =
            LittleEndian::read_u32(&hdr[WAL_REC_OFF_LEN..WAL_REC_OFF_LEN + 4]) as usize;
        let crc_expected =
            LittleEndian::read_u32(&hdr[WAL_REC_OFF_CRC32..WAL_REC_OFF_CRC32 + 4]);

        // Читаем payload
        let mut payload = vec![0u8; payload_len];
        if let Err(e) = inp.read_exact(&mut payload) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                break; // частичный хвост — нормальный EOF
            }
            return Err(anyhow!("read WAL record payload from stream: {e}"));
        }

        // CRC контроль
        let mut hasher = Crc32::new();
        hasher.update(&hdr[..WAL_REC_OFF_CRC32]);
        hasher.update(&payload);
        let crc_actual = hasher.finalize();
        if crc_actual != crc_expected {
            return Err(anyhow!("WAL stream CRC mismatch, aborting"));
        }

        let wal_lsn = LittleEndian::read_u64(&hdr[WAL_REC_OFF_LSN..WAL_REC_OFF_LSN + 8]);

        // Фильтр по from_lsn/to_lsn
        if let Some(min) = from_lsn {
            if wal_lsn <= min {
                continue;
            }
        }
        if let Some(maxv) = to_lsn {
            if wal_lsn > maxv {
                continue;
            }
        }

        if wal_lsn > max_lsn {
            max_lsn = wal_lsn;
        }

        match rec_type {
            WAL_REC_PAGE_IMAGE => {
                let page_id =
                    LittleEndian::read_u64(&hdr[WAL_REC_OFF_PAGE_ID..WAL_REC_OFF_PAGE_ID + 8]);

                // LSN новой страницы (если payload — v2)
                let new_lsn_opt = v2_page_lsn(&payload);

                // Предварительный LSN-гейтинг до ensure_allocated
                let mut apply = true;
                if page_id < pager.meta.next_page_id {
                    let mut cur = vec![0u8; pager.meta.page_size as usize];
                    if pager.read_page(page_id, &mut cur).is_ok() {
                        if let (Some(new_lsn), Some(cur_lsn)) = (new_lsn_opt, v2_page_lsn(&cur)) {
                            if cur_lsn >= new_lsn {
                                apply = false;
                            }
                        }
                    } else {
                        apply = true; // восстановление
                    }
                } else {
                    apply = true;
                }

                if apply {
                    pager.ensure_allocated(page_id)?;
                    pager.write_page_raw(page_id, &payload)?;
                }
            }
            WAL_REC_TRUNCATE => {
                // ignore
            }
            _ => {
                // незнакомый тип — игнорируем (forward-compatible)
            }
        }
    }

    if max_lsn > 0 {
        let _ = set_last_lsn(path, max_lsn);
    }
    Ok(())
}