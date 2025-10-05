use anyhow::{anyhow, Context, Result};
use byteorder::{ByteOrder, LittleEndian};
use crc32fast::Hasher as Crc32;
use flate2::read::GzDecoder;
use std::io::Read;
use std::path::{Path, PathBuf};
use zstd::stream::read::Decoder as ZstdDecoder;

use crate::consts::{
    WAL_HDR_SIZE, WAL_MAGIC, WAL_REC_HDR_SIZE, WAL_REC_OFF_CRC32, WAL_REC_OFF_LEN,
    WAL_REC_OFF_LSN, WAL_REC_OFF_PAGE_ID, WAL_REC_PAGE_IMAGE, WAL_REC_TRUNCATE,
};
use crate::meta::{read_meta, set_last_lsn};
use crate::pager::Pager;
use crate::util::v2_page_lsn;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DecompressKind {
    None,
    Gzip,
    Zstd,
}

fn env_decompress_kind() -> Result<DecompressKind> {
    match std::env::var("P1_APPLY_DECOMPRESS") {
        Err(_) => Ok(DecompressKind::None),
        Ok(s) => {
            let v = s.trim().to_ascii_lowercase();
            match v.as_str() {
                "" | "none" => Ok(DecompressKind::None),
                "gzip" => Ok(DecompressKind::Gzip),
                "zstd" => Ok(DecompressKind::Zstd),
                _ => Err(anyhow!(
                    "invalid P1_APPLY_DECOMPRESS='{}' (supported: none|gzip|zstd)",
                    s
                )),
            }
        }
    }
}

/// Применить WAL-поток из произвольного Read (идемпотентно).
/// Правила:
/// - Требуется начальный 16-байтовый WAL header.
/// - Допускается mid-stream header (после TRUNCATE) — пропускается.
/// - Частичные хвосты (неполный заголовок/пейлоад) считаются нормальным EOF.
/// - CRC в полной записи обязателен; mismatch => ошибка.
/// - Для v2-страниц действует LSN-гейтинг: применяем только если new_lsn > cur_lsn.
/// Оптимизация: LSN-гейтинг выполняется до ensure_allocated, чтобы избегать лишних аллокаций.
pub fn wal_apply_from_stream<R: Read>(path: &Path, mut inp: R) -> Result<()> {
    let mut pager = Pager::open(path)?;

    // Обязательный стартовый WAL header
    let mut hdr16 = vec![0u8; WAL_HDR_SIZE];
    inp.read_exact(&mut hdr16)
        .context("read WAL stream header from input")?;
    if &hdr16[..8] != WAL_MAGIC {
        return Err(anyhow!("bad WAL stream header magic"));
    }

    let mut max_lsn: u64 = read_meta(path)?.last_lsn;

    loop {
        // Читаем первые 8 байт — либо это начало записи, либо mid-stream header (MAGIC)
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
                        break; // оборван ровно посреди заголовка — завершаем
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
                break; // частичный хвост заголовка — нормальный EOF
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
                break; // частичный хвост payload — нормальный EOF
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
        if wal_lsn > max_lsn {
            max_lsn = wal_lsn;
        }

        match rec_type {
            WAL_REC_PAGE_IMAGE => {
                let page_id =
                    LittleEndian::read_u64(&hdr[WAL_REC_OFF_PAGE_ID..WAL_REC_OFF_PAGE_ID + 8]);

                // Предварительный LSN новой страницы (если payload — v2)
                let new_lsn_opt = v2_page_lsn(&payload);

                // LSN-гейтинг: сначала сравним с текущей страницей (если она логически существует)
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
                        // Не смогли прочитать текущую — применим запись (восстановление)
                        apply = true;
                    }
                } else {
                    // Страница логически не аллоцирована — применяем
                    apply = true;
                }

                if apply {
                    // Аллоцируем только при реальном применении
                    pager.ensure_allocated(page_id)?;
                    pager.write_page_raw(page_id, &payload)?;
                }
            }
            WAL_REC_TRUNCATE => {
                // ignore (служебный маркер ротации в ship‑стриме)
            }
            _ => {
                // forward‑совместимость: незнакомые типы игнорируем
            }
        }
    }

    if max_lsn > 0 {
        let _ = set_last_lsn(path, max_lsn);
    }
    Ok(())
}

/// CLI-обёртка: читает WAL‑стрим из stdin (с опциональной декомпрессией) и применяет его к БД path.
pub fn cmd_wal_apply(path: PathBuf) -> Result<()> {
    // stdin как базовый источник
    let stdin = std::io::stdin();
    let base = stdin.lock();

    // Выбираем декомпрессию по ENV
    let dec_kind = env_decompress_kind()?;
    match dec_kind {
        DecompressKind::None => {
            // Без декомпрессии
            wal_apply_from_stream(&path, base)
        }
        DecompressKind::Gzip => {
            // Gzip decoder поверх stdin
            let dec = GzDecoder::new(base);
            wal_apply_from_stream(&path, dec)
        }
        DecompressKind::Zstd => {
            // Zstd decoder поверх stdin
            let dec = ZstdDecoder::new(base).context("create zstd decoder")?;
            wal_apply_from_stream(&path, dec)
        }
    }
}