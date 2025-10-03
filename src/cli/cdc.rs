use anyhow::{anyhow, Context, Result};
use byteorder::{ByteOrder, LittleEndian};
use crc32fast::Hasher as Crc32;
use std::fs::OpenOptions;
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use crate::consts::{
    PAGE_MAGIC, WAL_FILE, WAL_HDR_SIZE, WAL_MAGIC, WAL_REC_HDR_SIZE, WAL_REC_OFF_CRC32,
    WAL_REC_OFF_LEN, WAL_REC_OFF_LSN, WAL_REC_OFF_PAGE_ID, WAL_REC_PAGE_IMAGE,
};
use crate::meta::set_last_lsn;
use crate::pager::Pager;
use crate::page_rh::rh_header_read;

/// Печать WAL-кадров как JSONL. Если follow=true — «подписка» на новые записи.
pub fn cmd_wal_tail(path: PathBuf, follow: bool) -> Result<()> {
    let wal_path = path.join(WAL_FILE);
    let mut f = OpenOptions::new()
        .read(true)
        .open(&wal_path)
        .with_context(|| format!("open wal {}", wal_path.display()))?;

    // Проверим заголовок
    if f.metadata()?.len() < WAL_HDR_SIZE as u64 {
        return Err(anyhow!(
            "WAL too small (< header), path={}",
            wal_path.display()
        ));
    }
    let mut magic = [0u8; 8];
    f.seek(SeekFrom::Start(0))?;
    f.read_exact(&mut magic)?;
    if &magic != WAL_MAGIC {
        return Err(anyhow!("bad WAL magic in {}", wal_path.display()));
    }

    // Начинаем со следующего байта после хедера
    let mut pos = WAL_HDR_SIZE as u64;

    loop {
        let len = f.metadata()?.len();
        // Если truncate до хедера — сообщим и прыгнем к началу
        if len < pos {
            pos = WAL_HDR_SIZE as u64;
            println!(r#"{{"event":"truncate","offset":{}}}"#, pos);
        }

        // Вычитаем всё доступное целиком
        let mut made_progress = false;
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
                // Частичная запись — ждём дозаписи
                break;
            }

            let mut payload = vec![0u8; payload_len];
            f.read_exact(&mut payload)?;

            // CRC
            let mut hasher = Crc32::new();
            hasher.update(&hdr[..WAL_REC_OFF_CRC32]);
            hasher.update(&payload);
            let crc_actual = hasher.finalize();
            if crc_actual != crc_expected {
                // Хвост ещё пишется — ждём
                break;
            }

            let wal_lsn = LittleEndian::read_u64(&hdr[WAL_REC_OFF_LSN..WAL_REC_OFF_LSN + 8]);
            match rec_type {
                WAL_REC_PAGE_IMAGE => {
                    let page_id = LittleEndian::read_u64(
                        &hdr[WAL_REC_OFF_PAGE_ID..WAL_REC_OFF_PAGE_ID + 8],
                    );
                    // Печатаем JSONL (без зависимостей на serde)
                    println!(
                        r#"{{"type":"page_image","lsn":{},"page_id":{},"len":{},"crc32":{}}}"#,
                        wal_lsn, page_id, payload_len, crc_expected
                    );
                }
                _ => {
                    println!(
                        r#"{{"type":"unknown","code":{},"lsn":{},"len":{},"crc32":{}}}"#,
                        rec_type, wal_lsn, payload_len, crc_expected
                    );
                }
            }

            pos += rec_total;
            made_progress = true;
        }

        if !follow {
            break;
        }
        if !made_progress {
            // Немного подождём и попробуем снова
            thread::sleep(Duration::from_millis(150));
        }
    }

    Ok(())
}

/// wal-ship: побайтово «стримит» WAL-кадры на stdout в исходном бинарном формате:
/// - сначала пишет 16-байтовый заголовок WAL (как в файле),
/// - затем повторяет последовательность [record header (28 B) + payload].
/// Если follow=true — продолжает слать новые записи, реагируя на truncate (переотправляет header).
pub fn cmd_wal_ship(path: PathBuf, follow: bool) -> Result<()> {
    let wal_path = path.join(WAL_FILE);
    let mut f = OpenOptions::new()
        .read(true)
        .open(&wal_path)
        .with_context(|| format!("open wal {}", wal_path.display()))?;

    // Проверим/прочитаем заголовок и отправим его в stdout
    if f.metadata()?.len() < WAL_HDR_SIZE as u64 {
        return Err(anyhow!(
            "WAL too small (< header), path={}",
            wal_path.display()
        ));
    }
    let mut hdr16 = vec![0u8; WAL_HDR_SIZE];
    f.seek(SeekFrom::Start(0))?;
    f.read_exact(&mut hdr16)?;
    if &hdr16[..8] != WAL_MAGIC {
        return Err(anyhow!("bad WAL magic in {}", wal_path.display()));
    }
    {
        let mut out = std::io::stdout().lock();
        out.write_all(&hdr16)?;
        out.flush()?;
    }

    let mut pos = WAL_HDR_SIZE as u64;

    loop {
        let len = f.metadata()?.len();
        if len < pos {
            // truncate: переотправим header и начнём заново
            pos = WAL_HDR_SIZE as u64;
            let mut out = std::io::stdout().lock();
            out.write_all(&hdr16)?;
            out.flush()?;
        }

        let mut made_progress = false;
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
            let rec_total = WAL_REC_HDR_SIZE as u64 + payload_len as u64;
            if pos + rec_total > len {
                break;
            }
            let mut payload = vec![0u8; payload_len];
            f.read_exact(&mut payload)?;

            // Проверим CRC, чтобы не послать полубитый хвост
            let mut hasher = Crc32::new();
            hasher.update(&hdr[..WAL_REC_OFF_CRC32]);
            hasher.update(&payload);
            let crc_actual = hasher.finalize();
            if crc_actual != crc_expected {
                break;
            }

            // Отправим запись (hdr + payload)
            {
                let mut out = std::io::stdout().lock();
                out.write_all(&hdr)?;
                out.write_all(&payload)?;
                out.flush()?;
            }

            pos += rec_total;
            made_progress = true;
        }

        if !follow {
            break;
        }
        if !made_progress {
            thread::sleep(Duration::from_millis(150));
        }
    }

    Ok(())
}

/// Внутренняя функция: применяет WAL-поток из произвольного Read.
/// Устойчива к:
/// - повторной отправке 16-байтового заголовка WAL в середине потока (после truncate),
/// - частичному хвосту потока (недочитанный заголовок/пейлоад) — трактуется как нормальный EOF.
pub fn wal_apply_from_stream<R: Read>(path: &Path, mut inp: R) -> Result<()> {
    let mut pager = Pager::open(path)?;

    // Считаем и проверим первый header (обязателен в начале)
    let mut hdr16 = vec![0u8; WAL_HDR_SIZE];
    inp.read_exact(&mut hdr16)
        .context("read WAL stream header from input")?;
    if &hdr16[..8] != WAL_MAGIC {
        return Err(anyhow!("bad WAL stream header magic"));
    }

    let mut max_lsn: u64 = 0;

    loop {
        // На границе записей: читаем 8 байт и проверяем:
        // - если это WAL_MAGIC — дочитываем ещё 8 байт заголовка и продолжаем (truncate/rotate);
        // - иначе — это начало заголовка записи (первые 8 байт), дочитываем оставшиеся 20.
        let mut first8 = [0u8; 8];
        match inp.read_exact(&mut first8) {
            Ok(()) => {}
            Err(e) => {
                if e.kind() == ErrorKind::UnexpectedEof {
                    break; // нормальный EOF
                }
                return Err(anyhow!("read from stream (first 8 bytes): {e}"));
            }
        }

        if &first8 == WAL_MAGIC {
            // Повторный 16-байтовый заголовок WAL — дочитываем и игнорируем.
            let mut rest8 = [0u8; 8];
            match inp.read_exact(&mut rest8) {
                Ok(()) => {}
                Err(e) => {
                    if e.kind() == ErrorKind::UnexpectedEof {
                        break; // поток закончился на половине заголовка — ок, завершаем
                    }
                    return Err(anyhow!("read WAL stream header (mid-stream): {e}"));
                }
            }
            // Переходим к следующей итерации — ждём следующий кадр.
            continue;
        }

        // Собираем полный заголовок записи (28 байт), первые 8 уже в first8.
        let mut hdr = vec![0u8; WAL_REC_HDR_SIZE];
        hdr[..8].copy_from_slice(&first8);
        if let Err(e) = inp.read_exact(&mut hdr[8..]) {
            if e.kind() == ErrorKind::UnexpectedEof {
                break; // частичный хвост заголовка — завершаем без ошибки
            }
            return Err(anyhow!("read WAL record header tail from stream: {e}"));
        }

        let rec_type = hdr[0];
        let payload_len =
            LittleEndian::read_u32(&hdr[WAL_REC_OFF_LEN..WAL_REC_OFF_LEN + 4]) as usize;
        let crc_expected =
            LittleEndian::read_u32(&hdr[WAL_REC_OFF_CRC32..WAL_REC_OFF_CRC32 + 4]);

        let mut payload = vec![0u8; payload_len];
        if let Err(e) = inp.read_exact(&mut payload) {
            if e.kind() == ErrorKind::UnexpectedEof {
                break; // частичный хвост payload — завершаем без ошибки
            }
            return Err(anyhow!("read WAL record payload from stream: {e}"));
        }

        // CRC
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
                // Убедимся, что страница физически доступна
                pager.ensure_allocated(page_id)?;

                // LSN-гейтинг для v2
                let mut apply = true;
                if payload.len() >= 8 && &payload[..4] == PAGE_MAGIC {
                    let ver = LittleEndian::read_u16(&payload[4..6]);
                    if ver >= 2 {
                        // Попробуем прочитать текущую страницу (CRC внутри read_page)
                        let mut cur = vec![0u8; pager.meta.page_size as usize];
                        if page_id < pager.meta.next_page_id {
                            if pager.read_page(page_id, &mut cur).is_ok() {
                                if &cur[..4] == PAGE_MAGIC {
                                    let cur_ver = LittleEndian::read_u16(&cur[4..6]);
                                    if cur_ver >= 2 {
                                        if let (Ok(h_cur), Ok(h_new)) =
                                            (rh_header_read(&cur), rh_header_read(&payload))
                                        {
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
                }
            }
            _ => {
                // незнакомый тип — игнорируем (для будущей совместимости)
            }
        }
    }

    // Обновим last_lsn в meta (best-effort)
    if max_lsn > 0 {
        let _ = set_last_lsn(path, max_lsn);
    }
    Ok(())
}

/// CLI-обёртка: читает из stdin и делегирует в wal_apply_from_stream.
pub fn cmd_wal_apply(path: PathBuf) -> Result<()> {
    let stdin = std::io::stdin();
    let mut inp = stdin.lock();
    wal_apply_from_stream(&path, &mut inp)
}