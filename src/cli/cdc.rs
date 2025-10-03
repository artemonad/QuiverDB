use anyhow::{anyhow, Context, Result};
use byteorder::{ByteOrder, LittleEndian};
use crc32fast::Hasher as Crc32;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use crate::consts::{
    WAL_FILE, WAL_HDR_SIZE, WAL_MAGIC, WAL_REC_HDR_SIZE, WAL_REC_OFF_CRC32, WAL_REC_OFF_LEN,
    WAL_REC_OFF_LSN, WAL_REC_OFF_PAGE_ID, WAL_REC_PAGE_IMAGE,
};

/// Печать WAL-кадров как JSONL. Если follow=true — «подписка» на новые записи.
pub fn cmd_wal_tail(path: PathBuf, follow: bool) -> Result<()> {
    let wal_path = path.join(WAL_FILE);
    let mut f = OpenOptions::new()
        .read(true)
        .open(&wal_path)
        .with_context(|| format!("open wal {}", wal_path.display()))?;

    // Проверим/инициализируем заголовок
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
                    // Печатаем JSONL (без зависимость на serde)
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
            // немножко «поспим» и попробуем снова
            thread::sleep(Duration::from_millis(150));
        }
    }

    Ok(())
}