use anyhow::{anyhow, Context, Result};
use byteorder::{ByteOrder, LittleEndian};
use crc32fast::Hasher as Crc32;
use std::fs::OpenOptions;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use crate::consts::{
    WAL_FILE, WAL_HDR_SIZE, WAL_MAGIC, WAL_REC_HDR_SIZE, WAL_REC_OFF_CRC32, WAL_REC_OFF_LEN,
    WAL_REC_OFF_LSN,
};

/// CDC record: сохранить текущее содержимое WAL в файл (wire-формат), с фильтром по LSN.
/// from_lsn: шлём только кадры с lsn > from_lsn; to_lsn: если задан, то lsn <= to_lsn.
/// follow не поддерживаем (детерминированный срез WAL).
pub fn cmd_cdc_record(
    path: PathBuf,
    out_path: PathBuf,
    from_lsn: Option<u64>,
    to_lsn: Option<u64>,
) -> Result<()> {
    let wal_path = path.join(WAL_FILE);
    let mut f = OpenOptions::new()
        .read(true)
        .open(&wal_path)
        .with_context(|| format!("open wal {}", wal_path.display()))?;

    // Заголовок WAL
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

    // Выходной файл (перезапись)
    let mut out = BufWriter::new(
        OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&out_path)
            .with_context(|| format!("open out {}", out_path.display()))?,
    );
    out.write_all(&hdr16)?;

    // Скан записей WAL
    let mut pos = WAL_HDR_SIZE as u64;
    let len = f.metadata()?.len();
    let mut kept = 0usize;

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
        let rec_total = WAL_REC_HDR_SIZE as u64 + (payload_len as u64);
        if pos + rec_total > len {
            break;
        }
        let mut payload = vec![0u8; payload_len];
        f.read_exact(&mut payload)?;

        // CRC контроль (не исправляем — фильтруем по LSN только валидные фреймы)
        let mut hasher = Crc32::new();
        hasher.update(&hdr[..WAL_REC_OFF_CRC32]);
        hasher.update(&payload);
        let crc_actual = hasher.finalize();
        if crc_expected != crc_actual {
            break;
        }

        let wal_lsn = LittleEndian::read_u64(&hdr[WAL_REC_OFF_LSN..WAL_REC_OFF_LSN + 8]);

        // LSN‑фильтр (exclusive для from_lsn; inclusive для to_lsn)
        let mut pass = true;
        if let Some(min) = from_lsn {
            if wal_lsn <= min {
                pass = false;
            }
        }
        if pass {
            if let Some(max) = to_lsn {
                if wal_lsn > max {
                    pass = false;
                }
            }
        }

        if pass {
            out.write_all(&hdr)?;
            out.write_all(&payload)?;
            kept += 1;
        }

        pos += rec_total;
    }

    out.flush()?;
    println!("Recorded {} frame(s) to {}", kept, out_path.display());
    Ok(())
}