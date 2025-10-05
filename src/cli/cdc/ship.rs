use anyhow::{anyhow, Context, Result};
use byteorder::{ByteOrder, LittleEndian};
use crc32fast::Hasher as Crc32;
use flate2::{write::GzEncoder, Compression as GzCompression};
use std::fs::OpenOptions;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::net::TcpStream;
use std::path::PathBuf;

use crate::consts::{
    WAL_FILE, WAL_HDR_SIZE, WAL_MAGIC, WAL_REC_HDR_SIZE, WAL_REC_OFF_CRC32,
    WAL_REC_OFF_LEN, WAL_REC_OFF_LSN,
};

/// Внутренний sink: stdout (по умолчанию), TCP или file://path.
/// Возвращает буферизованный Writer.
fn open_sink(sink: Option<String>) -> Result<Box<dyn Write>> {
    match sink {
        None => {
            let w = BufWriter::new(std::io::stdout());
            Ok(Box::new(w))
        }
        Some(s) => {
            if let Some(addr) = s.strip_prefix("tcp://") {
                let stream = TcpStream::connect(addr)
                    .with_context(|| format!("connect tcp sink {}", addr))?;
                stream.set_nodelay(true).ok();
                let w = BufWriter::new(stream);
                Ok(Box::new(w))
            } else if let Some(path) = s.strip_prefix("file://") {
                // file sink: создаём каталог (best-effort), перезаписываем файл
                let p = PathBuf::from(path);
                if let Some(parent) = p.parent() {
                    if !parent.as_os_str().is_empty() {
                        let _ = std::fs::create_dir_all(parent);
                    }
                }
                let f = OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .open(&p)
                    .with_context(|| format!("open file sink {}", p.display()))?;
                let w = BufWriter::new(f);
                Ok(Box::new(w))
            } else {
                Err(anyhow!(
                    "unsupported sink '{}': use tcp://host:port, file://path or omit for stdout",
                    s
                ))
            }
        }
    }
}

/// Прочитать ENV-переменную булева вида.
fn env_bool(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .map(|s| s.to_ascii_lowercase())
        .map(|s| s == "1" || s == "true" || s == "yes" || s == "on")
        .unwrap_or(false)
}

/// Прочитать ENV-переменную как usize (0 если не задана/ошибка).
fn env_usize(name: &str) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
        .unwrap_or(0)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CompressKind {
    None,
    Gzip,
    Zstd,
}

fn env_compress_kind() -> Result<CompressKind> {
    match std::env::var("P1_SHIP_COMPRESS") {
        Err(_) => Ok(CompressKind::None),
        Ok(s) => {
            let v = s.trim().to_ascii_lowercase();
            match v.as_str() {
                "" | "none" => Ok(CompressKind::None),
                "gzip" => Ok(CompressKind::Gzip),
                "zstd" => Ok(CompressKind::Zstd),
                _ => Err(anyhow!(
                    "invalid P1_SHIP_COMPRESS='{}' (supported: none|gzip|zstd)",
                    s
                )),
            }
        }
    }
}

/// Обёртка над sink с опциональной компрессией.
enum CompressingSink {
    Plain(Box<dyn Write>),
    Gzip(GzEncoder<Box<dyn Write>>),
    Zstd(zstd::stream::write::Encoder<'static, Box<dyn Write>>),
}

impl CompressingSink {
    fn new(inner: Box<dyn Write>, kind: CompressKind) -> Result<Self> {
        Ok(match kind {
            CompressKind::None => CompressingSink::Plain(inner),
            CompressKind::Gzip => {
                let enc = GzEncoder::new(inner, GzCompression::fast());
                CompressingSink::Gzip(enc)
            }
            CompressKind::Zstd => {
                // level 0: библиотека подберёт подходящий уровень
                let enc = zstd::stream::write::Encoder::new(inner, 0)
                    .context("create zstd encoder")?;
                CompressingSink::Zstd(enc)
            }
        })
    }

    fn finish(self) -> Result<()> {
        match self {
            CompressingSink::Plain(mut w) => {
                w.flush().ok();
                Ok(())
            }
            CompressingSink::Gzip(g) => {
                // Запишет трейлер и вернёт внутренний writer.
                let _ = g.finish().context("gzip finish")?;
                Ok(())
            }
            CompressingSink::Zstd(z) => {
                // Завершит поток и вернёт внутренний writer.
                let _ = z.finish().context("zstd finish")?;
                Ok(())
            }
        }
    }
}

impl Write for CompressingSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            CompressingSink::Plain(w) => w.write(buf),
            CompressingSink::Gzip(w) => w.write(buf),
            CompressingSink::Zstd(w) => w.write(buf),
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            CompressingSink::Plain(w) => w.flush(),
            CompressingSink::Gzip(w) => w.flush(),
            CompressingSink::Zstd(w) => w.flush(),
        }
    }
}

/// Ship WAL-стрим в stdout/TCP/file с фильтром по LSN и batch‑flush.
/// ENV:
/// - P1_SHIP_FLUSH_EVERY=N      — flush каждые N кадров (default 1 — как раньше)
/// - P1_SHIP_FLUSH_BYTES=B      — flush при накоплении ≥ B байт (default 0 — отключено)
/// - P1_SHIP_SINCE_INCLUSIVE=1  — трактовать lsn >= N (по умолчанию lsn > N)
/// - P1_SHIP_COMPRESS=none|gzip|zstd — компрессия вывода
pub fn cmd_wal_ship_ext(
    path: PathBuf,
    follow: bool,
    since_lsn: Option<u64>,
    sink: Option<String>,
) -> Result<()> {
    let wal_path = path.join(WAL_FILE);
    let mut f = OpenOptions::new()
        .read(true)
        .open(&wal_path)
        .with_context(|| format!("open wal {}", wal_path.display()))?;

    // Проверим/прочитаем заголовок и отправим его в sink
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

    // Базовый sink (stdout/TCP/file) + опциональная компрессия
    let raw: Box<dyn Write> = open_sink(sink)?;
    let compress_kind = env_compress_kind()?;
    let mut out = CompressingSink::new(raw, compress_kind)?;

    // Пишем header
    out.write_all(&hdr16)?;
    out.flush()?; // начальный header — сразу

    // Настройки batch-flush
    let mut flush_every = env_usize("P1_SHIP_FLUSH_EVERY");
    if flush_every == 0 {
        flush_every = 1; // по умолчанию — flush на каждый кадр
    }
    let flush_bytes = env_usize("P1_SHIP_FLUSH_BYTES");

    // Inclusive since-lsn flag
    let since_inclusive = env_bool("P1_SHIP_SINCE_INCLUSIVE");

    let mut frames_since_flush: usize = 0;
    let mut bytes_since_flush: usize = 0;

    let mut pos = WAL_HDR_SIZE as u64;

    loop {
        let len = f.metadata()?.len();
        if len < pos {
            // truncate: пошлём TRUNCATE-запись, затем повторим header и начнём заново
            pos = WAL_HDR_SIZE as u64;

            // Сформируем TRUNCATE record (len=0, lsn=0, page_id=0)
            let mut tr_hdr = vec![0u8; WAL_REC_HDR_SIZE];
            tr_hdr[0] = crate::consts::WAL_REC_TRUNCATE;
            tr_hdr[1] = 0;
            LittleEndian::write_u16(&mut tr_hdr[2..4], 0);
            LittleEndian::write_u64(&mut tr_hdr[crate::consts::WAL_REC_OFF_LSN..crate::consts::WAL_REC_OFF_LSN + 8], 0);
            LittleEndian::write_u64(&mut tr_hdr[crate::consts::WAL_REC_OFF_PAGE_ID..crate::consts::WAL_REC_OFF_PAGE_ID + 8], 0);
            LittleEndian::write_u32(&mut tr_hdr[crate::consts::WAL_REC_OFF_LEN..crate::consts::WAL_REC_OFF_LEN + 4], 0);
            let mut hasher = Crc32::new();
            hasher.update(&tr_hdr[..crate::consts::WAL_REC_OFF_CRC32]);
            let crc = hasher.finalize();
            LittleEndian::write_u32(
                &mut tr_hdr[crate::consts::WAL_REC_OFF_CRC32..crate::consts::WAL_REC_OFF_CRC32 + 4],
                crc,
            );
            out.write_all(&tr_hdr)?;
            out.write_all(&hdr16)?; // повторно пошлём header
            out.flush()?; // синхронизируем границу ротации
            frames_since_flush = 0;
            bytes_since_flush = 0;
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

            let wal_lsn = LittleEndian::read_u64(&hdr[WAL_REC_OFF_LSN..WAL_REC_OFF_LSN + 8]);
            if let Some(min_lsn) = since_lsn {
                let skip = if since_inclusive {
                    wal_lsn < min_lsn
                } else {
                    wal_lsn <= min_lsn
                };
                if skip {
                    pos += rec_total;
                    made_progress = true;
                    continue;
                }
            }

            // Отправим запись (hdr + payload)
            out.write_all(&hdr)?;
            out.write_all(&payload)?;

            // учёт для batch-flush
            frames_since_flush += 1;
            bytes_since_flush += WAL_REC_HDR_SIZE + payload_len;

            let need_flush_by_frames = frames_since_flush >= flush_every;
            let need_flush_by_bytes = flush_bytes > 0 && bytes_since_flush >= flush_bytes;

            if need_flush_by_frames || need_flush_by_bytes {
                out.flush()?;
                frames_since_flush = 0;
                bytes_since_flush = 0;
            }

            pos += rec_total;
            made_progress = true;
        }

        if !follow {
            break;
        }
        if !made_progress {
            std::thread::sleep(std::time::Duration::from_millis(150));
        }
    }

    // финальный flush/finish на завершение (особенно важно при компрессии)
    out.flush().ok();
    out.finish()?;
    Ok(())
}