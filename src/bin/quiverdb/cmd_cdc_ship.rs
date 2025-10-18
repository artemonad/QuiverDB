use anyhow::{anyhow, Context, Result};
use byteorder::{ByteOrder, LittleEndian};
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom};
use std::net::TcpStream;
use std::path::PathBuf;

use QuiverDB::wal::{
    encode,                            // build_hdr_with_crc / write_record
    registry::get_or_create_wal_inner, // для чтения stream_id
    write_wal_file_header_with_stream_id,
    WAL_HDR_SIZE,
    WAL_MAGIC,
    WAL_REC_HDR_SIZE,
};
// NEW: stateful WAL reader вместо глобальной функции
use QuiverDB::wal::reader::WalStreamReader;
// NEW: защищённый транспорт (framing + HMAC-PSK) + TLS
use QuiverDB::wal::net::{load_psk_from_env, open_tls_psk_stream, write_framed_psk, IoStream};
// NEW: персистентное состояние seq
use QuiverDB::wal::state::{load_last_seq, store_last_seq};

/// CDC ship (sink): скопировать WAL‑кадры в файл‑поток, по TCP+PSK или TLS+PSK.
///
/// Поддерживаемые sink’и:
///   - file://<path>        — записать поток WAL в файл (WAL header + кадры)
///   - tcp+psk://host:port  — отправить по TCP фреймы с HMAC (PSK из ENV)
///   - tls+psk://host:port  — отправить по TLS (native-tls) фреймы с HMAC (PSK из ENV)
///
/// Пример:
///   quiverdb cdc-ship --path ./db --to file://./wal-stream.bin
///   quiverdb cdc-ship --path ./db --to file://./wal-stream.bin --since-lsn 12345
///   quiverdb cdc-ship --path ./db --to tcp+psk://127.0.0.1:9099 --since-lsn 12345
///   quiverdb cdc-ship --path ./db --to tls+psk://127.0.0.1:9443 --since-lsn 12345
///
/// ENV:
///   P1_SHIP_SINCE_INCLUSIVE=1|true|yes|on  — трактовать --since-lsn как >=
///   P1_CDC_PSK_HEX / P1_CDC_PSK_BASE64 / P1_CDC_PSK — PSK ключ (минимум 16 байт)
///   P1_CDC_SEQ_RESET=1 — сбросить последовательность (начать с 1)
pub fn exec(path: PathBuf, to: String, since_lsn: Option<u64>) -> Result<()> {
    if let Some(dst_path) = to.strip_prefix("file://") {
        return ship_to_file(path, PathBuf::from(dst_path), since_lsn);
    }
    if let Some(addr) = to.strip_prefix("tcp+psk://") {
        return ship_to_psk_stream(path, addr, since_lsn, false);
    }
    if let Some(addr) = to.strip_prefix("tls+psk://") {
        return ship_to_psk_stream(path, addr, since_lsn, true);
    }
    Err(anyhow!(
        "unsupported sink '{}': use file://<path>, tcp+psk://host:port or tls+psk://host:port",
        to
    ))
}

// ---------------- file sink ----------------

fn ship_to_file(root: PathBuf, dst: PathBuf, since_lsn: Option<u64>) -> Result<()> {
    // Откроем исходный WAL (из корня DB)
    let wal_path = QuiverDB::wal::wal_path(&root);
    if !wal_path.exists() {
        return Err(anyhow!("WAL does not exist at {}", wal_path.display()));
    }
    let mut src = OpenOptions::new()
        .read(true)
        .open(&wal_path)
        .with_context(|| format!("open wal {}", wal_path.display()))?;

    // Валидация заголовка WAL
    if src.metadata()?.len() < WAL_HDR_SIZE as u64 {
        return Err(anyhow!("WAL too small (< header): {}", wal_path.display()));
    }
    let mut hdr = [0u8; WAL_HDR_SIZE];
    src.seek(SeekFrom::Start(0))?;
    std::io::Read::read_exact(&mut src, &mut hdr)?;
    if &hdr[..8] != WAL_MAGIC {
        return Err(anyhow!("bad WAL magic in {}", wal_path.display()));
    }

    // stream_id берём из живого WAL (WalInner), чтобы не полагаться на то,
    // что в исходном файле он корректно записан (на случай древнего header’а).
    let inner = get_or_create_wal_inner(&root)?;
    let stream_id = inner.get_stream_id();

    // Откроем sink-файл: создадим/перезапишем; запишем WAL header со stream_id.
    let mut out = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&dst)
        .with_context(|| format!("open sink {}", dst.display()))?;
    write_wal_file_header_with_stream_id(&mut out, stream_id)?;
    let _ = out.sync_all();

    // Параметры ship‑фильтра
    let since = since_lsn.unwrap_or(0);
    let inclusive = std::env::var("P1_SHIP_SINCE_INCLUSIVE")
        .ok()
        .map(|s| s.to_ascii_lowercase())
        .map(|s| s == "1" || s == "true" || s == "yes" || s == "on")
        .unwrap_or(false);

    // Идём по кадрам stateful‑ридером
    let mut pos = WAL_HDR_SIZE as u64;
    let file_len = src.metadata()?.len();
    let mut frames = 0u64;
    let mut bytes = 0u64;
    let mut max_lsn = 0u64;

    let mut rdr = WalStreamReader::new();

    while let Some((rec, next_pos)) = rdr.read_next(&mut src, pos, file_len)? {
        // LSN фильтр
        let pass = if inclusive {
            rec.lsn >= since
        } else {
            rec.lsn > since
        };
        if pass {
            // Запишем кадр в sink (заголовок+payload со свежей CRC)
            let before = out.metadata()?.len();
            encode::write_record(&mut out, rec.rec_type, rec.lsn, rec.page_id, &rec.payload)?;
            let after = out.metadata()?.len();
            frames += 1;
            bytes += after.saturating_sub(before);
            if rec.lsn > max_lsn {
                max_lsn = rec.lsn;
            }
        }
        pos = next_pos;
    }

    let _ = out.sync_all();

    println!(
        "cdc-ship[file]: wrote {} frames, {} bytes to {}, last_lsn={} (since_lsn={}{}), src={}, stream_id={}",
        frames,
        bytes,
        dst.display(),
        max_lsn,
        since,
        if inclusive { " (inclusive)" } else { "" },
        wal_path.display(),
        stream_id
    );
    Ok(())
}

// ---------------- TCP/TLS+PSK sink ----------------

fn ship_to_psk_stream(
    root: PathBuf,
    addr: &str,
    since_lsn: Option<u64>,
    use_tls: bool,
) -> Result<()> {
    // Откроем источник WAL (как в file sink)
    let wal_path = QuiverDB::wal::wal_path(&root);
    if !wal_path.exists() {
        return Err(anyhow!("WAL does not exist at {}", wal_path.display()));
    }
    let mut src = OpenOptions::new()
        .read(true)
        .open(&wal_path)
        .with_context(|| format!("open wal {}", wal_path.display()))?;

    if src.metadata()?.len() < WAL_HDR_SIZE as u64 {
        return Err(anyhow!("WAL too small (< header): {}", wal_path.display()));
    }
    let mut hdr = [0u8; WAL_HDR_SIZE];
    src.seek(SeekFrom::Start(0))?;
    std::io::Read::read_exact(&mut src, &mut hdr)?;
    if &hdr[..8] != WAL_MAGIC {
        return Err(anyhow!("bad WAL magic in {}", wal_path.display()));
    }

    // Настроим HMAC‑ключ (PSK)
    let psk = load_psk_from_env()?;

    // Транспорт: TLS или TCP
    let mut stream = if use_tls {
        open_tls_psk_stream(addr)?
    } else {
        let tcp =
            TcpStream::connect(addr).with_context(|| format!("connect tcp+psk sink {}", addr))?;
        let _ = tcp.set_nodelay(true);
        IoStream::Plain(tcp)
    };

    // Получим актуальный stream_id из WalInner
    let inner = get_or_create_wal_inner(&root)?;
    let stream_id = inner.get_stream_id();

    // Фильтр
    let since = since_lsn.unwrap_or(0);
    let inclusive = std::env::var("P1_SHIP_SINCE_INCLUSIVE")
        .ok()
        .map(|s| s.to_ascii_lowercase())
        .map(|s| s == "1" || s == "true" || s == "yes" || s == "on")
        .unwrap_or(false);

    // Последовательность кадров (персистентная)
    let mut seq = if std::env::var("P1_CDC_SEQ_RESET")
        .ok()
        .map(|s| s.to_ascii_lowercase())
        .map(|s| s == "1" || s == "true" || s == "yes" || s == "on")
        .unwrap_or(false)
    {
        // Ручной сброс последовательности
        let _ = store_last_seq(&root, 0);
        1u64
    } else {
        // Продолжим с сохранённого seq + 1
        load_last_seq(&root).unwrap_or(0).wrapping_add(1)
    };

    // 0) HELLO‑фрейм со stream_id: отправим один PSK‑кадр с 16‑байтовым WAL header (MAGIC + stream_id)
    let mut hello = Vec::with_capacity(WAL_HDR_SIZE);
    hello.extend_from_slice(WAL_MAGIC);
    let mut sid = [0u8; 8];
    LittleEndian::write_u64(&mut sid, stream_id);
    hello.extend_from_slice(&sid);

    write_framed_psk(&mut stream, seq, &hello, &psk)?;
    let _ = store_last_seq(&root, seq);
    seq = seq.wrapping_add(1);

    // Перебор кадров WAL stateful‑ридером
    let mut pos = WAL_HDR_SIZE as u64;
    let file_len = src.metadata()?.len();

    let mut frames = 0u64;
    let mut bytes = 0u64;
    let mut max_lsn = 0u64;

    let mut rdr = WalStreamReader::new();

    while let Some((rec, next_pos)) = rdr.read_next(&mut src, pos, file_len)? {
        let pass = if inclusive {
            rec.lsn >= since
        } else {
            rec.lsn > since
        };
        if pass {
            // Сформируем bytes кадра WAL: [WAL header 28][payload]
            let mut buf = Vec::with_capacity(WAL_REC_HDR_SIZE + rec.payload.len());
            let hdr28 =
                encode::build_hdr_with_crc(rec.rec_type, rec.lsn, rec.page_id, &rec.payload);
            buf.extend_from_slice(&hdr28);
            if !rec.payload.is_empty() {
                buf.extend_from_slice(&rec.payload);
            }

            // Отправим фрейм [header(len,seq,mac)][buf]
            write_framed_psk(&mut stream, seq, &buf, &psk)?;
            // Персистентно зафиксируем seq после успешной отправки
            let _ = store_last_seq(&root, seq);
            seq = seq.wrapping_add(1);

            frames += 1;
            bytes += buf.len() as u64;
            if rec.lsn > max_lsn {
                max_lsn = rec.lsn;
            }
        }
        pos = next_pos;
    }

    println!(
        "cdc-ship[{}+psk]: sent {} frames (+1 hello), {} bytes to {}, last_lsn={} (since_lsn={}{}), src={}, stream_id={}",
        if use_tls { "tls" } else { "tcp" },
        frames,
        bytes,
        addr,
        max_lsn,
        since,
        if inclusive { " (inclusive)" } else { "" },
        wal_path.display(),
        stream_id
    );

    Ok(())
}
