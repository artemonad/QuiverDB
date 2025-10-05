use anyhow::{anyhow, Context, Result};
use byteorder::{ByteOrder, LittleEndian};
use crc32fast::Hasher as Crc32;
use std::fs::OpenOptions;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use crate::consts::{
    PAGE_MAGIC, WAL_FILE, WAL_HDR_SIZE, WAL_MAGIC, WAL_REC_HDR_SIZE, WAL_REC_OFF_CRC32,
    WAL_REC_OFF_LEN, WAL_REC_OFF_LSN, WAL_REC_OFF_PAGE_ID, WAL_REC_PAGE_IMAGE, WAL_REC_TRUNCATE,
};
use crate::meta::{read_meta, set_last_lsn};
use crate::pager::Pager;
use crate::page_ovf::ovf_header_read;
use crate::page_rh::rh_header_read;

/// Вытянуть LSN из v2-страницы (RH или Overflow). None, если буфер не v2/не распознан.
fn v2_page_lsn(buf: &[u8]) -> Option<u64> {
    if buf.len() < 8 {
        return None;
    }
    if &buf[..4] != PAGE_MAGIC {
        return None;
    }
    let ver = LittleEndian::read_u16(&buf[4..6]);
    if ver < 2 {
        return None;
    }
    if let Ok(h) = rh_header_read(buf) {
        Some(h.lsn)
    } else if let Ok(h) = ovf_header_read(buf) {
        Some(h.lsn)
    } else {
        None
    }
}

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
                    println!(
                        r#"{{"type":"page_image","lsn":{},"page_id":{},"len":{},"crc32":{}}}"#,
                        wal_lsn, page_id, payload_len, crc_expected
                    );
                }
                t => {
                    println!(
                        r#"{{"type":"unknown","code":{},"lsn":{},"len":{},"crc32":{}}}"#,
                        t, wal_lsn, payload_len, crc_expected
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
            thread::sleep(Duration::from_millis(150));
        }
    }

    Ok(())
}

/// Внутренний sink: stdout (по умолчанию) или TCP.
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
            } else {
                Err(anyhow!("unsupported sink '{}': use tcp://host:port or omit for stdout", s))
            }
        }
    }
}

/// Расширенная версия wal-ship: поддерживает фильтр по LSN и TCP sink.
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

    let mut out: Box<dyn Write> = open_sink(sink)?;
    out.write_all(&hdr16)?;
    out.flush()?;

    let mut pos = WAL_HDR_SIZE as u64;

    loop {
        let len = f.metadata()?.len();
        if len < pos {
            // truncate: пошлём TRUNCATE-запись, затем повторим header и начнём заново
            pos = WAL_HDR_SIZE as u64;

            // Сформируем TRUNCATE record (len=0, lsn=0, page_id=0)
            let mut tr_hdr = vec![0u8; WAL_REC_HDR_SIZE];
            tr_hdr[0] = WAL_REC_TRUNCATE;
            tr_hdr[1] = 0;
            LittleEndian::write_u16(&mut tr_hdr[2..4], 0);
            LittleEndian::write_u64(&mut tr_hdr[WAL_REC_OFF_LSN..WAL_REC_OFF_LSN + 8], 0);
            LittleEndian::write_u64(&mut tr_hdr[WAL_REC_OFF_PAGE_ID..WAL_REC_OFF_PAGE_ID + 8], 0);
            LittleEndian::write_u32(&mut tr_hdr[WAL_REC_OFF_LEN..WAL_REC_OFF_LEN + 4], 0);
            let mut hasher = Crc32::new();
            hasher.update(&tr_hdr[..WAL_REC_OFF_CRC32]);
            let crc = hasher.finalize();
            LittleEndian::write_u32(
                &mut tr_hdr[WAL_REC_OFF_CRC32..WAL_REC_OFF_CRC32 + 4],
                crc,
            );
            out.write_all(&tr_hdr)?;
            out.write_all(&hdr16)?; // повторно пошлём header
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

            let wal_lsn = LittleEndian::read_u64(&hdr[WAL_REC_OFF_LSN..WAL_REC_OFF_LSN + 8]);
            if let Some(min_lsn) = since_lsn {
                if wal_lsn <= min_lsn {
                    // Пропускаем запись, смещаемся дальше
                    pos += rec_total;
                    made_progress = true;
                    continue;
                }
            }

            // Отправим запись (hdr + payload)
            out.write_all(&hdr)?;
            out.write_all(&payload)?;
            out.flush()?;

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
/// Делает LSN-гейтинг для v2-страниц (RH и Overflow).
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

        if &first8 == WAL_MAGIC {
            let mut rest8 = [0u8; 8];
            match inp.read_exact(&mut rest8) {
                Ok(()) => {}
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        break; // поток закончился на половине заголовка — ок, завершаем
                    }
                    return Err(anyhow!("read WAL stream header (mid-stream): {e}"));
                }
            }
            continue;
        }

        let mut hdr = vec![0u8; WAL_REC_HDR_SIZE];
        hdr[..8].copy_from_slice(&first8);
        if let Err(e) = inp.read_exact(&mut hdr[8..]) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
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
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
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

                pager.ensure_allocated(page_id)?;

                let mut apply = true;
                if let Some(new_lsn) = v2_page_lsn(&payload) {
                    let mut cur = vec![0u8; pager.meta.page_size as usize];
                    if page_id < pager.meta.next_page_id {
                        if pager.read_page(page_id, &mut cur).is_ok() {
                            if let Some(cur_lsn) = v2_page_lsn(&cur) {
                                if cur_lsn >= new_lsn {
                                    apply = false;
                                }
                            }
                        }
                    }
                }
                if apply {
                    pager.write_page_raw(page_id, &payload)?;
                }
            }
            WAL_REC_TRUNCATE => {
                // ignore
            }
            _ => {
                // незнакомый тип — игнорируем (forward-совместимость)
            }
        }
    }

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

/// Утилита: вывести meta.last_lsn для резюма CDC.
pub fn cmd_cdc_last_lsn(path: PathBuf) -> Result<()> {
    let m = read_meta(&path)?;
    println!("{}", m.last_lsn);
    Ok(())
}

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

    let mut out = BufWriter::new(
        OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&out_path)
            .with_context(|| format!("open out {}", out_path.display()))?,
    );
    out.write_all(&hdr16)?;

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

        // CRC check/fix (локально не исправляем — просто фильтруем по LSN)
        let mut hasher = Crc32::new();
        hasher.update(&hdr[..WAL_REC_OFF_CRC32]);
        hasher.update(&payload);
        let crc_actual = hasher.finalize();
        if crc_expected != crc_actual {
            break;
        }

        let wal_lsn = LittleEndian::read_u64(&hdr[WAL_REC_OFF_LSN..WAL_REC_OFF_LSN + 8]);

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

/// CDC replay: применить поток WAL из файла (или stdin), с фильтром по LSN.
/// Если input_path=None — читаем stdin.
/// from_lsn: применяем только кадры с lsn > from_lsn; to_lsn: и lsn <= to_lsn (если задан).
pub fn cmd_cdc_replay(
    path: PathBuf,
    input_path: Option<PathBuf>,
    from_lsn: Option<u64>,
    to_lsn: Option<u64>,
) -> Result<()> {
    use std::io::Read;

    // Открываем источник (файл или stdin)
    let boxed_reader: Box<dyn Read> = if let Some(p) = input_path {
        let f = OpenOptions::new().read(true).open(&p)
            .with_context(|| format!("open input {}", p.display()))?;
        Box::new(f)
    } else {
        Box::new(std::io::stdin().lock())
    };

    // Обёртка поверх wal_apply_from_stream с фильтром по wal_lsn
    cdc_apply_with_lsn_filter(&path, boxed_reader, from_lsn, to_lsn)
}

/// Применение WAL потоков с фильтром по wal_lsn.
fn cdc_apply_with_lsn_filter<R: Read>(
    path: &Path,
    mut inp: R,
    from_lsn: Option<u64>,
    to_lsn: Option<u64>,
) -> Result<()> {
    let mut pager = Pager::open(path)?;

    // Заголовок
    let mut hdr16 = vec![0u8; WAL_HDR_SIZE];
    inp.read_exact(&mut hdr16)
        .context("read WAL stream header from input")?;
    if &hdr16[..8] != WAL_MAGIC {
        return Err(anyhow!("bad WAL stream header magic"));
    }

    let mut max_lsn: u64 = 0;

    loop {
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

        if &first8 == WAL_MAGIC {
            let mut rest8 = [0u8; 8];
            match inp.read_exact(&mut rest8) {
                Ok(()) => {}
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        break;
                    }
                    return Err(anyhow!("read WAL stream header (mid-stream): {e}"));
                }
            }
            continue;
        }

        let mut hdr = vec![0u8; WAL_REC_HDR_SIZE];
        hdr[..8].copy_from_slice(&first8);
        if let Err(e) = inp.read_exact(&mut hdr[8..]) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                break;
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
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                break;
            }
            return Err(anyhow!("read WAL record payload from stream: {e}"));
        }

        let mut hasher = Crc32::new();
        hasher.update(&hdr[..WAL_REC_OFF_CRC32]);
        hasher.update(&payload);
        let crc_actual = hasher.finalize();
        if crc_actual != crc_expected {
            return Err(anyhow!("WAL stream CRC mismatch, aborting"));
        }

        let wal_lsn = LittleEndian::read_u64(&hdr[WAL_REC_OFF_LSN..WAL_REC_OFF_LSN + 8]);

        // LSN-фильтр на уровне стрима (дополнительно к page LSN-гейтингу)
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
                pager.ensure_allocated(page_id)?;

                let mut apply = true;
                if let Some(new_lsn) = v2_page_lsn(&payload) {
                    let mut cur = vec![0u8; pager.meta.page_size as usize];
                    if page_id < pager.meta.next_page_id {
                        if pager.read_page(page_id, &mut cur).is_ok() {
                            if let Some(cur_lsn) = v2_page_lsn(&cur) {
                                if cur_lsn >= new_lsn {
                                    apply = false;
                                }
                            }
                        }
                    }
                }
                if apply {
                    pager.write_page_raw(page_id, &payload)?;
                }
            }
            WAL_REC_TRUNCATE => { /* ignore */ }
            _ => { /* forward-compatible: ignore */ }
        }
    }

    if max_lsn > 0 {
        let _ = set_last_lsn(path, max_lsn);
    }
    Ok(())
}