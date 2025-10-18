use anyhow::{anyhow, Result};
use byteorder::{ByteOrder, LittleEndian};
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::thread;

use QuiverDB::db::Db;
use QuiverDB::wal::{
    crc32c_of_parts, WAL_HDR_SIZE, WAL_MAGIC, WAL_REC_HDR_SIZE, WAL_REC_HEADS_UPDATE,
    WAL_REC_OFF_CRC32, WAL_REC_OFF_LEN, WAL_REC_OFF_LSN, WAL_REC_OFF_PAGE_ID, WAL_REC_OFF_TYPE,
    WAL_REC_PAGE_IMAGE,
};
// NEW: stateful reader вместо deprecated read_next_record
use QuiverDB::wal::encode::build_hdr_with_crc;
use QuiverDB::wal::net::{load_psk_from_env, read_next_framed_psk, write_framed_psk};
use QuiverDB::wal::reader::WalStreamReader;

use QuiverDB::meta::set_last_lsn;
use QuiverDB::page::{
    KV_HDR_MIN, KV_OFF_LSN, OFF_TYPE, OVF_OFF_LSN, PAGE_MAGIC, PAGE_TYPE_KV_RH3,
    PAGE_TYPE_OVERFLOW3,
};

fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("qdb2-{}-{}-{}", prefix, pid, t))
}

#[test]
fn cdc_loop_psk_end_to_end() -> Result<()> {
    // Быстрая среда (без лишних fsync/CRC)
    std::env::set_var("P1_WAL_DISABLE_FSYNC", "1");
    std::env::set_var("P1_DATA_FSYNC", "0");
    std::env::set_var("P1_PAGE_CHECKSUM", "0");

    // PSK (32 байта 0x44)
    std::env::set_var("P1_CDC_PSK_HEX", "44".repeat(32));

    // Подготовка путей
    let prod = unique_root("cdc-prod");
    let foll = unique_root("cdc-foll");
    fs::create_dir_all(&prod)?;
    fs::create_dir_all(&foll)?;

    // Инициализация БД
    let page_size = 64 * 1024;
    let buckets = 64;
    Db::init(&prod, page_size, buckets)?;
    Db::init(&foll, page_size, buckets)?;

    // 1) Producer: большой батч, чтобы в WAL были PAGE_IMAGE и HEADS_UPDATE
    let n = 2000usize;
    // Открываем writer и держим его до чтения WAL, чтобы Drop не усёк WAL
    let mut db_prod = Db::open(&prod)?;
    db_prod.batch(|b| {
        for i in 0..n {
            let k = format!("key-{:06}", i).into_bytes();
            let v = vec![0xAB; if i % 7 == 0 { 8192 } else { 64 }]; // часть уйдет в OVF
            b.put(&k, &v)?;
        }
        Ok(())
    })?;

    // 2) Извлекаем кадры WAL пока writer жив (иначе Drop усечёт файл до заголовка)
    let wal_path = QuiverDB::wal::wal_path(&prod);
    let mut src = OpenOptions::new().read(true).open(&wal_path)?;
    // Проверка заголовка
    {
        let mut hdr = [0u8; WAL_HDR_SIZE];
        src.seek(SeekFrom::Start(0))?;
        Read::read_exact(&mut src, &mut hdr)?;
        if &hdr[..8] != WAL_MAGIC {
            return Err(anyhow!("bad WAL magic in {}", wal_path.display()));
        }
    }
    let mut frames_buf: Vec<Vec<u8>> = Vec::new();
    let mut pos = WAL_HDR_SIZE as u64;
    let file_len = src.metadata()?.len();

    // NEW: stateful WAL reader
    let mut rdr = WalStreamReader::new();

    while let Some((rec, next_pos)) = rdr.read_next(&mut src, pos, file_len)? {
        // Сформируем bytes кадра WAL: [WAL header 28][payload]
        let mut buf = Vec::with_capacity(WAL_REC_HDR_SIZE + rec.payload.len());
        let hdr28 = build_hdr_with_crc(rec.rec_type, rec.lsn, rec.page_id, &rec.payload);
        buf.extend_from_slice(&hdr28);
        if !rec.payload.is_empty() {
            buf.extend_from_slice(&rec.payload);
        }
        frames_buf.push(buf);
        pos = next_pos;
    }
    assert!(!frames_buf.is_empty(), "producer WAL must contain frames");

    // Writer нам больше не нужен — после этого Drop усечёт WAL, но кадры уже в памяти
    drop(db_prod);

    // 3) Поднимем локальный TCP "source" сервер: отправит все фреймы подписчику (apply)
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let psk = load_psk_from_env()?;
    let frames_clone = frames_buf.clone();

    let server = thread::spawn(move || -> Result<()> {
        let (mut sock, _) = listener.accept()?;
        // Отправляем все фреймы
        for (seq, payload) in frames_clone.into_iter().enumerate() {
            let seq_u64 = (seq + 1) as u64;
            write_framed_psk(&mut sock, seq_u64, &payload, &psk)?;
        }
        Ok(())
    });

    // 4) Follower: подключается к "source" и применяет фреймы (минимальный apply‑цикл)
    apply_psk_stream(&foll, &format!("127.0.0.1:{}", addr.port()))?;

    server.join().unwrap()?; // сервер завершился

    // 5) Проверка содержимого follower
    {
        let db = Db::open_ro(&foll)?;
        for i in (0..n).step_by(123) {
            let k = format!("key-{:06}", i).into_bytes();
            let got = db.get(&k)?;
            assert!(got.is_some(), "key must exist on follower");
        }
    }

    Ok(())
}

// Минимальный apply по PSK‑стриму (PAGE_IMAGE + HEADS_UPDATE)
fn apply_psk_stream(dst: &PathBuf, host_port: &str) -> Result<()> {
    let mut db = Db::open(dst)?;
    let ps = db.pager.meta.page_size as usize;
    let mut sock = TcpStream::connect(host_port)?;
    let psk = load_psk_from_env()?;

    // Статистика — префиксуем подчёркиванием, чтобы подавить warning “never used”
    let mut _frames = 0u64;
    let mut _bytes = 0u64;
    let mut max_lsn = db.pager.meta.last_lsn;

    // Максимальный размер payload: 28 байт заголовок + сама страница (<=1MiB) + запас
    let max_len = std::cmp::max(2 * 1024 * 1024, WAL_REC_HDR_SIZE + ps);

    // Персистентные маркеры консистентности
    let mut last_heads_lsn = QuiverDB::wal::state::load_last_heads_lsn(dst).unwrap_or(0);
    let mut last_seq = QuiverDB::wal::state::load_last_seq(dst).unwrap_or(0);

    while let Some((seq, payload)) = read_next_framed_psk(&mut sock, &psk, max_len)? {
        // Проверка монотонности seq (персистентная)
        if seq <= last_seq {
            continue;
        }

        if payload.len() < WAL_REC_HDR_SIZE {
            return Err(anyhow!("short WAL frame ({} < hdr)", payload.len()));
        }

        let rec_type = payload[WAL_REC_OFF_TYPE];
        let lsn = LittleEndian::read_u64(&payload[WAL_REC_OFF_LSN..WAL_REC_OFF_LSN + 8]);
        let page_id =
            LittleEndian::read_u64(&payload[WAL_REC_OFF_PAGE_ID..WAL_REC_OFF_PAGE_ID + 8]);
        let len = LittleEndian::read_u32(&payload[WAL_REC_OFF_LEN..WAL_REC_OFF_LEN + 4]) as usize;
        let stored_crc = LittleEndian::read_u32(&payload[WAL_REC_OFF_CRC32..WAL_REC_OFF_CRC32 + 4]);

        if payload.len() != WAL_REC_HDR_SIZE + len {
            return Err(anyhow!(
                "bad WAL frame len: payload {} vs header.len {}",
                payload.len(),
                len
            ));
        }

        // CRC
        let calc_crc = crc32c_of_parts(&payload[..WAL_REC_OFF_CRC32], &payload[WAL_REC_HDR_SIZE..]);
        if calc_crc != stored_crc {
            return Err(anyhow!(
                "WAL CRC mismatch (stored={}, calc={})",
                stored_crc,
                calc_crc
            ));
        }

        // Учёт максимумов
        _frames += 1;
        _bytes += payload.len() as u64;
        if lsn > max_lsn {
            max_lsn = lsn;
        }

        match rec_type {
            t if t == WAL_REC_PAGE_IMAGE => {
                let page_bytes = &payload[WAL_REC_HDR_SIZE..];

                // LSN‑гейтинг до ensure_allocated
                let mut apply = true;
                if page_id < db.pager.meta.next_page_id {
                    let mut cur = vec![0u8; ps];
                    if db.pager.read_page(page_id, &mut cur).is_ok() {
                        if let (Some(nl), Some(cl)) = (v3_page_lsn(page_bytes), v3_page_lsn(&cur)) {
                            if cl >= nl {
                                apply = false;
                            }
                        }
                    }
                }

                if apply {
                    db.pager.ensure_allocated(page_id)?;
                    db.pager.write_page_raw(page_id, page_bytes)?;
                }
            }

            t if t == WAL_REC_HEADS_UPDATE => {
                // LSN-гейтинг: применяем только если lsn > last_heads_lsn
                if lsn > last_heads_lsn {
                    let updates = parse_heads_updates(&payload[WAL_REC_HDR_SIZE..]);
                    if !updates.is_empty() {
                        db.set_dir_heads_bulk(&updates)?;
                        last_heads_lsn = lsn;
                        let _ = QuiverDB::wal::state::store_last_heads_lsn(dst, last_heads_lsn);
                    }
                }
            }

            _ => { /* BEGIN/COMMIT/TRUNCATE/PAGE_DELTA — игнорируем */ }
        }

        // Персистентно обновим last_seq после успешной обработки кадра
        last_seq = seq;
        let _ = QuiverDB::wal::state::store_last_seq(dst, last_seq);
    }

    // best-effort: обновим meta.last_lsn
    let _ = set_last_lsn(dst, max_lsn);

    Ok(())
}

fn v3_page_lsn(buf: &[u8]) -> Option<u64> {
    if buf.len() < KV_HDR_MIN {
        return None;
    }
    if &buf[..4] != PAGE_MAGIC {
        return None;
    }
    let ptype = LittleEndian::read_u16(&buf[OFF_TYPE..OFF_TYPE + 2]);
    match ptype {
        t if t == PAGE_TYPE_KV_RH3 => {
            Some(LittleEndian::read_u64(&buf[KV_OFF_LSN..KV_OFF_LSN + 8]))
        }
        t if t == PAGE_TYPE_OVERFLOW3 => {
            Some(LittleEndian::read_u64(&buf[OVF_OFF_LSN..OVF_OFF_LSN + 8]))
        }
        _ => None,
    }
}

#[inline]
fn parse_heads_updates(buf: &[u8]) -> Vec<(u32, u64)> {
    if buf.len() % 12 != 0 || buf.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(buf.len() / 12);
    let mut off = 0usize;
    while off + 12 <= buf.len() {
        let b = LittleEndian::read_u32(&buf[off..off + 4]);
        let pid = LittleEndian::read_u64(&buf[off + 4..off + 12]);
        out.push((b, pid));
        off += 12;
    }
    out
}
