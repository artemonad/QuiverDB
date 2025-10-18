use anyhow::{anyhow, Result};
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use byteorder::{ByteOrder, LittleEndian};

use QuiverDB::db::Db;
use QuiverDB::meta::set_last_lsn;
use QuiverDB::page::{
    KV_HDR_MIN, KV_OFF_LSN, OFF_TYPE, OVF_OFF_LSN, PAGE_MAGIC, PAGE_TYPE_KV_RH3,
    PAGE_TYPE_OVERFLOW3,
};
use QuiverDB::wal::state::{
    load_last_heads_lsn, load_stream_id, store_last_heads_lsn, store_stream_id,
};
use QuiverDB::wal::{
    reader::WalStreamReader, wal_header_read_stream_id, WAL_HDR_SIZE, WAL_MAGIC,
    WAL_REC_HEADS_UPDATE, WAL_REC_PAGE_IMAGE,
};

// ---------------- helpers ----------------

fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("qdb2-{}-{}-{}", prefix, pid, t))
}

fn unique_file(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("qdb2-{}-{}-{}.wal", prefix, pid, t))
}

#[inline]
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

// ---------------- tests ----------------

#[test]
fn cdc_stream_id_file_mismatch() -> Result<()> {
    // Producer
    let prod = unique_root("cdc-sid-prod");
    fs::create_dir_all(&prod)?;
    Db::init(&prod, 64 * 1024, 64)?;

    let n = 128usize;
    let mut db_prod = Db::open(&prod)?;
    db_prod.batch(|b| {
        for i in 0..n {
            let k = format!("key-{:06}", i).into_bytes();
            let v = vec![if i % 2 == 0 { 0xAB } else { 0xCD }; if i % 7 == 0 { 9000 } else { 64 }];
            b.put(&k, &v)?;
        }
        Ok(())
    })?;

    // Скопируем WAL в sink и прочитаем stream_id
    let wal_path = QuiverDB::wal::wal_path(&prod);
    let mut src = OpenOptions::new().read(true).open(&wal_path)?;
    let mut wal_bytes = Vec::new();
    src.seek(SeekFrom::Start(0))?;
    src.read_to_end(&mut wal_bytes)?;
    let sid = {
        let mut f = OpenOptions::new().read(true).open(&wal_path)?;
        wal_header_read_stream_id(&mut f)?
    };
    assert!(sid != 0, "producer stream_id must be non-zero");

    let sink = unique_file("cdc-sid-sink.bin");
    {
        let mut out = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&sink)?;
        out.write_all(&wal_bytes)?;
        out.flush()?;
    }

    // Follower с уже записанным другим stream_id
    let foll = unique_root("cdc-sid-foll-mismatch");
    fs::create_dir_all(&foll)?;
    Db::init(&foll, 64 * 1024, 64)?;
    store_stream_id(&foll, sid.wrapping_add(1))?;

    // Проверим, что валидация источника отклоняет поток (anti-mix)
    let incoming_sid = {
        let mut f = OpenOptions::new().read(true).open(&sink)?;
        wal_header_read_stream_id(&mut f)?
    };
    assert_eq!(incoming_sid, sid);

    // emulate verify_and_store_stream_id(...)
    let local = load_stream_id(&foll)?;
    let verify_err = if local == 0 {
        store_stream_id(&foll, incoming_sid).err()
    } else if local == incoming_sid {
        None
    } else {
        Some(anyhow!(
            "WAL stream source mismatch: local stream_id={} vs incoming={}",
            local,
            incoming_sid
        ))
    };
    assert!(verify_err.is_some(), "mismatch must be detected");

    // Закроем writer (после копирования WAL)
    drop(db_prod);
    Ok(())
}

#[test]
fn cdc_stream_id_file_apply_ok() -> Result<()> {
    // Producer
    let prod = unique_root("cdc-sid2-prod");
    fs::create_dir_all(&prod)?;
    Db::init(&prod, 64 * 1024, 64)?;

    let n = 64usize;
    let mut db_prod = Db::open(&prod)?;
    db_prod.batch(|b| {
        for i in 0..n {
            let k = format!("key2-{:06}", i).into_bytes();
            let v = vec![0xEE; if i % 5 == 0 { 16 * 1024 } else { 128 }];
            b.put(&k, &v)?;
        }
        Ok(())
    })?;

    // Скопируем WAL в sink
    let wal_path = QuiverDB::wal::wal_path(&prod);
    let mut src = OpenOptions::new().read(true).open(&wal_path)?;
    let mut wal_bytes = Vec::new();
    src.seek(SeekFrom::Start(0))?;
    src.read_to_end(&mut wal_bytes)?;
    let incoming_sid = {
        let mut f = OpenOptions::new().read(true).open(&wal_path)?;
        wal_header_read_stream_id(&mut f)?
    };
    assert!(incoming_sid != 0);

    let sink = unique_file("cdc-sid2-sink.bin");
    {
        let mut out = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&sink)?;
        out.write_all(&wal_bytes)?;
        out.flush()?;
    }

    // Follower (stream_id ещё не задан) — валидируем и сохраняем sid, затем применяем
    let foll = unique_root("cdc-sid2-foll");
    fs::create_dir_all(&foll)?;
    Db::init(&foll, 64 * 1024, 64)?;

    // Валидация источника (как в apply_from_file)
    {
        let mut f = OpenOptions::new().read(true).open(&sink)?;
        let sid = wal_header_read_stream_id(&mut f)?;
        let local = load_stream_id(&foll)?;
        if local == 0 {
            store_stream_id(&foll, sid)?;
        } else if local != sid {
            panic!("mismatch unexpected");
        }
    }

    // Минимальный apply WAL файла (по образцу cmd_cdc_apply.rs)
    apply_wal_file(&foll, &sink)?;

    // Проверим несколько ключей
    let dbr = Db::open_ro(&foll)?;
    for i in (0..n).step_by(7) {
        let k = format!("key2-{:06}", i).into_bytes();
        let got = dbr.get(&k)?;
        assert!(got.is_some(), "key must exist after apply");
    }

    drop(db_prod); // только после успешной проверки
    Ok(())
}

fn apply_wal_file(dst_root: &PathBuf, src_file: &PathBuf) -> Result<()> {
    let mut f = OpenOptions::new().read(true).open(src_file)?;

    // Проверим заголовок WAL
    if f.metadata()?.len() < WAL_HDR_SIZE as u64 {
        return Err(anyhow!(
            "source too small (< WAL header): {}",
            src_file.display()
        ));
    }
    let mut hdr = [0u8; WAL_HDR_SIZE];
    f.seek(SeekFrom::Start(0))?;
    Read::read_exact(&mut f, &mut hdr)?;
    if &hdr[..8] != WAL_MAGIC {
        return Err(anyhow!("bad WAL magic in source {}", src_file.display()));
    }

    let mut db = Db::open(dst_root)?;
    let ps = db.pager.meta.page_size as usize;

    let mut pos = WAL_HDR_SIZE as u64;
    let file_len = f.metadata()?.len();

    let mut _frames = 0u64;
    let mut _bytes = 0u64;
    let mut max_lsn = db.pager.meta.last_lsn;

    let mut last_heads_lsn = load_last_heads_lsn(dst_root).unwrap_or(0);

    let mut rdr = WalStreamReader::new();

    while let Some((rec, next_pos)) = rdr.read_next(&mut f, pos, file_len)? {
        let frame_len = next_pos - pos;
        _bytes += frame_len;
        _frames += 1;

        if rec.lsn > max_lsn {
            max_lsn = rec.lsn;
        }

        match rec.rec_type {
            WAL_REC_PAGE_IMAGE => {
                // LSN‑гейтинг до ensure_allocated, если страница уже существует
                let mut apply = true;
                if rec.page_id < db.pager.meta.next_page_id {
                    let mut cur = vec![0u8; ps];
                    if db.pager.read_page(rec.page_id, &mut cur).is_ok() {
                        if let (Some(nl), Some(cl)) = (v3_page_lsn(&rec.payload), v3_page_lsn(&cur))
                        {
                            if cl >= nl {
                                apply = false;
                            }
                        }
                    }
                }
                if apply {
                    db.pager.ensure_allocated(rec.page_id)?;
                    db.pager.write_page_raw(rec.page_id, &rec.payload)?;
                }
            }
            WAL_REC_HEADS_UPDATE => {
                // LSN-гейтинг: применяем только если rec.lsn > last_heads_lsn
                if rec.lsn > last_heads_lsn {
                    let updates = parse_heads_updates(&rec.payload);
                    if !updates.is_empty() {
                        db.set_dir_heads_bulk(&updates)?;
                        last_heads_lsn = rec.lsn;
                        let _ = store_last_heads_lsn(dst_root, last_heads_lsn);
                    }
                }
            }
            _ => {}
        }
        pos = next_pos;
    }

    let _ = set_last_lsn(dst_root, max_lsn);

    Ok(())
}
