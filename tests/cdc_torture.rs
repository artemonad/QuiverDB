// tests/cdc_torture.rs
//
// CDC torture test:
// - Собирает реальный WAL с лидера, конструирует искусственный поток:
//   [WAL header][кадры 0..i][TRUNCATE][mid-stream WAL header][кадры i..j][последний кадр ОТРЕЗАН (partial tail)]
// - Прогоняет в трёх режимах: без сжатия, gzip, zstd (сжатие всего потока).
// - Применяет поток на фолловере через wal_apply_from_stream (с соответствующим декодером).
// - Эталон: cdc record до LSN последнего полного кадра, применённый на отдельного фолловера.
// - Состояния сравниваются через реконструкцию KV из страниц (без directory), tail-wins + overflow.
//
// Покрывает: mid-stream header, TRUNCATE, partial tails, компрессия, устойчивость wal-apply.

use anyhow::{anyhow, Result};
use byteorder::{ByteOrder, LittleEndian};
use crc32fast::Hasher as Crc32;
use flate2::{write::GzEncoder, Compression as GzCompression};
use std::fs::{self, OpenOptions};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use zstd::stream::encode_all as zstd_encode_all;

use QuiverDB::{init_db, read_meta, Db, Directory};
use QuiverDB::cli::cdc::{cmd_cdc_record, cmd_cdc_replay, wal_apply_from_stream};
use QuiverDB::consts::{
    WAL_FILE, WAL_HDR_SIZE, WAL_MAGIC, WAL_REC_HDR_SIZE, WAL_REC_OFF_CRC32, WAL_REC_OFF_LEN,
    WAL_REC_OFF_LSN, WAL_REC_OFF_PAGE_ID, WAL_REC_PAGE_IMAGE, WAL_REC_TRUNCATE,
};
use QuiverDB::pager::Pager;
use QuiverDB::page_ovf::{ovf_parse_placeholder, ovf_read_chain};
use QuiverDB::page_rh::{rh_header_read, rh_kv_list, rh_page_is_kv};

// ---------------- helpers ----------------

fn nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = nanos();
    std::env::temp_dir().join(format!("qdbtest-cdc-tort-{prefix}-{pid}-{t}"))
}

// Прочитать WAL лидера и вернуть (hdr16, frames(полные записи), lsns по кадрам).
fn read_wal_frames(root: &PathBuf) -> Result<(Vec<u8>, Vec<Vec<u8>>, Vec<u64>)> {
    let wal_path = root.join(WAL_FILE);
    let mut f = OpenOptions::new().read(true).open(&wal_path)?;
    let mut hdr16 = vec![0u8; WAL_HDR_SIZE];
    f.read_exact(&mut hdr16)?;
    if &hdr16[..8] != WAL_MAGIC {
        return Err(anyhow!("bad WAL magic"));
    }
    let len = f.metadata()?.len();
    let mut pos = WAL_HDR_SIZE as u64;

    let mut frames: Vec<Vec<u8>> = Vec::new();
    let mut lsns: Vec<u64> = Vec::new();

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
            break; // частичный хвост
        }

        let mut payload = vec![0u8; payload_len];
        f.read_exact(&mut payload)?;

        // CRC check (ровно как wal-apply это ожидает)
        let mut hasher = Crc32::new();
        hasher.update(&hdr[..WAL_REC_OFF_CRC32]);
        hasher.update(&payload);
        let crc_actual = hasher.finalize();
        if crc_actual != crc_expected {
            break;
        }

        let wal_lsn = LittleEndian::read_u64(&hdr[WAL_REC_OFF_LSN..WAL_REC_OFF_LSN + 8]);
        lsns.push(wal_lsn);

        // full record bytes
        let mut full = hdr;
        full.extend_from_slice(&payload);
        frames.push(full);

        pos += rec_total;
    }

    Ok((hdr16, frames, lsns))
}

// Сконструировать TRUNCATE запись: [type=2][flags=0][lsn=0][page_id=0][len=0][crc]
fn make_truncate_record() -> Vec<u8> {
    let mut hdr = vec![0u8; WAL_REC_HDR_SIZE];
    hdr[0] = WAL_REC_TRUNCATE;
    hdr[1] = 0;
    LittleEndian::write_u16(&mut hdr[2..4], 0);
    LittleEndian::write_u64(&mut hdr[WAL_REC_OFF_LSN..WAL_REC_OFF_LSN + 8], 0);
    LittleEndian::write_u64(&mut hdr[WAL_REC_OFF_PAGE_ID..WAL_REC_OFF_PAGE_ID + 8], 0);
    LittleEndian::write_u32(&mut hdr[WAL_REC_OFF_LEN..WAL_REC_OFF_LEN + 4], 0);
    let mut hasher = Crc32::new();
    hasher.update(&hdr[..WAL_REC_OFF_CRC32]);
    let crc = hasher.finalize();
    LittleEndian::write_u32(&mut hdr[WAL_REC_OFF_CRC32..WAL_REC_OFF_CRC32 + 4], crc);
    hdr
}

// Реконструкция tail-wins KV из страниц фолловера (без directory).
fn reconstruct_state_without_directory(root: &PathBuf) -> Result<std::collections::HashMap<Vec<u8>, Vec<u8>>> {
    let pager = Pager::open(root)?;
    let ps = pager.meta.page_size as usize;

    let mut out = std::collections::HashMap::new();
    let mut page = vec![0u8; ps];

    for pid in 0..pager.meta.next_page_id {
        if pager.read_page(pid, &mut page).is_ok() && rh_page_is_kv(&page) {
            if let Ok(items) = rh_kv_list(&page) {
                for (k, v) in items {
                    let val = if let Some((total_len, head_pid)) = ovf_parse_placeholder(&v) {
                        ovf_read_chain(&pager, head_pid, Some(total_len as usize))?
                    } else {
                        v.to_vec()
                    };
                    out.insert(k.to_vec(), val);
                }
            }
        }
    }
    Ok(out)
}

// ---------------- test ----------------

#[test]
fn cdc_torture_stream_mid_header_truncate_partial_with_compression() -> Result<()> {
    std::env::set_var("P1_WAL_COALESCE_MS", "0");

    // 1) Лидер: генерируем записи
    let leader = unique_root("leader");
    let follow_ref_root = unique_root("follow-ref");
    let follow_tort_root = unique_root("follow-tort");
    fs::create_dir_all(&leader)?;
    fs::create_dir_all(&follow_ref_root)?;
    fs::create_dir_all(&follow_tort_root)?;
    init_db(&leader, 4096)?;
    Directory::create(&leader, 64)?;

    {
        let mut db = Db::open(&leader)?;
        for i in 0..40 {
            let k = format!("t{:03}", i);
            let v = format!("val{:04}", i * 11);
            db.put(k.as_bytes(), v.as_bytes())?;
        }
    }

    // 2) Считываем реальный WAL и собираем поток с инъекциями
    let (hdr16, frames, _lsns) = read_wal_frames(&leader)?;
    assert!(frames.len() >= 6, "need at least 6 frames in WAL");
    // Разрезы
    let i = frames.len() / 3;
    let j = (frames.len() * 2) / 3;

    // Последний полный кадр, который должен примениться на фолловере (после вставок/обрезаний)
    let last_full_lsn = LittleEndian::read_u64(&frames[j - 1][WAL_REC_OFF_LSN..WAL_REC_OFF_LSN + 8]);

    // Собираем искусственный поток
    let mut stream: Vec<u8> = Vec::new();
    // header
    stream.extend_from_slice(&hdr16);
    // кадры 0..i
    for rec in &frames[..i] {
        stream.extend_from_slice(rec);
    }
    // TRUNCATE
    stream.extend_from_slice(&make_truncate_record());
    // mid-stream WAL header (16 байт)
    stream.extend_from_slice(&hdr16);
    // кадры i..j
    for rec in &frames[i..j] {
        stream.extend_from_slice(rec);
    }
    // последний кадр — частичный хвост (берём j-й кадр и обрезаем payload)
    let last = &frames[j];
    let cut_len = (WAL_REC_HDR_SIZE + 8).min(last.len()); // частичный payload 8 байт
    stream.extend_from_slice(&last[..cut_len]);

    // 3 режима: none, gzip, zstd
    for mode in ["none", "gzip", "zstd"] {
        // Тестируемый фолловер (torture) — отдельная директория на режим
        let follow_tort_mode = follow_tort_root.join(mode);
        fs::create_dir_all(&follow_tort_mode)?;
        init_db(&follow_tort_mode, 4096)?;

        match mode {
            "none" => {
                let cursor = Cursor::new(stream.clone());
                wal_apply_from_stream(&follow_tort_mode, cursor)?;
            }
            "gzip" => {
                // Сжимаем весь поток в gzip
                let mut gz = GzEncoder::new(Vec::new(), GzCompression::fast());
                gz.write_all(&stream)?;
                let compressed = gz.finish()?;
                let dec = flate2::read::GzDecoder::new(Cursor::new(compressed));
                wal_apply_from_stream(&follow_tort_mode, dec)?;
            }
            "zstd" => {
                let compressed = zstd_encode_all(Cursor::new(&stream), 0)?;
                let dec = zstd::stream::read::Decoder::new(Cursor::new(compressed))?;
                wal_apply_from_stream(&follow_tort_mode, dec)?;
            }
            _ => unreachable!(),
        }

        // Эталон: до last_full_lsn — отдельная директория на режим
        let follow_ref_mode = follow_ref_root.join(mode);
        init_db(&follow_ref_mode, 4096)?;
        let slice = leader.join(format!("ref_slice_{}.bin", mode));
        cmd_cdc_record(leader.clone(), slice.clone(), Some(0), Some(last_full_lsn))?;
        cmd_cdc_replay(follow_ref_mode.clone(), Some(slice), None, None)?;

        // Сравнение состояний: follower_ref vs follower_tort_mode
        let map_ref = reconstruct_state_without_directory(&follow_ref_mode)?;
        let map_tort = reconstruct_state_without_directory(&follow_tort_mode)?;

        assert_eq!(
            map_ref.len(),
            map_tort.len(),
            "state size mismatch for mode={}",
            mode
        );
        for (k, v) in &map_ref {
            let got = map_tort.get(k)
                .unwrap_or_else(|| panic!("missing key on tort follower for mode={}: {}", mode, String::from_utf8_lossy(k)));
            assert_eq!(got, v, "value mismatch for key {:?} mode={}", String::from_utf8_lossy(k), mode);
        }
    }

    Ok(())
}