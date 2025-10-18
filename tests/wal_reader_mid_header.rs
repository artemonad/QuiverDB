use anyhow::Result;
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;

use QuiverDB::wal::encode::write_record;
use QuiverDB::wal::{
    write_wal_file_header, WAL_HDR_SIZE, WAL_MAGIC, WAL_REC_BEGIN, WAL_REC_TRUNCATE,
};
// NEW: stateful reader вместо deprecated read_next_record
use QuiverDB::wal::reader::WalStreamReader;

fn unique_file(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("qdb2-{}-{}-{}.wal", prefix, pid, t))
}

/// Должны пропускать mid-stream WAL header ТОЛЬКО после TRUNCATE.
#[test]
fn wal_reader_skips_midstream_header_after_truncate() -> Result<()> {
    let path = unique_file("wal-midheader-after-trunc");
    let mut f = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)?;

    // WAL file header
    write_wal_file_header(&mut f)?;

    // BEGIN #1 (lsn=1)
    write_record(&mut f, WAL_REC_BEGIN, 1, 0, &[])?;

    // TRUNCATE marker (lsn=0 обычно не важен для маркера)
    write_record(&mut f, WAL_REC_TRUNCATE, 0, 0, &[])?;

    // Вставляем mid-stream WAL header (как после ротации/транкации)
    f.write_all(WAL_MAGIC)?;
    f.write_all(&[0u8; WAL_HDR_SIZE - 8])?;

    // BEGIN #2 (lsn=2)
    write_record(&mut f, WAL_REC_BEGIN, 2, 0, &[])?;
    f.flush()?;

    // Читаем и убеждаемся, что видим: BEGIN(1), TRUNCATE, BEGIN(2)
    f.seek(SeekFrom::Start(WAL_HDR_SIZE as u64))?;
    let len = fs::metadata(&path)?.len();

    let mut pos = WAL_HDR_SIZE as u64;
    let mut seen_types: Vec<u8> = Vec::new();
    let mut seen_lsns: Vec<u64> = Vec::new();

    // NEW: stateful reader
    let mut r = WalStreamReader::new();

    while let Some((rec, next)) = r.read_next(&mut f, pos, len)? {
        seen_types.push(rec.rec_type);
        seen_lsns.push(rec.lsn);
        pos = next;
    }

    // Ожидаем три записи и корректные LSN у BEGIN кадров
    assert_eq!(
        seen_types.len(),
        3,
        "should see 3 records (BEGIN, TRUNCATE, BEGIN)"
    );
    assert_eq!(seen_types[0], WAL_REC_BEGIN);
    assert_eq!(seen_types[1], WAL_REC_TRUNCATE);
    assert_eq!(seen_types[2], WAL_REC_BEGIN);

    assert_eq!(seen_lsns[0], 1);
    // lsn у TRUNCATE не важен, не проверяем жестко
    assert_eq!(seen_lsns[2], 2);

    Ok(())
}

/// Без предшествующего TRUNCATE mid-header НЕ должен пропускаться —
/// ожидание: попытка прочитать приведет к ошибке CRC (а не к "тихому" пропуску).
#[test]
fn wal_reader_mid_header_without_truncate_errors() -> Result<()> {
    let path = unique_file("wal-midheader-no-trunc");
    let mut f = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)?;

    // WAL file header
    write_wal_file_header(&mut f)?;

    // BEGIN #1 (lsn=1)
    write_record(&mut f, WAL_REC_BEGIN, 1, 0, &[])?;

    // Вставляем mid-header без TRUNCATE перед ним
    f.write_all(WAL_MAGIC)?;
    f.write_all(&[0u8; WAL_HDR_SIZE - 8])?;

    // BEGIN #2 (lsn=2) — чтобы файл имел корректное продолжение после mid-header
    write_record(&mut f, WAL_REC_BEGIN, 2, 0, &[])?;
    f.flush()?;

    // Считываем первую запись успешно (BEGIN #1)
    f.seek(SeekFrom::Start(WAL_HDR_SIZE as u64))?;
    let len = fs::metadata(&path)?.len();

    let mut pos = WAL_HDR_SIZE as u64;
    let mut r = WalStreamReader::new();

    let (_rec1, next1) = r
        .read_next(&mut f, pos, len)?
        .expect("must read first record (BEGIN #1)");
    pos = next1;

    // Следующая позиция — как раз mid-header, но без флага TRUNCATE ранее
    // Ожидаем ошибку CRC (reader не должен "проглатывать" заголовок).
    let err = r.read_next(&mut f, pos, len).unwrap_err();
    let msg = format!("{:#}", err).to_ascii_lowercase();
    assert!(
        msg.contains("crc mismatch") || msg.contains("wal crc mismatch"),
        "expected CRC mismatch error, got: {}",
        msg
    );

    Ok(())
}
