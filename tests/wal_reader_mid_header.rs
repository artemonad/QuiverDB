use anyhow::Result;
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;

use QuiverDB::wal::{
    write_wal_file_header, WAL_HDR_SIZE, WAL_MAGIC,
    WAL_REC_BEGIN};
use QuiverDB::wal::encode::{write_record};
use QuiverDB::wal::reader::read_next_record;

fn unique_file(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("qdb2-{}-{}-{}.wal", prefix, pid, t))
}

#[test]
fn wal_reader_skips_midstream_header() -> Result<()> {
    let path = unique_file("wal-midheader");
    let mut f = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)?;

    // header
    write_wal_file_header(&mut f)?;
    // BEGIN #1 (lsn=1)
    write_record(&mut f, WAL_REC_BEGIN, 1, 0, &[])?;
    // Вставим mid-stream WAL header (как после TRUNCATE)
    f.write_all(WAL_MAGIC)?;
    f.write_all(&[0u8; WAL_HDR_SIZE - 8])?;
    // BEGIN #2 (lsn=2)
    write_record(&mut f, WAL_REC_BEGIN, 2, 0, &[])?;
    f.flush()?;

    // Читаем, ожидая 2 записи
    f.seek(SeekFrom::Start(WAL_HDR_SIZE as u64))?;
    let len = fs::metadata(&path)?.len();

    let mut pos = WAL_HDR_SIZE as u64;
    let mut seen = Vec::new();
    while let Some((rec, next)) = read_next_record(&mut f, pos, len)? {
        seen.push(rec.lsn);
        pos = next;
    }

    assert_eq!(seen, vec![1, 2], "should see records around mid-header");
    Ok(())
}