use anyhow::Result;
use std::fs;
use std::path::PathBuf;

use QuiverDB::db::Db;

#[test]
fn doctor_runs_ok() -> Result<()> {
    let root = unique_root("doctor");
    fs::create_dir_all(&root)?;

    // init
    let page_size = 64 * 1024;
    let buckets = 32;
    Db::init(&root, page_size, buckets)?;

    // записать пару ключей
    {
        let mut db = Db::open(&root)?;
        db.put(b"a", b"1")?;
        db.put(b"b", b"2")?;
    }

    // doctor должен отработать без ошибок (stdout нас не интересует)
    {
        let db = Db::open_ro(&root)?;
        db.doctor(false)?;
        db.doctor(true)?; // JSON
    }

    Ok(())
}

fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("qdb2-{}-{}-{}", prefix, pid, t))
}
