use anyhow::Result;
use std::fs;
use std::path::PathBuf;

use QuiverDB::db::Db;
use QuiverDB::dir::NO_PAGE;

#[test]
fn sweep_orphan_overflow_frees_pages() -> Result<()> {
    let root = unique_root("sweep");
    fs::create_dir_all(&root)?;

    // init
    let page_size = 64 * 1024;
    let buckets = 64;
    Db::init(&root, page_size, buckets)?;

    // put большой value -> создаются OVERFLOW3 страницы
    let big_len = page_size as usize * 2;
    let big = vec![0xCD; big_len];
    let key = b"k-big";

    {
        let mut db = Db::open(&root)?;
        db.put(key, &big)?;
    }

    // "Оборвём" head у бакета, чтобы overflow перестали быть достижимыми из каталога (writer-only API)
    {
        let mut db = Db::open(&root)?;
        let bucket = db.dir.bucket_of_key(key, db.pager.meta.hash_kind);
        db.set_dir_head(bucket, NO_PAGE)?;
    }

    // sweep
    {
        let mut db = Db::open(&root)?;
        let freed = db.sweep_orphan_overflow()?;
        assert!(freed > 0, "expected to free some orphan overflow pages");
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