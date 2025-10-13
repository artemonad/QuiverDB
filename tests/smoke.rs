use anyhow::Result;
use std::fs;
use std::path::PathBuf;

use QuiverDB::db::Db;
use QuiverDB::dir::Directory;
use QuiverDB::meta::read_meta;

#[test]
fn smoke_init_put_get_del_scan() -> Result<()> {
    let root = unique_root("smoke");
    fs::create_dir_all(&root)?;

    // 1) init
    let page_size = 64 * 1024;
    let buckets = 128;
    Db::init(&root, page_size, buckets)?;

    // 2) writer: put alpha + big
    {
        let mut db = Db::open(&root)?;
        db.put(b"alpha", b"1")?;

        // Большое значение гарантированно больше inline-capacity => OVERFLOW3
        let ps = db.pager.meta.page_size as usize;
        let big_len = ps; // > (ps - (header+trailer+record overhead))
        let big = build_pattern(big_len, 0xAB);
        db.put(b"big", &big)?;
    }

    // 3) reader: get, проверить значения
    {
        let db = Db::open_ro(&root)?;
        let got = db.get(b"alpha")?.expect("alpha must exist");
        assert_eq!(got.as_slice(), b"1");

        let got_big = db.get(b"big")?.expect("big must exist");
        let ps = db.pager.meta.page_size as usize;
        assert_eq!(got_big.len(), ps, "big value length must match");
        assert_eq!(got_big[0], 0xAB);
        assert_eq!(got_big[got_big.len() - 1], 0xAB);
    }

    // 4) delete alpha и проверка отсутствия
    {
        let mut db = Db::open(&root)?;
        let existed = db.del(b"alpha")?;
        assert!(existed, "alpha should likely exist before delete");
    }
    {
        let db = Db::open_ro(&root)?;
        let miss = db.get(b"alpha")?;
        assert!(miss.is_none(), "alpha must be absent after delete");
    }

    // 5) status sanity
    {
        let m = read_meta(&root)?;
        assert_eq!(m.version, 4, "meta version must be 4");
        assert_eq!(m.page_size, page_size);
        assert!(m.last_lsn > 0, "last_lsn must be > 0 after writes");

        let d = Directory::open(&root)?;
        assert_eq!(d.bucket_count, buckets);
        let used = d.count_used_buckets()?;
        assert!(used > 0, "used_buckets should be > 0 after puts");
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

fn build_pattern(len: usize, byte: u8) -> Vec<u8> {
    let mut v = vec![byte; len];
    // Немного разнообразия, чтобы не все байты были одинаковыми
    if len >= 8 {
        v[0] = byte;
        v[len / 2] = byte ^ 0x11;
        v[len - 1] = byte;
    }
    v
}