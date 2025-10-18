use anyhow::Result;
use std::fs;
use std::path::PathBuf;

use QuiverDB::db::Db;
use QuiverDB::meta::{init_meta_v4, CKSUM_CRC32C, CODEC_NONE, CODEC_ZSTD, HASH_KIND_XX64_SEED0};

fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("qdb2-{}-{}-{}", prefix, pid, t))
}

#[test]
fn stress_overflow_many_values() -> Result<()> {
    // Быстрое окружение
    std::env::set_var("P1_WAL_DISABLE_FSYNC", "1");
    std::env::set_var("P1_DATA_FSYNC", "0");

    let root = unique_root("ovf-many");
    fs::create_dir_all(&root)?;
    let page_size = 64 * 1024;
    let buckets = 64;

    // init meta (без zstd)
    init_meta_v4(
        &root,
        page_size,
        HASH_KIND_XX64_SEED0,
        CODEC_NONE,
        CKSUM_CRC32C,
    )?;
    QuiverDB::Directory::create(&root, buckets)?;

    let n_items = 512usize;
    let big_len = page_size as usize * 2 + 137;

    // 1) Пишем много больших значений (без zstd)
    {
        let mut db = Db::open(&root)?;
        db.batch(|b| {
            for i in 0..n_items {
                let k = format!("big-{:05}", i).into_bytes();
                let mut val = vec![0xEE; big_len];
                let mid = val.len() / 2;
                let last = val.len() - 1;
                val[0] = 0xEE;
                val[mid] = 0xAA;
                val[last] = 0xEE;
                b.put(&k, &val)?;
            }
            Ok(())
        })?;
    }

    // 2) Проверка чтения (без zstd)
    {
        let db = Db::open_ro(&root)?;
        for i in 0..n_items {
            let k = format!("big-{:05}", i).into_bytes();
            let got = db.get(&k)?.expect("must exist");
            assert_eq!(got.len(), big_len);
            assert_eq!(got[0], 0xEE);
            assert_eq!(got[got.len() / 2], 0xAA);
            assert_eq!(got[got.len() - 1], 0xEE);
        }
    }

    // 3) Повтор для zstd (кодек по умолчанию = zstd)
    let root2 = unique_root("ovf-many-zstd");
    fs::create_dir_all(&root2)?;
    init_meta_v4(
        &root2,
        page_size,
        HASH_KIND_XX64_SEED0,
        CODEC_ZSTD,
        CKSUM_CRC32C,
    )?;
    QuiverDB::Directory::create(&root2, buckets)?;

    {
        let mut db = Db::open(&root2)?;
        db.batch(|b| {
            for i in 0..n_items {
                let k = format!("big-{:05}", i).into_bytes();
                let mut val = vec![0xBB; big_len + 777];
                let mid = val.len() / 2;
                let last = val.len() - 1;
                val[0] = 0xBB;
                val[mid] = 0x99;
                val[last] = 0xBB;
                b.put(&k, &val)?;
            }
            Ok(())
        })?;
    }
    {
        let db = Db::open_ro(&root2)?;
        for i in 0..n_items {
            let k = format!("big-{:05}", i).into_bytes();
            let got = db.get(&k)?.expect("must exist");
            assert_eq!(got[0], 0xBB);
            assert_eq!(got[got.len() / 2], 0x99);
            assert_eq!(got[got.len() - 1], 0xBB);
        }
    }

    Ok(())
}
