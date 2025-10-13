use anyhow::Result;
use std::fs;
use std::path::PathBuf;

use QuiverDB::db::Db;
use QuiverDB::dir::{Directory, NO_PAGE};
use QuiverDB::pager::Pager;
use QuiverDB::page::{kv_header_read_v3, PAGE_MAGIC};

#[test]
fn kv_packing_batch_packs_multiple_records_per_page() -> Result<()> {
    // Подготовка корня
    let root = unique_root("kv-pack");
    fs::create_dir_all(&root)?;

    // Небольшая страница и один бакет — чтобы все записи оказались в одной цепочке
    let page_size = 4096;
    let buckets = 1;
    Db::init(&root, page_size, buckets)?;

    // Готовим 50 маленьких KV
    let n = 50usize;
    let mut kvs: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(n);
    for i in 0..n {
        let k = format!("k-{:04}", i).into_bytes();
        let v = vec![b'v'; 3]; // мелкие значения
        kvs.push((k, v));
    }

    // Batch write (должно упаковать многие записи на одну страницу)
    {
        let mut db = Db::open(&root)?;
        db.batch(|b| {
            for (k, v) in &kvs {
                b.put(k, v)?;
            }
            Ok(())
        })?;
    }

    // Проверим, что head-страница действительно упакована (table_slots > 1)
    let dir = Directory::open(&root)?;
    let bucket = 0u32;
    let head_pid = dir.head(bucket)?;
    assert_ne!(head_pid, NO_PAGE, "head must be present after batch");

    let pager = Pager::open(&root)?;
    let ps = pager.meta.page_size as usize;

    let mut head_page = vec![0u8; ps];
    pager.read_page(head_pid, &mut head_page)?;
    assert_eq!(&head_page[0..4], PAGE_MAGIC, "KV page magic expected at head");

    let hdr = kv_header_read_v3(&head_page)?;
    assert!(
        hdr.table_slots > 1 && hdr.used_slots >= 1,
        "expected packed head page with >1 slots, got table_slots={}, used_slots={}",
        hdr.table_slots,
        hdr.used_slots
    );

    // Санити: несколько случайных ключей читаются корректно
    let db_ro = Db::open_ro(&root)?;
    for idx in [0usize, n / 2, n - 1] {
        let (k, v) = &kvs[idx];
        let got = db_ro.get(k.as_slice())?.expect("value must exist");
        assert_eq!(got.as_slice(), v.as_slice(), "value mismatch for key {:?}", String::from_utf8_lossy(k));
    }

    Ok(())
}

// ---------- helpers ----------

fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("qdb2-{}-{}-{}", prefix, pid, t))
}