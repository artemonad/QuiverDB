use anyhow::Result;
use std::fs;
use std::path::PathBuf;

use QuiverDB::db::Db;
use QuiverDB::pager::Pager;
// diagnostics
use QuiverDB::pager::cache::{
    page_cache_clear, page_cache_configure, page_cache_evictions_total, page_cache_len,
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
fn stress_page_cache_gets_and_evictions() -> Result<()> {
    // Быстрое окружение (минимум I/O)
    std::env::set_var("P1_WAL_DISABLE_FSYNC", "1");
    std::env::set_var("P1_DATA_FSYNC", "0");
    std::env::set_var("P1_PAGE_CHECKSUM", "0");

    let root = unique_root("page-cache");
    fs::create_dir_all(&root)?;
    let page_size = 64 * 1024;
    let buckets = 64;
    Db::init(&root, page_size, buckets)?;

    // Изначально выставим маленькую ёмкость, но writer позже её перезапишет дефолтом (4096).
    // Это ок — после записей конфигурируем кэш ещё раз.
    page_cache_clear();
    page_cache_configure(page_size as usize, 8);

    // Запишем данные одиночными put: по спецификации это одна KV‑страница на запись.
    // Делаем достаточно записей, чтобы число уникальных страниц >> cap.
    let n_keys = 512usize;
    {
        let mut db = Db::open(&root)?;
        for i in 0..n_keys {
            let k = format!("k-{:06}", i).into_bytes();
            let v = vec![0xAA; 64];
            db.put(&k, &v)?;
        }
        // writer закрывается здесь → возможно перезапишет конфиг кэша при другом Db::open позже
    }

    // ВАЖНО: переинициализируем кэш после writer’а,
    // чтобы гарантировать cap=8 (writer мог выставить дефолт 4096).
    page_cache_clear();
    page_cache_configure(page_size as usize, 8);

    // Откроем Pager и последовательно прочитаем все выделенные страницы.
    // Это гарантированно положит в cache множество уникальных страниц.
    let pager = Pager::open(&root)?;
    let ps = pager.meta.page_size as usize;
    let total_pages = pager.meta.next_page_id;
    assert!(total_pages > 0, "must have allocated pages after puts");

    let mut buf = vec![0u8; ps];
    for pid in 0..total_pages {
        // read_page выполнит verify и положит страницу в page cache (KV страницы кэшируем).
        // Ошибки игнорируем (здесь их быть не должно).
        let _ = pager.read_page(pid, &mut buf);
    }

    let len = page_cache_len();
    let ev = page_cache_evictions_total();

    assert!(len > 0, "cache len must be > 0 (got {})", len);
    assert!(
        ev > 0,
        "evictions_total must grow (got {}). Total pages read: {}, cache cap: 8",
        ev,
        total_pages
    );

    Ok(())
}
