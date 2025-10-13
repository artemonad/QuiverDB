use anyhow::Result;
use std::path::PathBuf;

use QuiverDB::bloom::{bloom_cache_stats, bloom_cache_counters};
use QuiverDB::bloom::cache::{bloom_cache_get, bloom_cache_put};

fn unique_path(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    // Путь к "несуществующему" bloom.bin — нас интересует только хеш пути
    std::env::temp_dir().join(format!("qdb2-{}-{}-{}.bloom", prefix, pid, t))
}

#[test]
fn bloom_cache_o1_lru_basic() -> Result<()> {
    // Инициализируем кэш на небольшую ёмкость (если уже инициализирован — это значение будет проигнорировано)
    std::env::set_var("P1_BLOOM_CACHE_BUCKETS", "4");

    let path = unique_path("bloom-cache");
    let last_lsn = 42u64;

    // Заполним 4 разных бакета — entries <= cap
    for b in 0..4u32 {
        bloom_cache_put(&path, b, last_lsn, vec![b as u8; 8]);
    }
    let (cap, len) = bloom_cache_stats();
    assert!(cap >= 4, "capacity must be >= 4");
    assert_eq!(len, 4, "entries must be 4");

    // Хиты/промахи: сначала промахи (пустые last_lsn + 1)
    let (_, _) = bloom_cache_counters(); // снимем базу
    for b in 0..4u32 {
        // другой LSN → промах
        assert!(bloom_cache_get(&path, b, last_lsn + 1).is_none());
    }
    let (hits0, miss0) = bloom_cache_counters();
    assert!(miss0 >= 4 && hits0 == 0, "expect >=4 misses");

    // Теперь — хиты по актуальному LSN
    for b in 0..4u32 {
        let got = bloom_cache_get(&path, b, last_lsn);
        assert!(got.is_some(), "expect hit for bucket {}", b);
    }
    let (hits1, miss1) = bloom_cache_counters();
    assert!(hits1 >= 4 && miss1 >= miss0, "hits must grow");

    // Эвикт: вставим ещё 4 бакета → самые старые должны вытесниться
    for b in 4..8u32 {
        bloom_cache_put(&path, b, last_lsn, vec![b as u8; 8]);
    }
    let (_cap2, len2) = bloom_cache_stats();
    assert_eq!(len2, cap.min(8), "entries must match cap");

    // Старые (0..3) вероятно вытеснились; хотя точный порядок зависим от get() между put().
    // Проверим, что новые (4..7) присутствуют:
    for b in 4..8u32 {
        assert!(bloom_cache_get(&path, b, last_lsn).is_some(), "new bucket must be cached");
    }

    Ok(())
}