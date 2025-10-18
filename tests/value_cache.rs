use anyhow::Result;
use QuiverDB::pager::value_cache::{
    value_cache_clear, value_cache_configure, value_cache_counters, value_cache_get,
    value_cache_put, value_cache_stats,
};

#[test]
fn value_cache_basic_hits_misses_and_eviction() -> Result<()> {
    // Конфигурируем кэш на 4 KiB, min_size=32, и сбрасываем состояние.
    value_cache_configure(4096, 32);
    value_cache_clear();

    // Исходные stats/counters
    {
        let (cap, used, entries) = value_cache_stats();
        assert_eq!(cap, 4096, "cap_bytes must be 4096");
        assert_eq!(used, 0, "used_bytes must be 0 on clear");
        assert_eq!(entries, 0, "entries must be 0 on clear");

        let (hits, misses) = value_cache_counters();
        assert_eq!(hits, 0, "hits must start at 0");
        assert_eq!(misses, 0, "misses must start at 0");
    }

    // 1) Значение меньше min_size — не кэшируется
    let small = vec![0xAA; 16]; // < min_size(32)
    value_cache_put(1, 11, small.len(), &small);
    {
        let (cap, used, entries) = value_cache_stats();
        assert_eq!(cap, 4096);
        assert_eq!(used, 0, "small (< min_size) must not increase used_bytes");
        assert_eq!(entries, 0, "small (< min_size) must not create entry");
    }
    assert!(
        value_cache_get(1, 11, small.len()).is_none(),
        "small value must not be cached"
    );
    {
        let (hits, misses) = value_cache_counters();
        assert_eq!(hits, 0, "no hits expected so far");
        assert!(misses >= 1, "expected at least one miss on small get");
    }

    // 2) Кладём значение A (1000 B) — попадает в кэш, get → hit
    let a = vec![0xAB; 1000];
    value_cache_put(1, 101, a.len(), &a);
    {
        let (cap, used, entries) = value_cache_stats();
        assert_eq!(cap, 4096);
        assert!(
            used >= 1000 && used <= 4096,
            "used {} must reflect inserted A",
            used
        );
        assert_eq!(entries, 1, "one entry (A) expected");
    }
    let got_a = value_cache_get(1, 101, a.len()).expect("A must be cached");
    assert_eq!(got_a, a, "cached A must be equal");

    // Запомним текущие счётчики для последующих проверок
    let (hits_base, misses_base) = value_cache_counters();

    // 3) Добавляем B (2000 B) и C (2000 B), кап = 4096
    // После вставки C должен вытесниться LRU (A).
    let b = vec![0xBB; 2000];
    let c = vec![0xCC; 2000];
    value_cache_put(1, 102, b.len(), &b);
    {
        let (cap, used, entries) = value_cache_stats();
        assert_eq!(cap, 4096);
        assert!(
            used >= 3000 && used <= 4096,
            "used {} must include A+B before C",
            used
        );
        assert_eq!(entries, 2, "two entries expected after A+B");
    }
    value_cache_put(1, 103, c.len(), &c);
    {
        let (cap, used, entries) = value_cache_stats();
        assert_eq!(cap, 4096);
        // После вставки C (2000 B) суммарно 3000 + 2000 = 5000 > 4096
        // Должна произойти эвикция ~1000 (A) → остаётся ~2000+2000=~4000, 2 entries (B,C)
        assert!(used <= 4096, "used {} must not exceed cap", used);
        assert_eq!(entries, 2, "LRU eviction must keep two entries (B,C)");
    }

    // A больше не должен быть в кэше
    assert!(
        value_cache_get(1, 101, a.len()).is_none(),
        "A must be evicted (LRU)"
    );

    // B и C должны быть в кэше
    let got_b = value_cache_get(1, 102, b.len()).expect("B must be cached");
    assert_eq!(got_b, b);
    let got_c = value_cache_get(1, 103, c.len()).expect("C must be cached");
    assert_eq!(got_c, c);

    // Счётчики выросли: было уже один hit (A), потом два hit (B,C), и не нулевой miss
    let (hits_now, misses_now) = value_cache_counters();
    assert!(
        hits_now >= hits_base + 2,
        "hits must grow after B,C gets ({} -> {})",
        hits_base,
        hits_now
    );
    assert!(
        misses_now >= misses_base + 1,
        "misses must grow after evicted A get ({} -> {})",
        misses_base,
        misses_now
    );

    Ok(())
}
