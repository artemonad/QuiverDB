use anyhow::Result;
use oorandom::Rand64;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use QuiverDB::db::Db;

fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("qdb2-{}-{}-{}", prefix, pid, t))
}

#[test]
fn stress_churn_put_del_compact() -> Result<()> {
    // Быстрое окружение (без fsync/checksum для скорости)
    std::env::set_var("P1_WAL_DISABLE_FSYNC", "1");
    std::env::set_var("P1_DATA_FSYNC", "0");
    std::env::set_var("P1_PAGE_CHECKSUM", "0");

    let root = unique_root("stress-churn");
    fs::create_dir_all(&root)?;
    let page_size = 64 * 1024;
    let buckets = 128;
    QuiverDB::Db::init(&root, page_size, buckets)?;

    // Модель «истинного» состояния
    let mut model: HashMap<Vec<u8>, Option<Vec<u8>>> = HashMap::new();

    // Детерминированный генератор
    let mut rng = Rand64::new(0xA1B2_C3D4_E5F6_7788);

    // Параметры нагрузки
    let total_keys = 10_000usize;
    let value_len_small = 32usize;
    let value_len_big = 256usize;
    let batch_size = 500usize;

    // Сгенерим ключи
    let mut keys: Vec<Vec<u8>> = Vec::with_capacity(total_keys);
    for i in 0..total_keys {
        let rnd = rng.rand_u64();
        keys.push(format!("k-{:016x}-{:06}", rnd, i).into_bytes());
    }

    // 1) Массовая запись (batched)
    {
        let mut db = Db::open(&root)?;
        for chunk in keys.chunks(batch_size) {
            db.batch(|b| {
                for k in chunk {
                    // Перемешаем размеры
                    let vlen = if (rng.rand_u64() & 1) == 0 { value_len_small } else { value_len_big };
                    let val = vec![0xAB; vlen];
                    b.put(k, &val)?;
                    model.insert(k.clone(), Some(val));
                }
                Ok(())
            })?;
        }
    }

    // 2) Волна удалений/перезаписей
    {
        let mut db = Db::open(&root)?;
        // Удалим ~1/3, перезапишем ~1/3, оставим ~1/3
        for chunk in keys.chunks(batch_size) {
            db.batch(|b| {
                for k in chunk {
                    let r = (rng.rand_u64() % 3) as u8;
                    match r {
                        0 => {
                            // delete
                            let _ = b.del(k)?;
                            model.insert(k.clone(), None);
                        }
                        1 => {
                            // overwrite
                            let vlen = if (rng.rand_u64() & 1) == 0 { value_len_small } else { value_len_big };
                            let val = vec![0xCD; vlen];
                            b.put(k, &val)?;
                            model.insert(k.clone(), Some(val));
                        }
                        _ => {
                            // keep as is
                        }
                    }
                }
                Ok(())
            })?;
        }
    }

    // 3) Проверка соответствия get() с моделью
    {
        let db = Db::open_ro(&root)?;
        for k in &keys {
            let got = db.get(k)?;
            match model.get(k).and_then(|x| x.clone()) {
                Some(expected) => {
                    let g = got.expect("must exist by model");
                    assert_eq!(g, expected, "value mismatch for key {}", String::from_utf8_lossy(k));
                }
                None => {
                    assert!(got.is_none(), "must be None by model");
                }
            }
        }
    }

    // 4) Компактация всей базы + сверка снова
    {
        let mut db = Db::open(&root)?;
        let sum = db.compact_all()?;
        assert!(sum.pages_written_sum > 0, "compaction should write some pages");
    }
    {
        let db = Db::open_ro(&root)?;
        for k in &keys {
            let got = db.get(k)?;
            match model.get(k).and_then(|x| x.clone()) {
                Some(expected) => {
                    let g = got.expect("must exist after compaction");
                    assert_eq!(g, expected, "value mismatch after compaction for key {}", String::from_utf8_lossy(k));
                }
                None => {
                    assert!(got.is_none(), "must be None after compaction");
                }
            }
        }
    }

    Ok(())
}