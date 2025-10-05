// tests/snapshot_hot_churn.rs
//
// Запуск только этого файла:
//   cargo test --test snapshot_hot_churn -- --ignored --nocapture
//
// Сценарий (нагруженный, #[ignore]):
// - Пишем набор ключей (часть overflow).
// - Начинаем snapshot (фиксируем lsn).
// - Выполняем интенсивные put/del/обновления (churn) тех же ключей и новых.
// - Проверяем, что snapshot видит старые значения (как на момент snapshot_begin).
// - Проверяем, что ключи, созданные после снапшота, не видны под снапшотом.
// - Метрики snapshot_freeze_frames/snapshot_freeze_bytes > 0 (freeze действительно выполнялся).

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use oorandom::Rand64;

use QuiverDB::{init_db, read_meta, Db, Directory};
use QuiverDB::metrics;

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let base = std::env::temp_dir();
    base.join(format!("qdbtest-snap-churn-{prefix}-{pid}-{t}-{id}"))
}

#[test]
fn snapshot_hot_churn_consistency() -> Result<()> {
    // Делаем LSN/тайминги детерминированнее
    std::env::set_var("P1_WAL_COALESCE_MS", "0");

    let root = unique_root("churn");
    fs::create_dir_all(&root)?;
    init_db(&root, 4096)?;
    Directory::create(&root, 128)?;

    // Настроим начальный набор ключей (часть будет overflow)
    let ps = read_meta(&root)?.page_size as usize;
    let threshold = ps / 4;
    let key_count = 64usize;

    let mut rng = Rand64::new(0xDEADC0FFEE);

    // Генерация детерминированных начальных значений
    let mut initial: HashMap<String, Vec<u8>> = HashMap::new();
    for i in 0..key_count {
        let k = format!("k{:03}", i);
        let size_choice = i % 7;
        let len = match size_choice {
            0 => threshold.saturating_sub(1),
            1 => threshold,
            2 => threshold + 1,
            3 => (threshold + 100).min(ps - 64),
            4 => (threshold + 300).min(ps - 64),
            5 => (threshold + 700).min(ps - 64),
            _ => {
                let m = (rng.rand_u64() % (4 * threshold as u64 + 2048)) as usize;
                m.min(ps - 64)
            }
        };
        let mut v = vec![0u8; len.max(1)];
        for j in 0..v.len() {
            v[j] = ((i as u8).wrapping_mul(23)).wrapping_add((j as u8).wrapping_mul(31));
        }
        initial.insert(k, v);
    }

    // Запишем initial в БД
    {
        let mut db = Db::open(&root)?;
        for (k, v) in &initial {
            db.put(k.as_bytes(), v)?;
        }
    }

    // Начинаем снапшот (фиксируем lsn)
    let mut db = Db::open(&root)?;
    let mut snap = db.snapshot_begin()?;
    let _snap_lsn = snap.lsn();

    // Нагрузка (churn) — оперируем тем же writer'ом (в том же процессе), чтобы freeze_if_needed видел активный snapshot
    // Пусть будет ~1500 операций (значимо нагружает, но ещё быстро)
    let ops = 1500usize;
    let mut created_after_snapshot: Vec<String> = Vec::new();

    for i in 0..ops {
        // 70% put/update, 20% delete, 10% put новых ключей после снапшота
        let dice = rng.rand_u64() % 10;
        if dice < 7 {
            // update существующих
            let idx = (rng.rand_u64() as usize) % key_count;
            let key = format!("k{:03}", idx);
            // Размер вокруг порога overflow
            let choice = rng.rand_u64() % 7;
            let len = match choice {
                0 => threshold.saturating_sub(1),
                1 => threshold,
                2 => threshold + 1 + ((rng.rand_u64() % 256) as usize),
                3 => (ps / 2).min(threshold + 300 + ((rng.rand_u64() % 512) as usize)),
                4 => (threshold + 700).min(ps - 64),
                5 => (rng.rand_u64() as usize % (3 * threshold + 1024)).min(ps - 64),
                _ => (rng.rand_u64() as usize % (4 * threshold + 2048)).min(ps - 64),
            };
            let mut val = vec![0u8; len.max(1)];
            for j in 0..val.len() {
                val[j] = ((i as u8).wrapping_mul(37)).wrapping_add((j as u8).wrapping_mul(13));
            }
            db.put(key.as_bytes(), &val)?;
        } else if dice < 9 {
            // delete
            let idx = (rng.rand_u64() as usize) % key_count;
            let key = format!("k{:03}", idx);
            let _ = db.del(key.as_bytes())?;
        } else {
            // новые ключи после снапшота (с префиксом "z") — под снапшотом не должны быть видны
            let key = format!("z{:06}", i);
            let len = (rng.rand_u64() as usize % (threshold + 1024)).min(ps - 64);
            let mut val = vec![0u8; len.max(1)];
            for j in 0..val.len() {
                val[j] = ((i as u8).wrapping_mul(19)).wrapping_add((j as u8).wrapping_mul(7));
            }
            db.put(key.as_bytes(), &val)?;
            if created_after_snapshot.len() < 32 {
                created_after_snapshot.push(key);
            }
        }

        // Небольшая пауза, чтобы дать шанс группе fsync и freeze накопиться (и быть ближе к реальности)
        if i % 200 == 0 {
            thread::sleep(Duration::from_millis(1));
        }
    }

    // Проверки snapshot-view: все "kNNN" должны быть видны в старом состоянии, каким оно было до снапшота.
    for (k, v_old) in &initial {
        let got = snap.get(k.as_bytes())?;
        // Мы ожидаем наличие, потому что все эти ключи были записаны ДО snapshot_begin()
        let got = got.expect("initial key must exist under snapshot");
        assert_eq!(
            got.as_slice(),
            v_old.as_slice(),
            "snapshot view must return OLD value for key {}",
            k
        );
    }

    // Новые ключи (z...) после снапшота не должны быть видны
    for k in created_after_snapshot.iter().take(16) {
        let got = snap.get(k.as_bytes())?;
        assert!(
            got.is_none(),
            "key '{}' created after snapshot must not be visible under snapshot",
            k
        );
    }

    // Метрики: фриз точно происходил
    let m = metrics::snapshot();
    assert!(
        m.snapshot_freeze_frames > 0,
        "expected snapshot_freeze_frames > 0 after churn; got {}",
        m.snapshot_freeze_frames
    );
    assert!(
        m.snapshot_freeze_bytes > 0,
        "expected snapshot_freeze_bytes > 0 after churn; got {}",
        m.snapshot_freeze_bytes
    );

    // Завершаем снапшот
    db.snapshot_end(&mut snap)?;

    Ok(())
}