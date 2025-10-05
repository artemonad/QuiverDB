// tests/backup_hot_leader.rs
//
// Запуск только этого файла:
//   cargo test --test backup_hot_leader -- --ignored --nocapture
//
// Сценарий:
// - Инициализируем набор ключей (часть overflow).
// - Начинаем snapshot (фиксируем срез).
// - Пока snapshot держится, выполняем churn (put/del/updates) тех же ключей + новых (с префиксом "z").
// - Делаем backup_to_dir на основе snapshot.
// - Закрываем snapshot, делаем restore_from_dir в новый корень.
// - Проверяем:
//   - Все старые ключи (“kNNN”) восстановлены ровно как до snapshot (старые значения).
//   - Новые ключи “z...” (созданные после snapshot) отсутствуют в восстановленном бэкапе.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use oorandom::Rand64;

use QuiverDB::{init_db, read_meta, Db, Directory};
use QuiverDB::backup::{backup_to_dir, restore_from_dir};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let base = std::env::temp_dir();
    base.join(format!("qdbtest-hot-backup-{prefix}-{pid}-{t}-{id}"))
}

#[ignore]
#[test]
fn backup_hot_leader_snapshot_consistency() -> Result<()> {
    // Снижаем дребезг LSN/fsync
    std::env::set_var("P1_WAL_COALESCE_MS", "0");

    let leader = unique_root("leader");
    let follower = unique_root("follower");
    fs::create_dir_all(&leader)?;
    fs::create_dir_all(&follower)?;
    init_db(&leader, 4096)?;
    Directory::create(&leader, 128)?;

    // Начальный набор ключей (часть overflow)
    let ps = read_meta(&leader)?.page_size as usize;
    let threshold = ps / 4;

    let key_count = 64usize;
    let mut rng = Rand64::new(0xBADC0FFEE);

    let mut initial: HashMap<String, Vec<u8>> = HashMap::new();
    for i in 0..key_count {
        let k = format!("k{:03}", i);
        // разные размеры вокруг порога overflow
        let size_choice = i % 6;
        let len = match size_choice {
            0 => threshold.saturating_sub(1),
            1 => threshold,
            2 => threshold + 1,
            3 => (threshold + 250).min(ps - 64),
            4 => (threshold + 600).min(ps - 64),
            _ => (rng.rand_u64() as usize % (3 * threshold + 2048)).min(ps - 64),
        };
        let mut v = vec![0u8; len.max(1)];
        for j in 0..v.len() {
            v[j] = ((i as u8).wrapping_mul(17)).wrapping_add((j as u8).wrapping_mul(29));
        }
        initial.insert(k, v);
    }

    // Записываем initial на лидере
    {
        let mut db = Db::open(&leader)?;
        for (k, v) in &initial {
            db.put(k.as_bytes(), v)?;
        }
    }

    // Начинаем snapshot
    let mut db = Db::open(&leader)?;
    let mut snap = db.snapshot_begin()?;
    let _snap_lsn = snap.lsn();

    // Пока snapshot держится — churn (обновляем/удаляем/создаём новые ключи)
    // Используем того же writer'а db (single-writer), чтобы freeze_if_needed отрабатывал.
    let ops = 1200usize;
    for i in 0..ops {
        let dice = rng.rand_u64() % 10;
        if dice < 7 {
            // update существующих
            let idx = (rng.rand_u64() as usize) % key_count;
            let key = format!("k{:03}", idx);
            let choice = rng.rand_u64() % 7;
            let len = match choice {
                0 => threshold.saturating_sub(1),
                1 => threshold,
                2 => threshold + 1 + ((rng.rand_u64() % 128) as usize),
                3 => (threshold + 300).min(ps - 64),
                4 => (threshold + 700).min(ps - 64),
                5 => (rng.rand_u64() as usize % (2 * threshold + 512)).min(ps - 64),
                _ => (rng.rand_u64() as usize % (3 * threshold + 2048)).min(ps - 64),
            };
            let mut val = vec![0u8; len.max(1)];
            for j in 0..val.len() {
                val[j] = ((i as u8).wrapping_mul(41)).wrapping_add((j as u8).wrapping_mul(11));
            }
            db.put(key.as_bytes(), &val)?;
        } else if dice < 9 {
            // delete некоторых исходных ключей
            let idx = (rng.rand_u64() as usize) % key_count;
            let key = format!("k{:03}", idx);
            let _ = db.del(key.as_bytes())?;
        } else {
            // новые ключи после snapshot (префикс z...)
            let key = format!("z{:06}", i);
            let len = (rng.rand_u64() as usize % (threshold + 1024)).min(ps - 64);
            let mut val = vec![0u8; len.max(1)];
            for j in 0..val.len() {
                val[j] = ((i as u8).wrapping_mul(23)).wrapping_add((j as u8).wrapping_mul(7));
            }
            db.put(key.as_bytes(), &val)?;
        }
    }

    // Делаем backup из snapshot (на горячую — writer уже поменял кучу ключей)
    let backup_dir = leader.join("backup_hot");
    backup_to_dir(&db, &snap, &backup_dir, None)?;

    // Закрываем snapshot
    db.snapshot_end(&mut snap)?;

    // Восстанавливаем в follower
    restore_from_dir(&follower, &backup_dir)?;

    // Проверяем: все исходные “k..” в follower равны initial (срез до snapshot),
    // а ключи “z..” (созданные после snapshot) отсутствуют.
    {
        let db_f = Db::open(&follower)?;
        for (k, v_old) in &initial {
            let got = db_f.get(k.as_bytes())?;
            let got = got.expect("initial key must exist in restored snapshot");
            assert_eq!(
                got.as_slice(),
                v_old.as_slice(),
                "restored snapshot must contain OLD value for key {}",
                k
            );
        }
        // Префикс z не должен быть в срезе
        let z_pairs = db_f.scan_prefix(b"z")?;
        assert!(
            z_pairs.is_empty(),
            "keys created after snapshot (prefix 'z') must not be in restored backup"
        );
    }

    Ok(())
}