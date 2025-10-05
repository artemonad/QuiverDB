// tests/backup_restore.rs
//
// Запуск только этого файла:
//   cargo test --test backup_restore -- --nocapture
//
// Покрываем:
// 1) Полный бэкап + рестор: значения (включая overflow) восстанавливаются корректно.
// 2) Инкрементальный бэкап: применяем поверх полного — итоговое состояние соответствует лидеру.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;

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
    base.join(format!("qdbtest-backup-{prefix}-{pid}-{t}-{id}"))
}

#[test]
fn backup_full_and_restore_roundtrip() -> Result<()> {
    // Делаем тайминги LSN детерминированнее
    std::env::set_var("P1_WAL_COALESCE_MS", "0");

    let leader = unique_root("full-leader");
    let follower = unique_root("full-follower");
    fs::create_dir_all(&leader)?;
    fs::create_dir_all(&follower)?;
    init_db(&leader, 4096)?;
    Directory::create(&leader, 64)?;

    // Подготовка данных на лидере (включая overflow)
    {
        let ps = read_meta(&leader)?.page_size as usize;
        let threshold = ps / 4;
        let big = vec![0xABu8; threshold + 300];

        let mut db = Db::open(&leader)?;
        db.put(b"alpha", b"1")?;
        db.put(b"beta", b"2")?;
        db.put(b"big", &big)?;
    }

    // Бэкап (full) через SnapshotHandle
    let backup_dir = leader.join("backup_full");
    {
        let mut db = Db::open(&leader)?;
        let mut snap = db.snapshot_begin()?;
        backup_to_dir(&db, &snap, &backup_dir, None)?;
        db.snapshot_end(&mut snap)?;
    }

    // Рестор в назначение
    restore_from_dir(&follower, &backup_dir)?;

    // Проверяем содержимое у фолловера
    {
        let db = Db::open(&follower)?;
        let ps = db.pager.meta.page_size as usize;
        let threshold = ps / 4;
        let expected_big = vec![0xABu8; threshold + 300];

        assert_eq!(db.get(b"alpha")?.as_deref(), Some(b"1".as_ref()));
        assert_eq!(db.get(b"beta")?.as_deref(), Some(b"2".as_ref()));
        let got_big = db.get(b"big")?.expect("big must exist after restore");
        assert_eq!(got_big.len(), expected_big.len());
        assert_eq!(got_big, expected_big);
    }

    Ok(())
}

#[test]
fn backup_incremental_apply_over_full() -> Result<()> {
    std::env::set_var("P1_WAL_COALESCE_MS", "0");

    let leader = unique_root("incr-leader");
    let follower = unique_root("incr-follower");
    fs::create_dir_all(&leader)?;
    fs::create_dir_all(&follower)?;
    init_db(&leader, 4096)?;
    Directory::create(&leader, 64)?;

    let ps = read_meta(&leader)?.page_size as usize;
    let threshold = ps / 4;

    // 1) Базовые записи
    {
        let big1 = vec![0xCDu8; threshold + 200];
        let mut db = Db::open(&leader)?;
        db.put(b"k1", b"v1")?;
        db.put(b"big", &big1)?;
    }
    let lsn1 = read_meta(&leader)?.last_lsn;

    // Снимем базовый фулл-бэкап
    let backup_full_dir = leader.join("backup_full_base");
    {
        let mut db = Db::open(&leader)?;
        let mut snap = db.snapshot_begin()?;
        backup_to_dir(&db, &snap, &backup_full_dir, None)?;
        db.snapshot_end(&mut snap)?;
    }

    // 2) Изменения после базового бэкапа
    {
        let big2 = vec![0xEEu8; threshold + 350];
        let mut db = Db::open(&leader)?;
        db.put(b"k1", b"v1b")?;
        db.put(b"k2", b"v2")?;
        db.put(b"big", &big2)?;
    }
    let _lsn2 = read_meta(&leader)?.last_lsn;

    // Снимем инкрементальный бэкап (все страницы с LSN ∈ (lsn1, S])
    let backup_incr_dir = leader.join("backup_incr");
    {
        let mut db = Db::open(&leader)?;
        let mut snap = db.snapshot_begin()?;
        backup_to_dir(&db, &snap, &backup_incr_dir, Some(lsn1))?;
        db.snapshot_end(&mut snap)?;
    }

    // Восстановим базовый фулл на фолловере
    restore_from_dir(&follower, &backup_full_dir)?;
    {
        let db = Db::open(&follower)?;
        let big1 = vec![0xCDu8; threshold + 200];
        assert_eq!(db.get(b"k1")?.as_deref(), Some(b"v1".as_ref()));
        assert_eq!(db.get(b"k2")?.as_deref(), None);
        let got_big = db.get(b"big")?.expect("big must exist after base restore");
        assert_eq!(got_big, big1);
    }

    // Применим инкремент поверх базового
    restore_from_dir(&follower, &backup_incr_dir)?;

    // Проверяем итоговое состояние соответствует лидеру после изменений
    {
        let db = Db::open(&follower)?;
        let big2 = vec![0xEEu8; threshold + 350];
        assert_eq!(db.get(b"k1")?.as_deref(), Some(b"v1b".as_ref()));
        assert_eq!(db.get(b"k2")?.as_deref(), Some(b"v2".as_ref()));
        let got_big = db.get(b"big")?.expect("big must exist after incr restore");
        assert_eq!(got_big, big2);
    }

    Ok(())
}