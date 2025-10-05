use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;

use QuiverDB::{init_db, read_meta, Directory};
use QuiverDB::cli::cdc::{cmd_cdc_record, cmd_cdc_replay};
use QuiverDB::pager::Pager;
use QuiverDB::page_rh::{rh_page_is_kv, rh_kv_lookup};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let base = std::env::temp_dir();
    base.join(format!("qdbtest-rec-repl-{prefix}-{pid}-{t}-{id}"))
}

// Сканирует все RH-страницы у root, ищет key (без использования directory).
fn db_has_key(root: &PathBuf, key: &[u8]) -> Result<bool> {
    let pager = Pager::open(root)?;
    let ps = pager.meta.page_size as usize;
    let mut page = vec![0u8; ps];

    for pid in 0..pager.meta.next_page_id {
        if pager.read_page(pid, &mut page).is_ok() {
            if rh_page_is_kv(&page) {
                if let Ok(v) = rh_kv_lookup(&page, pager.meta.hash_kind, key) {
                    if v.is_some() {
                        return Ok(true);
                    }
                }
            }
        }
    }
    Ok(false)
}

#[test]
fn cdc_record_replay_range_only_middle_key() -> Result<()> {
    // Лидер/фолловер, одна сессия writer'а на лидере для всех записей (чтобы WAL содержал 3 кадра).
    let leader = unique_root("leader-range");
    let follower = unique_root("follower-range");
    fs::create_dir_all(&leader)?;
    fs::create_dir_all(&follower)?;
    init_db(&leader, 4096)?;
    init_db(&follower, 4096)?;
    Directory::create(&leader, 64)?;

    use QuiverDB::Db;

    let k1 = b"k1";
    let k2 = b"k2";
    let k3 = b"k3";

    // Одна writer-сессия: записываем три ключа
    {
        let mut db = Db::open(&leader)?;
        db.put(k1, b"v1")?;
        let lsn1 = read_meta(&leader)?.last_lsn;
        db.put(k2, b"v2")?;
        let lsn2 = read_meta(&leader)?.last_lsn;
        db.put(k3, b"v3")?;
        let _lsn3 = read_meta(&leader)?.last_lsn;

        // Запишем только диапазон (lsn1, lsn2] — т.е. кадр с k2
        let out = leader.join("slice.bin");
        cmd_cdc_record(leader.clone(), out.clone(), Some(lsn1), Some(lsn2))?;

        // Применим срез к фолловеру
        cmd_cdc_replay(follower.clone(), Some(out), None, None)?;
    }

    // Проверяем: у фолловера есть только k2
    assert!(!db_has_key(&follower, k1)?, "k1 must be absent on follower");
    assert!( db_has_key(&follower, k2)?, "k2 must be present on follower");
    assert!(!db_has_key(&follower, k3)?, "k3 must be absent on follower");
    Ok(())
}

#[test]
fn cdc_record_replay_idempotent() -> Result<()> {
    // Тест идемпотентности: один кадр, применяем дважды — состояние не портится.
    let leader = unique_root("leader-idem");
    let follower = unique_root("follower-idem");
    fs::create_dir_all(&leader)?;
    fs::create_dir_all(&follower)?;
    init_db(&leader, 4096)?;
    init_db(&follower, 4096)?;
    Directory::create(&leader, 64)?;

    use QuiverDB::Db;

    let key = b"idemp";
    {
        let mut db = Db::open(&leader)?;
        db.put(key, b"v")?;
        let lsn = read_meta(&leader)?.last_lsn;

        // Сохраняем только кадры с lsn > 0 и <= lsn (фактически один кадр)
        let out = leader.join("one.bin");
        cmd_cdc_record(leader.clone(), out.clone(), Some(0), Some(lsn))?;

        // Применяем дважды
        cmd_cdc_replay(follower.clone(), Some(out.clone()), None, None)?;
        cmd_cdc_replay(follower.clone(), Some(out), None, None)?;
    }

    assert!(db_has_key(&follower, key)?, "key must exist after idempotent apply twice");
    Ok(())
}