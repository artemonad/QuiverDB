use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;

use QuiverDB::{init_db, Directory};
use QuiverDB::cli::admin::{cmd_check_strict_json, cmd_metrics_json};
use QuiverDB::pager::Pager;
use QuiverDB::page_ovf::ovf_page_init;
use QuiverDB::page_rh::{rh_page_update_crc};
use QuiverDB::consts::NO_PAGE;

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let base = std::env::temp_dir();
    base.join(format!("qdbtest-json-{prefix}-{pid}-{t}-{id}"))
}

#[test]
fn metrics_json_runs_ok() -> Result<()> {
    let root = unique_root("metrics");
    fs::create_dir_all(&root)?;
    init_db(&root, 4096)?;
    Directory::create(&root, 16)?;

    // Должно успешно отработать (вывод в stdout; проверяем только отсутствие ошибок)
    cmd_metrics_json(true)?;
    Ok(())
}

#[test]
fn check_json_runs_ok_on_clean_db() -> Result<()> {
    let root = unique_root("check-clean");
    fs::create_dir_all(&root)?;
    init_db(&root, 4096)?;
    Directory::create(&root, 16)?;

    // На чистой БД — строгий JSON-check должен успешно завершаться
    cmd_check_strict_json(root, true, true)?;
    Ok(())
}

#[test]
fn check_json_strict_fails_on_orphans() -> Result<()> {
    let root = unique_root("check-orphans");
    fs::create_dir_all(&root)?;
    init_db(&root, 4096)?;
    Directory::create(&root, 16)?;

    // Создадим сиротские overflow-страницы
    let mut pager = Pager::open(&root)?;
    let ps = pager.meta.page_size as usize;

    // orphan 1
    let pid1 = pager.allocate_one_page()?;
    let mut b1 = vec![0u8; ps];
    ovf_page_init(&mut b1, pid1)?;
    rh_page_update_crc(&mut b1)?;
    pager.commit_page(pid1, &mut b1)?;

    // orphan 2
    let pid2 = pager.allocate_one_page()?;
    let mut b2 = vec![0u8; ps];
    ovf_page_init(&mut b2, pid2)?;
    rh_page_update_crc(&mut b2)?;
    pager.commit_page(pid2, &mut b2)?;

    // Убедимся, что каталог пустой (нет достижимых цепочек)
    let dir = Directory::open(&root)?;
    for b in 0..dir.bucket_count {
        assert_eq!(dir.head(b)?, NO_PAGE);
    }

    // Строгий JSON-check должен возвращать ошибку из‑за сирот
    let res = cmd_check_strict_json(root, true, true);
    assert!(res.is_err(), "strict JSON check must fail on orphans");
    Ok(())
}