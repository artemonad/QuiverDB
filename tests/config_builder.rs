use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;

use QuiverDB::{init_db, Db, Directory, set_clean_shutdown};
use QuiverDB::config::QuiverConfig;
use QuiverDB::consts::{WAL_FILE, WAL_HDR_SIZE};
use QuiverDB::pager::Pager;
use QuiverDB::page_rh::{rh_page_init, rh_page_update_crc};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let base = std::env::temp_dir();
    base.join(format!("qdbtest-builder-{prefix}-{pid}-{t}-{id}"))
}

#[test]
fn builder_data_fsync_off_keeps_wal_until_replay() -> Result<()> {
    // Инициализация БД
    let root = unique_root("fsync-off");
    fs::create_dir_all(&root)?;
    init_db(&root, 4096)?;
    Directory::create(&root, 8)?;

    // Конфиг с data_fsync=false (durability за счёт WAL, без truncate в commit_page)
    let cfg = QuiverConfig::from_env()
        .with_wal_coalesce_ms(0)
        .with_data_fsync(false);

    {
        let mut db = Db::open_with_config(&root, cfg.clone())?;
        db.put(b"x", b"y")?;
        // drop -> clean_shutdown=true
    }

    // WAL не должен быть усечён (длина > заголовка)
    let wal_path = root.join(WAL_FILE);
    let len1 = fs::metadata(&wal_path)?.len();
    assert!(
        len1 > WAL_HDR_SIZE as u64,
        "WAL must not be truncated when data_fsync=false; got len={len1}"
    );

    // Форсим незавершённость и повторное открытие writer'ом -> replay усечёт WAL
    set_clean_shutdown(&root, false)?;
    {
        let _db2 = Db::open(&root)?;
    }
    let len2 = fs::metadata(&wal_path)?.len();
    assert_eq!(
        len2,
        WAL_HDR_SIZE as u64,
        "WAL must be truncated to header after replay"
    );

    Ok(())
}

#[test]
fn builder_page_cache_enables_hits_on_repeat_reads() -> Result<()> {
    // Обнулим метрики
    QuiverDB::metrics::reset();

    let root = unique_root("cache");
    fs::create_dir_all(&root)?;
    init_db(&root, 4096)?;

    // Включим кэш страниц (8 страниц)
    let cfg = QuiverConfig::from_env()
        .with_page_cache_pages(8)
        .with_data_fsync(true);

    // Низкоуровневый тест на Pager (чтобы гарантировать чтение одной и той же страницы через один инстанс)
    let mut pager = Pager::open_with_config(&root, &cfg)?;
    let pid = pager.allocate_one_page()?;
    let ps = pager.meta.page_size as usize;

    // Запишем страницу
    let mut page = vec![0u8; ps];
    rh_page_init(&mut page, pid)?;
    rh_page_update_crc(&mut page)?;
    pager.commit_page(pid, &mut page)?;

    // Дважды читаем ту же страницу через один pager
    let mut buf = vec![0u8; ps];
    pager.read_page(pid, &mut buf)?; // miss
    pager.read_page(pid, &mut buf)?; // hit

    // Проверим метрики
    let m = QuiverDB::metrics::snapshot();
    assert!(
        m.page_cache_hits >= 1,
        "expected >=1 page_cache_hits after repeated reads, got {}",
        m.page_cache_hits
    );

    Ok(())
}