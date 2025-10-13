use anyhow::Result;
use std::fs;
use std::path::PathBuf;

use QuiverDB::db::Db;
use QuiverDB::meta::{set_clean_shutdown};
use QuiverDB::pager::{Pager, DATA_SEG_EXT, DATA_SEG_PREFIX};
use QuiverDB::page::kv_init_v3;
use QuiverDB::wal::{WAL_FILE, WAL_HDR_SIZE};

#[test]
fn crash_and_replay_restores_data_and_truncates_wal() -> Result<()> {
    // Подготовка
    let root = unique_root("crash-replay");
    fs::create_dir_all(&root)?;

    // init
    let page_size = 64 * 1024;
    let buckets = 128;
    Db::init(&root, page_size, buckets)?;

    // writer put (единичная страница)
    {
        let mut pager = Pager::open(&root)?;
        let pid = pager.allocate_one_page()?;
        let ps = pager.meta.page_size as usize;
        let mut page = vec![0u8; ps];
        kv_init_v3(&mut page, pid, 0)?;
        pager.commit_page(pid, &mut page)?;

        // убедимся, что last_lsn продвинулся (в памяти)
        let lsn_mem = pager.meta.last_lsn;
        assert!(lsn_mem > 0, "last_lsn must be > 0 after put (in-memory)");
    }

    // Имитация "краша": удалим сегмент 1 и clean_shutdown=false
    let seg1 = root.join(format!("{}{:06}.{}", DATA_SEG_PREFIX, 1, DATA_SEG_EXT));
    if seg1.exists() {
        fs::remove_file(&seg1)?;
    }
    set_clean_shutdown(&root, false)?;

    // WAL replay
    Pager::wal_replay_with_pager(&root)?;

    // Страница должна восстановиться
    {
        let pager = Pager::open(&root)?;
        let ps = pager.meta.page_size as usize;
        let mut buf = vec![0u8; ps];
        pager.read_page(0, &mut buf)?; // page 0 должна существовать
    }

    // WAL должен быть усечён до заголовка
    let wal_len = fs::metadata(root.join(WAL_FILE))?.len();
    assert_eq!(wal_len, WAL_HDR_SIZE as u64, "WAL must be truncated to header after replay");

    Ok(())
}

fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("qdb2-{}-{}-{}", prefix, pid, t))
}