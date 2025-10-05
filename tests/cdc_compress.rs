// tests/cdc_compress.rs
//
// Запуск только этого файла:
//   cargo test --test cdc_compress -- --nocapture
//
// Сценарии:
// 1) GZIP: wal-ship с P1_SHIP_COMPRESS=gzip -> файл; wal-apply читает gzip-декодер поверх файла.
// 2) ZSTD: wal-ship с P1_SHIP_COMPRESS=zstd -> файл; wal-apply читает zstd-декодер поверх файла.

use std::fs;
use std::fs::OpenOptions;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use flate2::read::GzDecoder;
use zstd::stream::read::Decoder as ZstdDecoder;

use QuiverDB::{init_db, Db, Directory};
use QuiverDB::cli::cdc::cmd_wal_ship_ext;
use QuiverDB::cli::wal_apply_from_stream;
use QuiverDB::pager::Pager;
use QuiverDB::page_rh::{rh_kv_lookup, rh_page_is_kv};

// ------- helpers -------

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let base = std::env::temp_dir();
    base.join(format!("qdbtest-compress-{prefix}-{pid}-{t}-{id}"))
}

// Скан всех RH-страниц: ищем ключ (без directory).
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

// ------- tests -------

#[test]
fn cdc_ship_gzip_and_apply() -> Result<()> {
    std::env::set_var("P1_WAL_COALESCE_MS", "0");

    let leader = unique_root("leader-gzip");
    let follower = unique_root("follower-gzip");
    fs::create_dir_all(&leader)?;
    fs::create_dir_all(&follower)?;
    init_db(&leader, 4096)?;
    init_db(&follower, 4096)?;
    Directory::create(&leader, 64)?;

    // Пишем пару ключей на лидере
    {
        let mut db = Db::open(&leader)?;
        db.put(b"gz_a", b"1")?;
        db.put(b"gz_b", b"2")?;
    }

    // Ship с компрессией gzip в файл
    let out = leader.join("wal_gzip.bin");
    std::env::set_var("P1_SHIP_COMPRESS", "gzip");
    let sink = format!("file://{}", out.display());
    cmd_wal_ship_ext(leader.clone(), false, None, Some(sink))?;
    std::env::remove_var("P1_SHIP_COMPRESS");

    // Применяем: читаем файл через GzDecoder и подаём в wal_apply_from_stream
    {
        let f = OpenOptions::new().read(true).open(&out)?;
        let mut dec = GzDecoder::new(f);
        wal_apply_from_stream(&follower, &mut dec)?;
    }

    // Проверяем ключи на фолловере
    assert!(db_has_key(&follower, b"gz_a")?, "gz_a must exist after gzip apply");
    assert!(db_has_key(&follower, b"gz_b")?, "gz_b must exist after gzip apply");

    Ok(())
}

#[test]
fn cdc_ship_zstd_and_apply() -> Result<()> {
    std::env::set_var("P1_WAL_COALESCE_MS", "0");

    let leader = unique_root("leader-zstd");
    let follower = unique_root("follower-zstd");
    fs::create_dir_all(&leader)?;
    fs::create_dir_all(&follower)?;
    init_db(&leader, 4096)?;
    init_db(&follower, 4096)?;
    Directory::create(&leader, 64)?;

    // Пишем пару ключей на лидере
    {
        let mut db = Db::open(&leader)?;
        db.put(b"zs_a", b"1")?;
        db.put(b"zs_b", b"2")?;
    }

    // Ship с компрессией zstd в файл
    let out = leader.join("wal_zstd.bin");
    std::env::set_var("P1_SHIP_COMPRESS", "zstd");
    let sink = format!("file://{}", out.display());
    cmd_wal_ship_ext(leader.clone(), false, None, Some(sink))?;
    std::env::remove_var("P1_SHIP_COMPRESS");

    // Применяем: читаем файл через ZstdDecoder и подаём в wal_apply_from_stream
    {
        let f = OpenOptions::new().read(true).open(&out)?;
        let mut dec = ZstdDecoder::new(f)?;
        wal_apply_from_stream(&follower, &mut dec)?;
    }

    // Проверяем ключи на фолловере
    assert!(db_has_key(&follower, b"zs_a")?, "zs_a must exist after zstd apply");
    assert!(db_has_key(&follower, b"zs_b")?, "zs_b must exist after zstd apply");

    Ok(())
}