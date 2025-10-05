// tests/cdc_file_sink.rs
//
// Как запустить только этот тест:
//   cargo test --test cdc_file_sink -- --nocapture
//
// Сценарий:
// 1) Лидер: создаём БД, пишем несколько ключей.
// 2) wal-ship --sink=file://... сохраняет полный WAL-стрим (header + frames) в файл.
// 3) wal-apply применяет сохранённый файл к фолловеру.
// 4) Проверяем, что у фолловера появились ключи (сканируя все RH-страницы напрямую).

use std::fs;
use std::fs::OpenOptions;
use std::io::Read;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;

use QuiverDB::{init_db, Db, Directory};
use QuiverDB::cli::cdc::{cmd_wal_ship_ext};
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
    base.join(format!("qdbtest-file-sink-{prefix}-{pid}-{t}-{id}"))
}

// Скан всех RH-страниц фолловера: ищем ключ (без directory).
fn follower_has_key(root: &PathBuf, key: &[u8]) -> Result<bool> {
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

// ------- test -------

#[test]
fn cdc_file_sink_roundtrip() -> Result<()> {
    // Детеминизируем тайминги коалессы WAL (не обязательно, но так стабильнее)
    std::env::set_var("P1_WAL_COALESCE_MS", "0");

    // leader + follower
    let leader = unique_root("leader");
    let follower = unique_root("follower");
    fs::create_dir_all(&leader)?;
    fs::create_dir_all(&follower)?;
    init_db(&leader, 4096)?;
    init_db(&follower, 4096)?;
    Directory::create(&leader, 64)?;

    // Пишем ключи на лидере
    {
        let mut db = Db::open(&leader)?;
        db.put(b"a1", b"v1")?;
        db.put(b"b2", b"v2")?;
    }

    // Ship WAL в файл
    let out = leader.join("wal_full.bin");
    let sink = format!("file://{}", out.display());
    cmd_wal_ship_ext(leader.clone(), false, None, Some(sink))?;

    // Применяем файл к фолловеру
    {
        let mut f = OpenOptions::new().read(true).open(&out)?;
        wal_apply_from_stream(&follower, &mut f)?;
    }

    // Проверяем наличие ключей на фолловере
    assert!(
        follower_has_key(&follower, b"a1")?,
        "follower must contain key 'a1' after apply"
    );
    assert!(
        follower_has_key(&follower, b"b2")?,
        "follower must contain key 'b2' after apply"
    );

    Ok(())
}