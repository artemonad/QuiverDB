// tests/check_strict_v1.rs
//
// Проверяем strict-режим проверки БД:
// - На чистой БД strict проходит (OK).
// - При наличии сиротских overflow-страниц strict падает, после repair — проходит.
// - При порче CRC каталога strict падает.
//
// Запуск:
//   cargo test --test check_strict_v1 -- --nocapture

use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use byteorder::{LittleEndian, ReadBytesExt};

use QuiverDB::{init_db, Db, Directory};
use QuiverDB::consts::{DIR_FILE, DIR_HDR_SIZE, DIR_MAGIC};
use QuiverDB::pager::Pager;
use QuiverDB::page_ovf::ovf_page_init;
use QuiverDB::page_rh::rh_page_update_crc;
use QuiverDB::cli::admin::{cmd_check_strict, cmd_repair};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let base = std::env::temp_dir();
    base.join(format!("qdbtest-strict-{prefix}-{pid}-{t}-{id}"))
}

#[test]
fn strict_ok_on_clean_db() -> Result<()> {
    std::env::set_var("P1_WAL_COALESCE_MS", "0");

    let root = unique_root("clean");
    fs::create_dir_all(&root)?;
    init_db(&root, 4096)?;
    Directory::create(&root, 32)?;

    {
        let mut db = Db::open(&root)?;
        db.put(b"k1", b"v1")?;
        db.put(b"k2", b"v2")?;
        let _ = db.get(b"k1")?;
    }

    // strict должен проходить без ошибок
    cmd_check_strict(root, true)?;
    Ok(())
}

#[test]
fn strict_fails_on_orphans_and_passes_after_repair() -> Result<()> {
    std::env::set_var("P1_WAL_COALESCE_MS", "0");

    let root = unique_root("orphans");
    fs::create_dir_all(&root)?;
    init_db(&root, 4096)?;
    Directory::create(&root, 32)?;

    // Создадим сиротские overflow-страницы напрямую через Pager
    let mut pager = Pager::open(&root)?;
    let ps = pager.meta.page_size as usize;

    // orphan #1
    let pid1 = pager.allocate_one_page()?;
    let mut b1 = vec![0u8; ps];
    ovf_page_init(&mut b1, pid1)?;
    rh_page_update_crc(&mut b1)?;
    pager.commit_page(pid1, &mut b1)?;

    // orphan #2
    let pid2 = pager.allocate_one_page()?;
    let mut b2 = vec![0u8; ps];
    ovf_page_init(&mut b2, pid2)?;
    rh_page_update_crc(&mut b2)?;
    pager.commit_page(pid2, &mut b2)?;

    // strict должен упасть
    let res = cmd_check_strict(root.clone(), true);
    assert!(res.is_err(), "strict check must fail with orphan overflow pages");

    // repair освобождает сироты
    cmd_repair(root.clone())?;

    // strict теперь проходит
    cmd_check_strict(root, true)?;
    Ok(())
}

#[test]
fn strict_fails_on_directory_crc_corruption() -> Result<()> {
    let root = unique_root("dircrc");
    fs::create_dir_all(&root)?;
    init_db(&root, 4096)?;
    Directory::create(&root, 16)?;

    // Портим 1 байт в области heads каталога — open начнёт падать по CRC
    let dir_path = root.join(DIR_FILE);
    let mut f = OpenOptions::new().read(true).write(true).open(&dir_path)?;
    let mut magic = [0u8; 8];
    f.read_exact(&mut magic)?;
    assert_eq!(&magic, DIR_MAGIC);
    let _version = f.read_u32::<LittleEndian>()?;
    let buckets = f.read_u32::<LittleEndian>()?;
    let _stored_crc = f.read_u64::<LittleEndian>()?;

    // heads начинаются с смещения DIR_HDR_SIZE
    let heads_len = (buckets as usize) * 8;
    let mut heads = vec![0u8; heads_len];
    f.seek(SeekFrom::Start(DIR_HDR_SIZE as u64))?;
    f.read_exact(&mut heads)?;
    heads[0] ^= 0xFF; // flip byte
    f.seek(SeekFrom::Start(DIR_HDR_SIZE as u64))?;
    f.write_all(&mut heads)?;
    f.sync_all()?;

    // strict должен упасть (Directory: ERROR)
    let res = cmd_check_strict(root, true);
    assert!(res.is_err(), "strict check must fail on DIR CRC mismatch");
    Ok(())
}