// tests/check_repair_v0_9.rs
//
// Проверяем:
// - Directory CRC: корректный при создании и mismatch при порче.
// - Repair: обнаружение и освобождение сиротских overflow-страниц.
// - Check: команда выполняется без ошибок и сканирует БД.
//
// Запуск:
//   cargo test --test check_repair_v0_9 -- --nocapture

use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use byteorder::{ByteOrder, LittleEndian}; // Нужен ByteOrder для LittleEndian::read/write_uXX
use crc32fast::Hasher as Crc32;

use QuiverDB::{init_db, read_meta, Db, Directory};
use QuiverDB::consts::{
    DIR_FILE, DIR_HDR_SIZE, DIR_MAGIC, FREE_FILE, FREE_HDR_SIZE, FREE_MAGIC, NO_PAGE,
};
use QuiverDB::pager::Pager;
use QuiverDB::page_ovf::ovf_page_init;
use QuiverDB::page_rh::{rh_page_is_kv, rh_page_update_crc};
use QuiverDB::cli::admin::{cmd_check, cmd_repair};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let base = std::env::temp_dir();
    base.join(format!("qdbtest-chk-rep-{prefix}-{pid}-{t}-{id}"))
}

// ----- helpers -----

fn read_free_set(root: &PathBuf) -> Result<std::collections::HashSet<u64>> {
    let path = root.join(FREE_FILE);
    let mut set = std::collections::HashSet::new();
    if !path.exists() {
        return Ok(set);
    }
    let mut f = OpenOptions::new().read(true).open(&path)?;
    let mut magic = [0u8; 8];
    f.read_exact(&mut magic)?;
    assert_eq!(&magic, FREE_MAGIC, "bad FREE magic");
    let mut hdr = vec![0u8; FREE_HDR_SIZE - 8];
    f.read_exact(&mut hdr)?;
    let len = f.metadata()?.len();
    let mut pos = FREE_HDR_SIZE as u64;
    while pos + 8 <= len {
        let mut buf8 = [0u8; 8];
        f.seek(SeekFrom::Start(pos))?;
        f.read_exact(&mut buf8)?;
        let pid = LittleEndian::read_u64(&buf8);
        set.insert(pid);
        pos += 8;
    }
    Ok(set)
}

// compute CRC over [version u32][buckets u32] + heads bytes
fn compute_dir_crc_for_test(version: u32, buckets: u32, heads_bytes: &[u8]) -> u32 {
    let mut h = Crc32::new();
    let mut buf4 = [0u8; 4];
    LittleEndian::write_u32(&mut buf4, version);
    h.update(&buf4);
    LittleEndian::write_u32(&mut buf4, buckets);
    h.update(&buf4);
    h.update(heads_bytes);
    h.finalize()
}

// ----- tests -----

#[test]
fn directory_crc_valid_and_detects_corruption() -> Result<()> {
    let root = unique_root("dir-crc");
    fs::create_dir_all(&root)?;
    init_db(&root, 4096)?;
    let _dir_created = Directory::create(&root, 16)?;

    // Откроется и валидирует CRC
    let _dir2 = Directory::open(&root)?;

    // Прочитаем файл и сверим CRC вручную
    let path = root.join(DIR_FILE);
    let mut f = OpenOptions::new().read(true).open(&path)?;
    let mut magic = [0u8; 8];
    f.read_exact(&mut magic)?;
    assert_eq!(&magic, DIR_MAGIC);

    // version u32
    let mut ver4 = [0u8; 4];
    f.read_exact(&mut ver4)?;
    let version = LittleEndian::read_u32(&ver4);
    // buckets u32
    let mut b4 = [0u8; 4];
    f.read_exact(&mut b4)?;
    let buckets = LittleEndian::read_u32(&b4);
    // stored_crc u64 (берём низ 32 бита)
    let mut c8 = [0u8; 8];
    f.read_exact(&mut c8)?;
    let stored_crc = LittleEndian::read_u64(&c8) as u32;

    let heads_len = (buckets as usize) * 8;
    let mut heads = vec![0u8; heads_len];
    f.read_exact(&mut heads)?;

    let calc_crc = compute_dir_crc_for_test(version, buckets, &heads);
    assert_eq!(stored_crc, calc_crc, "stored CRC must match calculated");

    // Портим один байт в heads и убеждаемся, что open теперь падает по CRC mismatch.
    drop(f);
    let mut fw = OpenOptions::new().read(true).write(true).open(&path)?;
    let off = DIR_HDR_SIZE as u64; // начало heads
    fw.seek(SeekFrom::Start(off))?;
    fw.read_exact(&mut heads)?;
    heads[0] ^= 0xFF;
    fw.seek(SeekFrom::Start(off))?;
    fw.write_all(&heads)?;
    fw.sync_all()?;

    // Не используем unwrap_err (требует Debug у Ok-типа)
    let res = Directory::open(&root);
    let err = match res {
        Ok(_) => panic!("expected CRC error after corrupting DIR heads"),
        Err(e) => e,
    };
    let msg = err.to_string().to_ascii_lowercase();
    assert!(msg.contains("crc"), "expected CRC error, got: {msg}");

    // Восстановим корректный файл (пересоздадим), чтобы не влиять на другие тесты
    let _ = fs::remove_file(&path);
    let _dir3 = Directory::create(&root, 16)?;
    let _ok = Directory::open(&root)?;
    Ok(())
}

#[test]
fn repair_frees_orphan_overflow_pages() -> Result<()> {
    std::env::set_var("P1_WAL_COALESCE_MS", "0");

    let root = unique_root("repair");
    fs::create_dir_all(&root)?;
    init_db(&root, 4096)?;
    Directory::create(&root, 64)?;

    // 1) Добавим валидную запись с overflow (для маркировки достижимых цепочек)
    {
        let mut db = Db::open(&root)?;
        let ps = read_meta(&root)?.page_size as usize;
        let threshold = ps / 4;
        let val = vec![0xAA; threshold + 500]; // гарантированный overflow
        db.put(b"ovf_key", &val)?;
        let got = db.get(b"ovf_key")?.expect("must exist");
        assert_eq!(got, val);
    }

    // 2) Создадим несколько "сиротских" overflow-страниц напрямую через Pager
    let mut pager = Pager::open(&root)?;
    let ps = pager.meta.page_size as usize;

    // orphan 1
    let pid1 = pager.allocate_one_page()?;
    let mut b1 = vec![0u8; ps];
    ovf_page_init(&mut b1, pid1)?;
    rh_page_update_crc(&mut b1)?;
    pager.commit_page(pid1, &mut b1)?;

    // orphan 2 (одиночный)
    let pid2 = pager.allocate_one_page()?;
    let mut b2 = vec![0u8; ps];
    ovf_page_init(&mut b2, pid2)?;
    rh_page_update_crc(&mut b2)?;
    pager.commit_page(pid2, &mut b2)?;

    // 3) Посчитаем орфанов "вручную" (overflow pages not marked as reachable, not in free-list)
    let before_orphans = {
        let free = read_free_set(&root)?;
        let mut marked = std::collections::HashSet::new();

        // Маркируем reachable из каталога
        let dir = Directory::open(&root)?;
        for b in 0..dir.bucket_count {
            let mut pid = dir.head(b)?;
            while pid != NO_PAGE {
                let mut buf = vec![0u8; ps];
                if pager.read_page(pid, &mut buf).is_err() {
                    break;
                }
                if !rh_page_is_kv(&buf) {
                    break;
                }
                // извлечём placeholder'ы
                if let Ok(items) = QuiverDB::page_rh::rh_kv_list(&buf) {
                    for (_k, v) in items {
                        if v.len() == 18 && v[0] == 0xFF {
                            let head = LittleEndian::read_u64(&v[10..18]);
                            let mut cur = head;
                            while cur != NO_PAGE && !marked.contains(&cur) {
                                let mut ob = vec![0u8; ps];
                                if pager.read_page(cur, &mut ob).is_err() {
                                    break;
                                }
                                if let Ok(h) = QuiverDB::page_ovf::ovf_header_read(&ob) {
                                    marked.insert(cur);
                                    cur = h.next_page_id;
                                } else {
                                    break;
                                }
                            }
                        }
                    }
                }
                let h = QuiverDB::page_rh::rh_header_read(&buf)?;
                pid = h.next_page_id;
            }
        }

        // Соберём все overflow
        let mut all_ovf = Vec::new();
        for pid in 0..pager.meta.next_page_id {
            let mut buf = vec![0u8; ps];
            if pager.read_page(pid, &mut buf).is_ok() {
                if QuiverDB::page_ovf::ovf_header_read(&buf).is_ok() {
                    all_ovf.push(pid);
                }
            }
        }

        all_ovf
            .into_iter()
            .filter(|pid| !marked.contains(pid) && !free.contains(pid))
            .count()
    };

    assert!(
        before_orphans >= 2,
        "expected at least 2 orphan overflow pages before repair, got {}",
        before_orphans
    );

    // 4) Запустим repair и проверим, что сироты ушли в free-list
    cmd_repair(root.clone())?;

    let after_orphans = {
        let free = read_free_set(&root)?;
        let mut marked = std::collections::HashSet::new();
        let dir = Directory::open(&root)?;
        for b in 0..dir.bucket_count {
            let mut pid = dir.head(b)?;
            while pid != NO_PAGE {
                let mut buf = vec![0u8; ps];
                if pager.read_page(pid, &mut buf).is_err() {
                    break;
                }
                if !rh_page_is_kv(&buf) {
                    break;
                }
                if let Ok(items) = QuiverDB::page_rh::rh_kv_list(&buf) {
                    for (_k, v) in items {
                        if v.len() == 18 && v[0] == 0xFF {
                            let head = LittleEndian::read_u64(&v[10..18]);
                            let mut cur = head;
                            while cur != NO_PAGE && !marked.contains(&cur) {
                                let mut ob = vec![0u8; ps];
                                if pager.read_page(cur, &mut ob).is_err() {
                                    break;
                                }
                                if let Ok(h) = QuiverDB::page_ovf::ovf_header_read(&ob) {
                                    marked.insert(cur);
                                    cur = h.next_page_id;
                                } else {
                                    break;
                                }
                            }
                        }
                    }
                }
                let h = QuiverDB::page_rh::rh_header_read(&buf)?;
                pid = h.next_page_id;
            }
        }

        // Соберём overflow
        let mut all_ovf = Vec::new();
        for pid in 0..pager.meta.next_page_id {
            let mut buf = vec![0u8; ps];
            if pager.read_page(pid, &mut buf).is_ok() {
                if QuiverDB::page_ovf::ovf_header_read(&buf).is_ok() {
                    all_ovf.push(pid);
                }
            }
        }
        all_ovf
            .into_iter()
            .filter(|pid| !marked.contains(pid) && !free.contains(pid))
            .count()
    };

    assert_eq!(
        after_orphans, 0,
        "expected no orphan overflow pages after repair"
    );

    Ok(())
}

#[test]
fn cmd_check_runs_without_errors() -> Result<()> {
    std::env::set_var("P1_WAL_COALESCE_MS", "0");

    let root = unique_root("check");
    fs::create_dir_all(&root)?;
    init_db(&root, 4096)?;
    Directory::create(&root, 32)?;

    // Немного данных
    {
        let mut db = Db::open(&root)?;
        db.put(b"k1", b"v1")?;
        db.put(b"k2", b"v2")?;
    }

    // Команда check должна выполниться без ошибок
    cmd_check(root)?;
    Ok(())
}