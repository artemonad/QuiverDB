use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use byteorder::{ByteOrder, LittleEndian};

use QuiverDB::{init_db, Db, Directory, read_meta};
use QuiverDB::pager::Pager;
use QuiverDB::page_ovf::{ovf_chunk_capacity, ovf_header_read};
use QuiverDB::consts::{FREE_FILE, FREE_HDR_SIZE, FREE_MAGIC};

// Если хочешь обойтись без внешнего RNG — можно закомментить heavy-тест ниже
// или добавить dev-dependency "oorandom" в Cargo.toml (см. ниже).

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let base = std::env::temp_dir();
    base.join(format!("qdbtest-ovf-{prefix}-{pid}-{t}-{id}"))
}

// Прочитать множество page_id, которые лежат в free-list (read-only).
fn read_free_set(root: &PathBuf) -> Result<HashSet<u64>> {
    let path = root.join(FREE_FILE);
    let mut set = HashSet::new();
    if !path.exists() {
        return Ok(set);
    }
    let mut f = OpenOptions::new().read(true).open(&path)?;
    let mut magic = [0u8; 8];
    f.read_exact(&mut magic)?;
    assert_eq!(&magic, FREE_MAGIC, "bad FREE magic");
    let mut hdr = vec![0u8; FREE_HDR_SIZE - 8];
    f.read_exact(&mut hdr)?;
    // count в header можно игнорировать — читаем до конца файла
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

// Посчитать «живые» overflow-страницы (не лежащие в free-list) и суммарные данные в них.
fn count_live_overflow(root: &PathBuf) -> Result<(u64, u64)> {
    let pager = Pager::open(root)?;
    let ps = pager.meta.page_size as usize;
    let free = read_free_set(root)?;
    let mut ovf_pages = 0u64;
    let mut ovf_bytes = 0u64;

    for pid in 0..pager.meta.next_page_id {
        if free.contains(&pid) {
            continue; // страница уже в free-list — не считаем как «живую»
        }
        let mut buf = vec![0u8; ps];
        if pager.read_page(pid, &mut buf).is_ok() {
            if let Ok(h) = ovf_header_read(&buf) {
                ovf_pages += 1;
                ovf_bytes += h.chunk_len as u64;
            }
        }
    }
    Ok((ovf_pages, ovf_bytes))
}

#[test]
fn overflow_roundtrip_various_sizes() -> Result<()> {
    // Делаем тайминги детерминированнее
    std::env::set_var("P1_WAL_COALESCE_MS", "0");

    let root = unique_root("sizes");
    fs::create_dir_all(&root)?;
    init_db(&root, 4096)?;
    Directory::create(&root, 64)?;

    let meta = read_meta(&root)?;
    let ps = meta.page_size as usize;
    let cap = ovf_chunk_capacity(ps);
    let threshold = ps / 4;

    // Набор размеров вокруг порога и емкости страницы
    let sizes = vec![
        threshold.saturating_sub(1),
        threshold,
        threshold + 1,
        cap.saturating_sub(1),
        cap,
        cap + 1,
        2 * cap - 3,
        2 * cap,
        2 * cap + 5,
        5 * cap + 17,
    ];

    for (i, sz) in sizes.iter().enumerate() {
        let key = format!("k{:03}", i);
        let mut val = vec![0u8; *sz];
        // паттерн
        for j in 0..val.len() {
            val[j] = (i as u8).wrapping_add((j as u8).wrapping_mul(31));
        }

        {
            let mut db = Db::open(&root)?;
            db.put(key.as_bytes(), &val)?;
            // get -> must equal
            let got = db.get(key.as_bytes())?.expect("value must exist");
            assert_eq!(got, val, "roundtrip mismatch for size {}", sz);
        }

        // Проверим количество живых overflow-страниц после вставки
        let (ovf_pages, _ovf_bytes) = count_live_overflow(&root)?;
        let rec_sz_inline = 4 + key.len() + val.len();
        let rec_fits_inline = rec_sz_inline
            + QuiverDB::page_rh::RH_SLOT_SIZE
            + QuiverDB::consts::PAGE_HDR_V2_SIZE
            <= ps;
        let need_overflow = val.len() > threshold || !rec_fits_inline;
        let expected_pages = if need_overflow {
            ((val.len() + cap - 1) / cap) as u64
        } else {
            0
        };
        assert_eq!(
            ovf_pages, expected_pages,
            "overflow pages after put (size={})",
            sz
        );

        // Обновим значение (другой размер), проверим, что старые страницы освобождены
        let new_sz = val.len() + cap + 7; // гарантированно другой chain length
        let mut val2 = vec![0u8; new_sz];
        for j in 0..val2.len() {
            val2[j] = (i as u8).wrapping_add(0x55).wrapping_add((j as u8).wrapping_mul(17));
        }
        {
            let mut db = Db::open(&root)?;
            db.put(key.as_bytes(), &val2)?;
            let got = db.get(key.as_bytes())?.expect("value must exist");
            assert_eq!(got, val2, "roundtrip mismatch after update (size={})", new_sz);
        }

        let (ovf_pages2, _ovf_bytes2) = count_live_overflow(&root)?;
        let rec_sz_inline2 = 4 + key.len() + val2.len();
        let rec_fits_inline2 = rec_sz_inline2
            + QuiverDB::page_rh::RH_SLOT_SIZE
            + QuiverDB::consts::PAGE_HDR_V2_SIZE
            <= ps;
        let need_overflow2 = val2.len() > threshold || !rec_fits_inline2;
        let expected_pages2 = if need_overflow2 {
            ((val2.len() + cap - 1) / cap) as u64
        } else {
            0
        };
        assert_eq!(
            ovf_pages2, expected_pages2,
            "overflow pages after update (size={})",
            new_sz
        );

        // Удаление — overflow-страницы должны исчезнуть
        {
            let mut db = Db::open(&root)?;
            assert!(db.del(key.as_bytes())?, "key must be deleted");
            let miss = db.get(key.as_bytes())?;
            assert!(miss.is_none(), "key must be absent after delete");
        }

        let (ovf_pages3, _ovf_bytes3) = count_live_overflow(&root)?;
        assert_eq!(ovf_pages3, 0, "overflow pages must be gone after delete");
    }

    Ok(())
}

// Нагрузочный тест (по умолчанию отключен).
// Запуск: cargo test --test overflow_roundtrip_v0_6 -- --ignored --nocapture
#[test]
fn heavy_churn_many_updates() -> Result<()> {
    std::env::set_var("P1_WAL_COALESCE_MS", "0");

    let root = unique_root("heavy");
    fs::create_dir_all(&root)?;
    init_db(&root, 4096)?;
    Directory::create(&root, 128)?;

    let ps = read_meta(&root)?.page_size as usize;
    let cap = ovf_chunk_capacity(ps);
    let threshold = ps / 4;

    // 200 итераций обновлений одного ключа с рандомным размером вокруг капов.
    let mut rng = oorandom::Rand64::new(0xDEADBEEF);
    let key = b"hot";
    for i in 0..200 {
        let mut len = (rng.rand_u64() % (5 * cap as u64 + 4096)) as usize;
        // слегка подталкиваем к перепадам вокруг порогов
        if i % 7 == 0 {
            len = threshold.saturating_add((rng.rand_u64() % 1024) as usize);
        }
        let mut val = vec![0u8; len];
        for j in 0..val.len() {
            val[j] = (j as u8).wrapping_mul(13).wrapping_add((i as u8).wrapping_mul(7));
        }
        let mut db = Db::open(&root)?;
        db.put(key, &val)?;
        let got = db.get(key)?.expect("must exist");
        assert_eq!(got, val);
    }

    // После нагрузки просто проверим, что всё читается и БД не сломалась
    let db = Db::open(&root)?;
    let _ = db.get(b"hot")?;
    Ok(())
}