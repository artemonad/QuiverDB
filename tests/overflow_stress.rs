use std::collections::{HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use byteorder::{ByteOrder, LittleEndian};
use oorandom::Rand64;

use QuiverDB::{init_db, Db, Directory, read_meta};
use QuiverDB::pager::Pager;
use QuiverDB::page_ovf::{ovf_chunk_capacity, ovf_header_read};
use QuiverDB::page_rh::RH_SLOT_SIZE;
use QuiverDB::consts::{PAGE_HDR_V2_SIZE, FREE_FILE, FREE_HDR_SIZE, FREE_MAGIC};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let base = std::env::temp_dir();
    base.join(format!("qdbtest-ovf-stress-{prefix}-{pid}-{t}-{id}"))
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
            continue;
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

// Подсчёт ожидаемого числа overflow-страниц для текущей карты значений.
fn expected_overflow_pages(ps: usize, cap: usize, threshold: usize, entries: &HashMap<String, Vec<u8>>) -> u64 {
    let mut total = 0u64;
    for (k, v) in entries {
        let rec_inline = 4 + k.as_bytes().len() + v.len();
        let fits_inline = rec_inline + RH_SLOT_SIZE + PAGE_HDR_V2_SIZE <= ps;
        let need_overflow = v.len() > threshold || !fits_inline;
        if need_overflow {
            let n = (v.len() + cap - 1) / cap;
            total += n as u64;
        }
    }
    total
}

#[test]
fn overflow_stress_update_delete() -> Result<()> {
    // Делаем тайминги детерминированнее
    std::env::set_var("P1_WAL_COALESCE_MS", "0");

    let root = unique_root("stress");
    fs::create_dir_all(&root)?;
    init_db(&root, 4096)?;
    Directory::create(&root, 128)?;

    let meta = read_meta(&root)?;
    let ps = meta.page_size as usize;
    let cap = ovf_chunk_capacity(ps);
    let threshold = ps / 4;

    // Пул ключей
    let key_count = 64usize;
    let keys: Vec<String> = (0..key_count).map(|i| format!("k{:03}", i)).collect();

    // Эталонная карта состояний (то, что должно храниться в БД)
    let mut model: HashMap<String, Vec<u8>> = HashMap::new();

    // RNG
    let mut rng = Rand64::new(0xC0FFEE_u128);

    // 1200 операций с проверками каждые 100
    let total_ops = 1200usize;
    let checkpoint = 100usize;

    for i in 0..total_ops {
        // Откроем БД на короткое время (single-writer)
        {
            let mut db = Db::open(&root)?;

            // Выберем случайный ключ
            let idx = (rng.rand_u64() as usize) % key_count;
            let key = &keys[idx];

            // Выберем операцию (70% put/update, 30% delete)
            let op = rng.rand_u64() % 10;
            if op < 7 {
                // Размеры вокруг порогов и капы overflow
                let choice = rng.rand_u64() % 7;
                let len = match choice {
                    0 => threshold.saturating_sub(1),
                    1 => threshold,
                    2 => threshold + 1,
                    3 => cap.saturating_sub(1),
                    4 => cap,
                    5 => cap + 1 + ((rng.rand_u64() % 256) as usize),
                    _ => {
                        let m = (rng.rand_u64() % (5 * cap as u64 + 2048)) as usize;
                        m
                    }
                };

                // Сгенерируем значение
                let mut val = vec![0u8; len];
                for j in 0..val.len() {
                    val[j] = ((i as u8).wrapping_mul(29)).wrapping_add((j as u8).wrapping_mul(31));
                }

                // PUT/UPDATE
                db.put(key.as_bytes(), &val)?;
                model.insert(key.clone(), val);
            } else {
                // DELETE
                let existed = db.del(key.as_bytes())?;
                if existed {
                    model.remove(key);
                }
            }
        }

        // Каждые checkpoint операций — проверка целостности и отсутствие утечек
        if (i + 1) % checkpoint == 0 {
            // Закрываем writer (уже вышли из scope), считаем live overflow
            let (live_pages, _live_bytes) = count_live_overflow(&root)?;

            // Посчитаем ожидаемые overflow страницы
            let expected_pages = expected_overflow_pages(ps, cap, threshold, &model);

            assert_eq!(
                live_pages, expected_pages,
                "leak detected after {} ops: live={}, expected={}",
                i + 1, live_pages, expected_pages
            );

            // Дополнительно проверим чтение всех ключей из модели
            {
                let db = Db::open(&root)?;
                for (k, v) in &model {
                    let got = db.get(k.as_bytes())?.expect("key must exist");
                    assert_eq!(
                        &got, v,
                        "value mismatch for key '{}' at checkpoint {}",
                        k, i + 1
                    );
                }
            }
        }
    }

    Ok(())
}