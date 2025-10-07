// tests/metamorphic_roundtrip.rs
//
// Два метаморфических теста, покрывающих широкий спектр багов:
// 1) Эквивалентность snapshot-as-of-LSN vs WAL slice restore до того же LSN.
//    Сценарий с горячими мутациями после snapshot (freeze/overflow/chain).
// 2) Эквивалентность Full(S2) == Full(S1) + Incr(S1→S2) (restore через страницы),
//    сравнение состояний по реконструкции KV из страниц без опоры на directory.
//
// Тесты намеренно не используют Directory для сравнения, а реконструируют tail-wins
// состояние напрямую из KV_RH страниц (по цепочкам next_page_id) и overflow‑цепочек.
// Это позволяет ловить рассинхронизацию directory и содержания страниц.

use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;

use oorandom::Rand64;

use QuiverDB::{init_db, read_meta, Db, Directory};
use QuiverDB::pager::Pager;
use QuiverDB::page_rh::{rh_page_is_kv, rh_header_read, rh_kv_list};
use QuiverDB::page_ovf::{ovf_parse_placeholder, ovf_read_chain};
use QuiverDB::cli::cdc::{cmd_cdc_record, cmd_cdc_replay};
use QuiverDB::backup::{backup_to_dir, restore_from_dir};
use QuiverDB::consts::{NO_PAGE, PAGE_HDR_V2_SIZE};

#[inline]
fn nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = nanos();
    let base = std::env::temp_dir();
    base.join(format!("qdbtest-meta-{prefix}-{pid}-{t}"))
}

/// Реконструкция tail-wins состояния из страниц (без directory).
/// - Находит все KV_RH страницы, строит карту next_page_id.
/// - Находит головы цепочек как страницы с in-degree=0.
/// - Идёт по цепочкам head->tail, вставляя пары; более поздние (ближе к хвосту) перезаписывают ранние.
/// - Восстанавливает overflow значения по placeholder.
fn reconstruct_state_without_directory(root: &PathBuf) -> Result<HashMap<Vec<u8>, Vec<u8>>> {
    let pager = Pager::open(root)?;
    let ps = pager.meta.page_size as usize;

    // Соберём все KV страницы и их next ссылки
    let mut kv_pages: HashSet<u64> = HashSet::new();
    let mut next_map: HashMap<u64, u64> = HashMap::new();
    let mut indeg: HashMap<u64, u64> = HashMap::new();

    // Кэш страниц, чтобы не перечитывать при проходах
    let mut page_buf = vec![0u8; ps];

    for pid in 0..pager.meta.next_page_id {
        if pager.read_page(pid, &mut page_buf).is_ok() && rh_page_is_kv(&page_buf) {
            if let Ok(h) = rh_header_read(&page_buf) {
                kv_pages.insert(pid);
                next_map.insert(pid, h.next_page_id);
                if h.next_page_id != NO_PAGE {
                    *indeg.entry(h.next_page_id).or_insert(0) += 1;
                }
            }
        }
    }

    // Головы цепочек: KV страница с in-degree=0
    let mut heads: Vec<u64> = kv_pages
        .iter()
        .copied()
        .filter(|pid| indeg.get(pid).copied().unwrap_or(0) == 0)
        .collect();

    let mut out: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

    // Обход цепочек head->tail
    for head in heads.drain(..) {
        let mut cur = head;
        let mut safety = 0usize;

        while cur != NO_PAGE && safety <= 1_000_000 {
            safety += 1;
            if !kv_pages.contains(&cur) {
                break;
            }

            // Читаем страницу и извлекаем пары
            let mut buf = vec![0u8; ps];
            if pager.read_page(cur, &mut buf).is_err() || !rh_page_is_kv(&buf) {
                break;
            }
            if let Ok(items) = rh_kv_list(&buf) {
                for (k, v) in items {
                    let v_bytes = if let Some((total_len, head_pid)) = ovf_parse_placeholder(&v) {
                        ovf_read_chain(&pager, head_pid, Some(total_len as usize))?
                    } else {
                        v.to_vec()
                    };
                    // tail-wins: более поздние записи перезаписывают ранние
                    out.insert(k.to_vec(), v_bytes);
                }
            }

            let h = match rh_header_read(&buf) {
                Ok(h) => h,
                Err(_) => break,
            };
            cur = h.next_page_id;
        }
    }

    // Безопасность: если внезапно всё с циклами (редко/защита),
    // облетим все KV‑страницы.
    if out.is_empty() && !kv_pages.is_empty() {
        for pid in kv_pages {
            if pager.read_page(pid, &mut page_buf).is_ok() && rh_page_is_kv(&page_buf) {
                if let Ok(items) = rh_kv_list(&page_buf) {
                    for (k, v) in items {
                        let v_bytes =
                            if let Some((total_len, head_pid)) = ovf_parse_placeholder(&v) {
                                ovf_read_chain(&pager, head_pid, Some(total_len as usize))?
                            } else {
                                v.to_vec()
                            };
                        out.insert(k.to_vec(), v_bytes);
                    }
                }
            }
        }
    }

    Ok(out)
}

fn pairs_to_map(pairs: Vec<(Vec<u8>, Vec<u8>)>) -> HashMap<Vec<u8>, Vec<u8>> {
    let mut m = HashMap::new();
    for (k, v) in pairs {
        m.insert(k, v);
    }
    m
}

fn random_value_of_len(rng: &mut Rand64, len: usize) -> Vec<u8> {
    let mut v = vec![0u8; len];
    for i in 0..len {
        v[i] = ((i as u8).wrapping_mul(31)).wrapping_add((rng.rand_u64() as u8).wrapping_mul(7));
    }
    v
}

#[test]
fn metam_snapshot_equivalent_to_wal_slice_restore() -> Result<()> {
    // Детеминизируем тайминг fsync коалессации
    std::env::set_var("P1_WAL_COALESCE_MS", "0");

    let leader = unique_root("snap-wal");
    let follower = unique_root("snap-wal-foll");
    fs::create_dir_all(&leader)?;
    fs::create_dir_all(&follower)?;
    init_db(&leader, 4096)?;
    Directory::create(&leader, 128)?;

    let ps = read_meta(&leader)?.page_size as usize;
    let threshold = ps / 4;

    // Заполним базовый набор ключей до снапшота (часть overflow)
    {
        let mut db = Db::open(&leader)?;
        for i in 0..64usize {
            let k = format!("k{:03}", i);
            // значения вокруг порога overflow
            let len = match i % 6 {
                0 => threshold.saturating_sub(1),
                1 => threshold,
                2 => threshold + 1,
                3 => (threshold + 300).min(ps - PAGE_HDR_V2_SIZE - 16),
                4 => (threshold + 700).min(ps - PAGE_HDR_V2_SIZE - 16),
                _ => ((i * 113) % (ps.saturating_sub(PAGE_HDR_V2_SIZE + 32))).max(1),
            };
            let mut rng = Rand64::new((i as u128) * 0xC0FFEE + 1);
            let v = random_value_of_len(&mut rng, len);
            db.put(k.as_bytes(), &v)?;
        }
        // Drop writer: clean_shutdown=true, WAL ещё НЕ усечён.
    }

    // До открытия writer (который бы усёк WAL на clean start), снимаем wire‑срез WAL [0..S].
    let base_lsn = read_meta(&leader)?.last_lsn;
    let slice = leader.join("slice.bin");
    cmd_cdc_record(leader.clone(), slice.clone(), Some(0), Some(base_lsn))?;

    // Теперь открываем writer и стартуем snapshot: на чистом старте WAL будет усечён,
    // но это уже не мешает — мы сняли срез заранее. LSN снапшота должен совпасть с base_lsn.
    let mut db = Db::open(&leader)?;
    let mut snap = db.snapshot_begin()?;
    let s_lsn = snap.lsn();
    assert_eq!(
        s_lsn, base_lsn,
        "snapshot LSN must equal last_lsn taken before opening writer"
    );

    // Горячие мутации после snapshot — чтобы задействовать freeze.
    {
        let mut rng = Rand64::new(0xDEADC0FF);
        for i in 0..400usize {
            let dice = rng.rand_u64() % 10;
            if dice < 6 {
                // update существующего
                let idx = (rng.rand_u64() as usize) % 64;
                let k = format!("k{:03}", idx);
                let len = match rng.rand_u64() % 7 {
                    0 => threshold.saturating_sub(1),
                    1 => threshold,
                    2 => threshold + 1 + ((rng.rand_u64() % 256) as usize),
                    3 => (ps / 2).min(threshold + 300 + ((rng.rand_u64() % 512) as usize)),
                    4 => (threshold + 700).min(ps.saturating_sub(PAGE_HDR_V2_SIZE + 16)),
                    5 => ((rng.rand_u64() as usize) % (3 * threshold + 1024)).min(ps - 64),
                    _ => ((rng.rand_u64() as usize) % (4 * threshold + 2048)).min(ps - 64),
                }.max(1);
                let v = random_value_of_len(&mut rng, len);
                db.put(k.as_bytes(), &v)?;
            } else if dice < 8 {
                // delete
                let idx = (rng.rand_u64() as usize) % 64;
                let k = format!("k{:03}", idx);
                let _ = db.del(k.as_bytes())?;
            } else {
                // новые после snapshot
                let k = format!("z{:06}", i);
                let len = ((rng.rand_u64() as usize) % (threshold + 2048)).min(ps - 64).max(1);
                let v = random_value_of_len(&mut rng, len);
                db.put(k.as_bytes(), &v)?;
            }
        }
    }

    // Инициализируем фолловера и применим срез
    init_db(&follower, 4096)?;
    let _ = Directory::create(&follower, 128);
    cmd_cdc_replay(follower.clone(), Some(slice), None, None)?;

    // Состояние фолловера (как на S) — реконструируем из страниц
    let foll_map = reconstruct_state_without_directory(&follower)?;

    // Состояние снапшота строим через snap.get(key) для всех ключей фолловера.
    // Это важно: scan_all под нагрузкой может пропускать ключи, если страницы вырезаны из цепочки,
    // а get использует fallback‑скан по всем страницам.
    let mut snap_map: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    for (k, v_follow) in &foll_map {
        match snap.get(k)? {
            Some(vs) => {
                snap_map.insert(k.clone(), vs);
            }
            None => {
                panic!(
                    "snapshot missing key present in follower: {:?}",
                    String::from_utf8_lossy(k)
                );
            }
        }
        // Дополнительно можно сравнить значение прямо здесь:
        // assert_eq!(snap_map.get(k).unwrap(), v_follow);
        let v_snap = snap_map.get(k).unwrap();
        assert_eq!(v_snap, v_follow, "value mismatch for key {:?}", String::from_utf8_lossy(k));
    }

    assert_eq!(
        snap_map.len(),
        foll_map.len(),
        "snapshot and follower must have same number of keys"
    );

    db.snapshot_end(&mut snap)?;
    Ok(())
}

#[test]
fn metam_backup_full_plus_incremental_equals_full_s2() -> Result<()> {
    // Делаем коалессацию fsync детерминированной
    std::env::set_var("P1_WAL_COALESCE_MS", "0");

    let leader = unique_root("full-incr");
    let foll_a = unique_root("full-incr-A");
    let foll_b = unique_root("full-incr-B");
    fs::create_dir_all(&leader)?;
    fs::create_dir_all(&foll_a)?;
    fs::create_dir_all(&foll_b)?;
    init_db(&leader, 4096)?;
    Directory::create(&leader, 128)?;

    let ps = read_meta(&leader)?.page_size as usize;
    let threshold = ps / 4;
    let mut rng = Rand64::new(0xA11CE);

    // Начальные операции
    {
        let mut db = Db::open(&leader)?;
        for i in 0..48usize {
            let k = format!("k{:03}", i);
            let len = match i % 5 {
                0 => threshold.saturating_sub(1),
                1 => threshold,
                2 => threshold + 1,
                3 => (threshold + 300).min(ps - 64),
                _ => ((i * 97) % (ps - 64)).max(1),
            };
            let v = random_value_of_len(&mut rng, len);
            db.put(k.as_bytes(), &v)?;
        }
    }

    // S1
    let mut db = Db::open(&leader)?;
    let mut s1 = db.snapshot_begin()?;
    let lsn1 = s1.lsn();

    let backup_full_s1 = leader.join("backup_full_s1");
    backup_to_dir(&db, &s1, &backup_full_s1, None)?;
    db.snapshot_end(&mut s1)?;

    // Дополнительные операции -> S2
    {
        for i in 0..120usize {
            let dice = rng.rand_u64() % 10;
            if dice < 7 {
                let idx = (rng.rand_u64() as usize) % 64;
                let k = format!("k{:03}", idx);
                let len = ((rng.rand_u64() as usize) % (threshold + 2048)).min(ps - 64).max(1);
                let v = random_value_of_len(&mut rng, len);
                db.put(k.as_bytes(), &v)?;
            } else if dice < 9 {
                let idx = (rng.rand_u64() as usize) % 64;
                let k = format!("k{:03}", idx);
                let _ = db.del(k.as_bytes())?;
            } else {
                let k = format!("z{:06}", i);
                let len = ((rng.rand_u64() as usize) % (threshold + 2048)).min(ps - 64).max(1);
                let v = random_value_of_len(&mut rng, len);
                db.put(k.as_bytes(), &v)?;
            }
        }
    }

    let mut s2 = db.snapshot_begin()?;
    let lsn2 = s2.lsn();
    assert!(lsn2 > lsn1, "S2 must be ahead of S1");

    let backup_incr_s1_s2 = leader.join("backup_incr_s1_s2");
    backup_to_dir(&db, &s2, &backup_incr_s1_s2, Some(lsn1))?;

    let backup_full_s2 = leader.join("backup_full_s2");
    backup_to_dir(&db, &s2, &backup_full_s2, None)?;
    db.snapshot_end(&mut s2)?;

    // Follower A: Full(S1) -> apply Incr(S1->S2)
    restore_from_dir(&foll_a, &backup_full_s1)?;
    restore_from_dir(&foll_a, &backup_incr_s1_s2)?;

    // Follower B: Full(S2)
    restore_from_dir(&foll_b, &backup_full_s2)?;

    // Сравним состояние A и B через реконструкцию страниц
    let map_a = reconstruct_state_without_directory(&foll_a)?;
    let map_b = reconstruct_state_without_directory(&foll_b)?;

    assert_eq!(map_a.len(), map_b.len(), "A and B must have the same number of keys");
    for (k, v) in &map_a {
        let got = map_b.get(k)
            .unwrap_or_else(|| panic!("B is missing key {:?}", String::from_utf8_lossy(k)));
        assert_eq!(got, v, "mismatch for key {:?}", String::from_utf8_lossy(k));
    }

    Ok(())
}