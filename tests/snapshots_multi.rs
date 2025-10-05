// tests/snapshots_multi.rs
//
// Запуск:
//   cargo test --test snapshots_multi -- --nocapture
//
// Сценарий:
// 1) Пишем исходные значения (k, big_ovf).
// 2) Снимаем S1.
// 3) Обновляем оба значения -> v1 / big1.
// 4) Снимаем S2.
// 5) Ещё раз обновляем -> v2 / big2.
// Проверяем:
// - S1 видит исходные значения,
// - S2 видит значения после 1‑го обновления,
// - live видит финальные значения.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;

use QuiverDB::{init_db, read_meta, Db, Directory};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);
fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("qdbtest-snap-multi-{prefix}-{pid}-{t}-{id}"))
}

#[test]
fn snapshots_with_different_lsn_see_their_versions() -> Result<()> {
    // Коалессацию fsync выключим для детерминизма LSN
    std::env::set_var("P1_WAL_COALESCE_MS", "0");

    let root = unique_root("multi");
    fs::create_dir_all(&root)?;
    init_db(&root, 4096)?;
    Directory::create(&root, 128)?;

    let ps = read_meta(&root)?.page_size as usize;
    let threshold = ps / 4;

    // Исходные значения
    let k = b"k";
    let v0 = b"v0";
    let big0 = vec![0x11u8; threshold + 300];

    // Запись исходных значений
    {
        let mut db = Db::open(&root)?;
        db.put(k, v0)?;
        db.put(b"big", &big0)?;
    }

    // Writer, под которым будем делать все снапшоты/обновления
    let mut db = Db::open(&root)?;
    // S1
    let mut s1 = db.snapshot_begin()?;

    // 1-е обновление
    let v1 = b"v1";
    let big1 = vec![0x22u8; threshold + 450];
    db.put(k, v1)?;
    db.put(b"big", &big1)?;

    // S2
    let mut s2 = db.snapshot_begin()?;

    // 2-е обновление
    let v2 = b"v2";
    let big2 = vec![0x33u8; threshold + 600];
    db.put(k, v2)?;
    db.put(b"big", &big2)?;

    // Проверки S1 (должен видеть исходные значения)
    let got_s1_k = s1.get(k)?.expect("k must exist under s1");
    assert_eq!(got_s1_k.as_slice(), v0, "S1 must see v0 for k");
    let got_s1_big = s1.get(b"big")?.expect("big must exist under s1");
    assert_eq!(got_s1_big, big0, "S1 must see big0");

    // Проверки S2 (должен видеть значения после первого обновления)
    let got_s2_k = s2.get(k)?.expect("k must exist under s2");
    assert_eq!(got_s2_k.as_slice(), v1, "S2 must see v1 for k");
    let got_s2_big = s2.get(b"big")?.expect("big must exist under s2");
    assert_eq!(got_s2_big, big1, "S2 must see big1");

    // Live (writer) должен видеть второе обновление
    let got_live_k = db.get(k)?.expect("k must exist live");
    assert_eq!(got_live_k.as_slice(), v2, "live must see v2 for k");
    let got_live_big = db.get(b"big")?.expect("big must exist live");
    assert_eq!(got_live_big, big2, "live must see big2");

    // Завершаем снапшоты
    db.snapshot_end(&mut s2)?;
    db.snapshot_end(&mut s1)?;
    Ok(())
}