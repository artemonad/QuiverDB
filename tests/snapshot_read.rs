// tests/snapshot_read.rs
//
// Запуск только этого файла:
//   cargo test --test snapshot_read -- --nocapture
//
// Покрываем:
// 1) SnapshotHandle::get: возвращает значения "как на момент снапшота",
//    даже если после снапшота writer обновил ключи.
// 2) SnapshotHandle::scan_prefix: отдает только те ключи и значения,
//    которые были видимы на момент снапшота.
// 3) Overflow-значение: под снапшотом читается старая цепочка (до обновления).

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
    let base = std::env::temp_dir();
    base.join(format!("qdbtest-snap-{prefix}-{pid}-{t}-{id}"))
}

#[test]
fn snapshot_get_and_scan_basic_and_overflow() -> Result<()> {
    // Делаем LSN-дребезг детерминированным для теста
    std::env::set_var("P1_WAL_COALESCE_MS", "0");

    let root = unique_root("basic");
    fs::create_dir_all(&root)?;
    init_db(&root, 4096)?;
    Directory::create(&root, 64)?;

    // 1) Подготовим данные: обычный ключ и overflow-значение
    let (old_v_small, new_v_small) = (b"old", b"new");
    let ps = read_meta(&root)?.page_size as usize;
    let threshold = ps / 4;
    let old_big = vec![0xA5u8; threshold + 300];
    let new_big = vec![0x5Au8; threshold + 500];

    {
        let mut db = Db::open(&root)?;
        db.put(b"k1", old_v_small)?;
        db.put(b"big", &old_big)?;
    }

    // 2) Начинаем снапшот (фиксируем lsn)
    let mut db = Db::open(&root)?;
    let mut snap = db.snapshot_begin()?;
    let _snap_lsn = snap.lsn();

    // 3) Параллельно writer меняет значения и добавляет новый ключ
    db.put(b"k1", new_v_small)?;
    db.put(b"big", &new_big)?;      // обновление overflow — старая цепочка должна остаться доступной под снапшотом
    db.put(b"k2", b"later")?;       // ключ появился после снапшота — под снапшотом не должен быть виден

    // 4) Проверяем чтение под снапшотом
    // k1 должен быть старым
    let got_k1_snap = snap.get(b"k1")?.expect("k1 must exist under snapshot");
    assert_eq!(got_k1_snap.as_slice(), old_v_small, "k1 must be old under snapshot");

    // overflow "big" должен быть старым по содержимому
    let got_big_snap = snap.get(b"big")?.expect("big must exist under snapshot");
    assert_eq!(got_big_snap.len(), old_big.len());
    assert_eq!(got_big_snap, old_big, "overflow value under snapshot must be old");

    // k2 не должен быть виден под снапшотом
    let got_k2_snap = snap.get(b"k2")?;
    assert!(got_k2_snap.is_none(), "k2 must be absent under snapshot");

    // 5) Проверяем scan_prefix под снапшотом
    let pairs_k_snap = snap.scan_prefix(b"k")?;
    let mut map_k = std::collections::HashMap::new();
    for (k, v) in pairs_k_snap {
        map_k.insert(k, v);
    }
    // Должен быть только k1 (старое значение), k2 не было на момент снапшота
    assert_eq!(map_k.len(), 1, "only k1 should be visible under snapshot");
    assert_eq!(
        map_k.get(b"k1".as_ref()).map(|v| v.as_slice()),
        Some(old_v_small.as_ref())
    );

    // 6) Актуальное состояние в live-db: уже новые значения
    let got_k1_live = db.get(b"k1")?.expect("k1 must exist live");
    assert_eq!(got_k1_live.as_slice(), new_v_small, "k1 must be new in live db");

    let got_big_live = db.get(b"big")?.expect("big must exist live");
    assert_eq!(got_big_live, new_big, "big must be new in live db");

    let got_k2_live = db.get(b"k2")?.expect("k2 must exist live");
    assert_eq!(got_k2_live.as_slice(), b"later");

    // 7) Завершаем снапшот
    db.snapshot_end(&mut snap)?;

    Ok(())
}