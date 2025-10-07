// Phase 2 tests: SnapStore, hashindex, persisted registry, compact.
//
// Запуск только этого файла:
//   cargo test --test phase2_snapstore -- --nocapture
//
// Примечание: один из тестов помечен #[ignore], т.к. выявляет вероятный баг refcount
// при freeze одинакового содержимого для нескольких снапшотов.

use anyhow::Result;
use byteorder::{ByteOrder, LittleEndian};
use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use QuiverDB::{init_db, Db, Directory, read_meta};
use QuiverDB::snapshots::store::SnapStore;

// ---------- helpers ----------

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("qdbtest-p2-{prefix}-{pid}-{t}-{id}"))
}

fn read_hashes_from_hashindex(dir: &PathBuf) -> Result<HashSet<u64>> {
    let mut set = HashSet::new();
    let path = dir.join("hashindex.bin");
    if !path.exists() {
        return Ok(set);
    }
    let mut f = OpenOptions::new().read(true).open(&path)?;
    let len = f.metadata()?.len();
    const REC: u64 = 8 + 8 + 8; // [page_id u64][hash u64][page_lsn u64]
    let mut pos = 0u64;
    let mut buf = vec![0u8; REC as usize];
    while pos + REC <= len {
        f.seek(SeekFrom::Start(pos))?;
        f.read_exact(&mut buf)?;
        let _pid = LittleEndian::read_u64(&buf[0..8]);
        let hash = LittleEndian::read_u64(&buf[8..16]);
        set.insert(hash);
        pos += REC;
    }
    Ok(set)
}

fn read_registry_json(root: &PathBuf) -> Option<serde_json::Value> {
    let path = root.join(".snapshots").join("registry.json");
    if !path.exists() {
        return None;
    }
    let bytes = fs::read(path).ok()?;
    serde_json::from_slice::<serde_json::Value>(&bytes).ok()
}

// ---------- tests ----------

#[test]
fn phase2_dedup_writes_hashindex_and_snapstore() -> Result<()> {
    std::env::set_var("P1_WAL_COALESCE_MS", "0");
    std::env::set_var("P1_SNAP_DEDUP", "1");
    std::env::set_var("P1_SNAP_PERSIST", "1");

    let root = unique_root("dedup");
    fs::create_dir_all(&root)?;
    init_db(&root, 4096)?;
    Directory::create(&root, 128)?;

    // 1) Базовое значение
    {
        let mut db = Db::open(&root)?;
        db.put(b"k", b"v0")?;
    }

    // 2) Начинаем snapshot, обновляем ключ -> freeze старого образа
    let mut db = Db::open(&root)?;
    let mut s = db.snapshot_begin()?;
    db.put(b"k", b"v1")?;

    // 3) Проверим hashindex и snapstore
    let sdir = root.join(".snapshots").join(&s.id);
    assert!(sdir.exists(), "snapshot sidecar must exist");

    let hashes = read_hashes_from_hashindex(&sdir)?;
    assert!(!hashes.is_empty(), "hashindex.bin must contain entries");

    let ps = read_meta(&root)?.page_size;
    let ss = SnapStore::open(&root, ps)?;
    // Должен присутствовать хотя бы один кадр
    let mut found = false;
    for h in hashes.iter() {
        if ss.contains(*h) {
            found = true;
            break;
        }
    }
    assert!(found, "snapstore must contain at least one frame referenced by hashindex");

    // 4) Persisted registry должен содержать запись
    let reg = read_registry_json(&root).expect("registry.json must exist when P1_SNAP_PERSIST=1");
    let arr = reg.get("entries").and_then(|x| x.as_array()).unwrap();
    assert!(
        arr.iter().any(|e| e.get("id").and_then(|x| x.as_str()) == Some(s.id.as_str())),
        "registry must include our snapshot id"
    );

    db.snapshot_end(&mut s)?;
    Ok(())
}

#[test]
fn phase2_snapstore_compact_is_idempotent() -> Result<()> {
    std::env::set_var("P1_WAL_COALESCE_MS", "0");
    std::env::set_var("P1_SNAP_DEDUP", "1");
    std::env::set_var("P1_SNAP_PERSIST", "1");

    let root = unique_root("compact");
    fs::create_dir_all(&root)?;
    init_db(&root, 4096)?;
    Directory::create(&root, 64)?;

    {
        let mut db = Db::open(&root)?;
        db.put(b"k", b"v0")?;
    }
    let mut db = Db::open(&root)?;
    let mut s = db.snapshot_begin()?;
    db.put(b"k", b"v1")?;

    // Откроем SnapStore и запустим compact дважды
    let ps = read_meta(&root)?.page_size;
    let mut ss = SnapStore::open(&root, ps)?;
    let rep1 = ss.compact()?;
    let rep2 = ss.compact()?;

    // Идемпотентность: второй прогон не должен уменьшить файл сильнее первого,
    // и размер после второй компакции не больше первого "after".
    assert!(rep2.after <= rep1.after, "second compact must not increase size");
    assert!(rep1.after <= rep1.before, "first compact should not increase size");

    // Завершим срез
    db.snapshot_end(&mut s)?;
    Ok(())
}

#[test]
fn phase2_registry_end_flag_changes_on_end() -> Result<()> {
    std::env::set_var("P1_WAL_COALESCE_MS", "0");
    std::env::set_var("P1_SNAP_DEDUP", "1");
    std::env::set_var("P1_SNAP_PERSIST", "1");

    let root = unique_root("registry");
    fs::create_dir_all(&root)?;
    init_db(&root, 4096)?;
    Directory::create(&root, 32)?;

    {
        let mut db = Db::open(&root)?;
        db.put(b"k", b"v0")?;
    }

    let mut db = Db::open(&root)?;
    let mut s = db.snapshot_begin()?;
    // Обновим, чтобы у снапшота были данные
    db.put(b"k", b"v1")?;

    let reg_before = read_registry_json(&root).expect("registry must exist");
    let entries_before = reg_before.get("entries").and_then(|x| x.as_array()).unwrap();
    let my = entries_before.iter().find(|e| e.get("id").and_then(|x| x.as_str()) == Some(s.id.as_str())).unwrap();
    let ended_before = my.get("ended").and_then(|x| x.as_bool()).unwrap_or(false);
    assert_eq!(ended_before, false, "ended must be false before ending snapshot");

    db.snapshot_end(&mut s)?;

    let reg_after = read_registry_json(&root).expect("registry must exist (after)");
    let entries_after = reg_after.get("entries").and_then(|x| x.as_array()).unwrap();
    let my_after = entries_after.iter().find(|e| e.get("id").and_then(|x| x.as_str()) == Some(s.id.as_str())).unwrap();
    let ended_after = my_after.get("ended").and_then(|x| x.as_bool()).unwrap_or(false);
    assert_eq!(ended_after, true, "ended must be true after snapshot_end");
    Ok(())
}

// ВЫЯВЛЕНИЕ БАГА (скорее всего): refcount в SnapStore при freeze одной и той же страницы для двух снапшотов.
// Ожидаемое поведение: после удаления первого снимка и compact кадр не должен пропасть,
// т.к. второй снимок всё ещё ссылается на него. При некорректном учёте refcount кадр может удалиться.
//
// Этот тест помечён #[ignore], чтобы не ломать сборку, но его полезно периодически включать
// и фиксить поведение SnapStore/SnapshotManager при работе с несколькими снапшотами.
#[test]
fn phase2_refcount_two_snapshots_freeze_same_content_then_remove_one_and_compact() -> Result<()> {
    std::env::set_var("P1_WAL_COALESCE_MS", "0");
    std::env::set_var("P1_SNAP_DEDUP", "1");
    std::env::set_var("P1_SNAP_PERSIST", "1");

    let root = unique_root("refcnt");
    fs::create_dir_all(&root)?;
    init_db(&root, 4096)?;
    Directory::create(&root, 64)?;

    // Базовое значение
    {
        let mut db = Db::open(&root)?;
        db.put(b"k", b"v0")?;
    }

    // Два снапшота
    let mut db = Db::open(&root)?;
    let s1 = db.snapshot_begin()?;
    let mut s2 = db.snapshot_begin()?;

    // Обновляем ключ -> freeze старого образа для обоих
    db.put(b"k", b"v1")?;

    // Удалим sidecar freeze.bin у s2, чтобы заставить fallback читать из SnapStore
    let s2_dir = root.join(".snapshots").join(&s2.id);
    let _ = fs::remove_file(s2_dir.join("freeze.bin"));

    // Прочитаем hashindex у s1 -> список хэшей и уменьшим ссылки (эмулируем SnapRm)
    let s1_dir = root.join(".snapshots").join(&s1.id);
    let hashes = read_hashes_from_hashindex(&s1_dir)?;
    if !hashes.is_empty() {
        let ps = read_meta(&root)?.page_size;
        let mut ss = SnapStore::open(&root, ps)?;
        for h in hashes {
            let _ = ss.dec_ref(h);
        }
    }
    // Удалим sidecar s1
    let _ = fs::remove_dir_all(&s1_dir);

    // Компактим хранилище
    let ps = read_meta(&root)?.page_size;
    let mut ss = SnapStore::open(&root, ps)?;
    let _ = ss.compact()?;

    // Теперь s2 должен уметь прочитать старое значение через fallback из snapstore
    let got = s2.get(b"k")?.expect("k must be readable under s2");
    assert_eq!(got.as_slice(), b"v0", "s2 must see v0 via snapstore fallback");

    // Завершаем s2
    db.snapshot_end(&mut s2)?;
    Ok(())
}