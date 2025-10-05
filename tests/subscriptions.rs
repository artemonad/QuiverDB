use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use QuiverDB::{init_db, Db, Directory};
use QuiverDB::subs::Event;

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let base = std::env::temp_dir();
    base.join(format!("qdbtest-subs-{prefix}-{pid}-{t}-{id}"))
}

#[test]
fn subscriptions_put_and_del_with_prefix_and_order() -> Result<()> {
    let root = unique_root("basic");
    fs::create_dir_all(&root)?;
    init_db(&root, 4096)?;
    Directory::create(&root, 64)?;

    let mut db = Db::open(&root)?;

    // Коллектор событий
    let events: Arc<Mutex<Vec<Event>>> = Arc::new(Mutex::new(Vec::new()));
    let ev_clone = events.clone();

    // Подписка на префикс "k"
    let _h = db.subscribe_prefix(b"k".to_vec(), move |ev: &Event| {
        ev_clone.lock().unwrap().push(ev.clone());
    });

    // Пишем k1, потом удаляем k1; пишем x1 — не должен попасть в подписку
    db.put(b"k1", b"v1")?;
    db.del(b"k1")?;
    db.put(b"x1", b"z")?;

    let got = events.lock().unwrap().clone();
    assert_eq!(got.len(), 2, "expected 2 events for prefix 'k'");

    // 1) put(k1) -> Some("v1")
    assert_eq!(got[0].key, b"k1");
    assert_eq!(got[0].value.as_deref(), Some(b"v1".as_ref()));
    // 2) del(k1) -> None
    assert_eq!(got[1].key, b"k1");
    assert!(got[1].value.is_none());

    // LSN должен возрастать
    assert!(got[1].lsn > got[0].lsn, "lsn must increase");

    Ok(())
}

#[test]
fn subscriptions_no_event_for_non_matching_prefix() -> Result<()> {
    let root = unique_root("prefix");
    fs::create_dir_all(&root)?;
    init_db(&root, 4096)?;
    Directory::create(&root, 64)?;

    let mut db = Db::open(&root)?;
    let events: Arc<Mutex<Vec<Event>>> = Arc::new(Mutex::new(Vec::new()));
    let ev_clone = events.clone();

    // Подписка только на "a"
    let _h = db.subscribe_prefix(b"a".to_vec(), move |ev: &Event| {
        ev_clone.lock().unwrap().push(ev.clone());
    });

    db.put(b"b1", b"v")?;  // не должен попасть
    db.put(b"a1", b"v")?;  // попадёт
    db.del(b"b1")?;        // не попадёт

    let got = events.lock().unwrap().clone();
    assert_eq!(got.len(), 1, "only 'a1' should match");
    assert_eq!(got[0].key, b"a1");
    assert_eq!(got[0].value.as_deref(), Some(b"v".as_ref()));

    Ok(())
}