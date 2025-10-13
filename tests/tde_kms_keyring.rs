use anyhow::Result;
use std::fs;
use std::path::PathBuf;

use rand::RngCore;
use QuiverDB::db::Db;
use QuiverDB::crypto::{KeyRing, EnvKmsProvider, KmsProvider, KeyJournal};

fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("qdb2-{}-{}-{}", prefix, pid, t))
}

#[test]
fn tde_kms_keyring_roundtrip() -> Result<()> {
    let root = unique_root("tde-kms");
    fs::create_dir_all(&root)?;

    // 1) init DB
    Db::init(&root, 64 * 1024, 64)?;

    // 2) Подготовим KMS KEK (для dev промазываем фиксированным 0x33)
    std::env::set_var("P1_KMS_KEK_KID", "dev-kek");
    std::env::set_var("P1_KMS_KEK_HEX", "33".repeat(32));
    // Без EnvKeyProvider (fallback) — ухудшает изоляцию
    std::env::remove_var("P1_TDE_KEY_HEX");
    std::env::remove_var("P1_TDE_KEY_BASE64");

    // 3) Сгенерим запросный KID, завернём DEK и положим его в keyring
    let requested_kid = format!("kid-{:016x}", rand::random::<u64>());
    let kms = EnvKmsProvider::from_env()?;
    let mut dek = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut dek);
    let wrapped = kms.wrap(kms.default_kek_kid(), &dek)?;
    let ring = KeyRing::open_or_create(&root)?;
    ring.put(&requested_kid, &wrapped)?;

    // 4) Включим TDE через writer (KMS+KeyRing → ensure_tde_key должен подняться)
    {
        let mut db = Db::open(&root)?;
        db.pager.set_tde_config(true, Some(requested_kid.clone()));
        db.pager.ensure_tde_key()?;

        // Запишем пару ключей (будут с AEAD‑tag)
        db.put(b"alpha", b"1")?;
        db.put(b"beta", b"2")?;

        // Зафиксируем epoch в журнале
        let since_lsn = db.pager.meta.last_lsn.saturating_add(1);
        let j = KeyJournal::open_or_create(&root)?;
        j.add_epoch(since_lsn, &requested_kid)?;
    }

    // 5) Reader должен уметь читать под TDE:
    // Проставим ENV для RO‑открытия (open_ro читает QuiverConfig::from_env() → enable TDE)
    std::env::set_var("P1_TDE_ENABLED", "1");
    std::env::set_var("P1_TDE_KID", &requested_kid);

    let ro = Db::open_ro(&root)?;
    let a = ro.get(b"alpha")?.expect("alpha must exist");
    let b = ro.get(b"beta")?.expect("beta must exist");
    assert_eq!(a.as_slice(), b"1");
    assert_eq!(b.as_slice(), b"2");

    Ok(())
}