use anyhow::Result;
use std::fs;
use std::path::PathBuf;

use QuiverDB::snapstore::SnapStore;

/// Уникальный корневой путь для теста.
fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("qdb2-{}-{}-{}", prefix, pid, t))
}

#[test]
fn snapstore_basic_put_get_refcount() -> Result<()> {
    let root = unique_root("snapstore");
    fs::create_dir_all(&root)?;

    // Открыть/создать SnapStore
    let ss = SnapStore::open_or_create(&root)?;
    let dir = ss.dir_path();
    assert!(dir.exists(), ".snapstore directory must exist");

    // 1) Положим объект bytes1
    let bytes1 = vec![0xAA; 1024];
    let (h1, existed1, rc1) = ss.put(&bytes1)?;
    assert_eq!(h1.len(), 64, "sha256 hex must be 64 chars");
    assert!(!existed1, "first put must report existed=false");
    assert_eq!(rc1, 1, "refcount after first put must be 1");
    assert!(ss.has(&h1), "object must exist after put");
    assert_eq!(ss.size(&h1)?, Some(bytes1.len() as u64), "size must match");
    let got1 = ss.get(&h1)?.expect("object must be readable");
    assert_eq!(got1, bytes1, "content mismatch");

    // Проверка layout: objects/<hh>/<rest>
    let obj_path = dir.join("objects").join(&h1[0..2]).join(&h1[2..]);
    assert!(
        obj_path.exists(),
        "object layout path must exist: {}",
        obj_path.display()
    );

    // 2) Повторный put того же содержимого → existed=true, refcount инкремент
    let (h1b, existed1b, rc2) = ss.put(&bytes1)?;
    assert_eq!(h1b, h1, "hash must be stable");
    assert!(existed1b, "second put must report existed=true");
    assert_eq!(rc2, 2, "refcount after second put must be 2");

    // 3) add_ref → 3, затем dec_ref → 2 → 1 → 0 и объект удаляется
    let rc3 = ss.add_ref(&h1)?;
    assert_eq!(rc3, 3, "add_ref must increment to 3");

    let rc2b = ss.dec_ref(&h1)?;
    assert_eq!(rc2b, 2, "dec_ref must decrement to 2");
    let rc1b = ss.dec_ref(&h1)?;
    assert_eq!(rc1b, 1, "dec_ref must decrement to 1");
    let rc0 = ss.dec_ref(&h1)?;
    assert_eq!(rc0, 0, "dec_ref must reach 0");

    // При нулевом refcount объект и .ref должны быть удалены
    assert!(!ss.has(&h1), "object must be deleted at refcount=0");
    let ref_path = dir.join("refs").join(format!("{}.ref", h1));
    assert!(
        !ref_path.exists(),
        "ref file must be deleted at refcount=0: {}",
        ref_path.display()
    );
    assert!(
        !obj_path.exists(),
        "object file must be deleted at refcount=0: {}",
        obj_path.display()
    );

    // 4) Независимый объект bytes2 должен работать параллельно
    let bytes2 = b"hello-snapstore-2.2".to_vec();
    let (h2, existed2, rc2c) = ss.put(&bytes2)?;
    assert!(!existed2 && rc2c == 1, "fresh object must start with rc=1");
    assert_eq!(ss.get(&h2)?.unwrap(), bytes2);
    // cleanup
    let _ = ss.dec_ref(&h2)?;

    Ok(())
}
