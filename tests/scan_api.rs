use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use QuiverDB::{init_db, Db, Directory};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let base = std::env::temp_dir();
    base.join(format!("qdbtest-scan-{prefix}-{pid}-{t}-{id}"))
}

#[test]
fn scan_all_and_prefix_basic_and_overflow() -> Result<()> {
    // Инициализация
    let root = unique_root("basic");
    fs::create_dir_all(&root)?;
    init_db(&root, 4096)?;
    Directory::create(&root, 64)?;

    // Writer: базовые ключи + overflow-значение
    {
        let mut db = Db::open(&root)?;
        db.put(b"alpha", b"1")?;
        db.put(b"beta", b"2")?;
        db.put(b"k123", b"vA")?;
        db.put(b"k999", b"vB")?;

        // overflow value (> page_size/4)
        let ps = db.pager.meta.page_size as usize;
        let threshold = ps / 4;
        let big = vec![0xABu8; threshold + 300];
        db.put(b"big", &big)?;
    }

    // Reader: scan_all
    {
        let db = Db::open_ro(&root)?;
        let all = db.scan_all()?;
        let mut map = std::collections::HashMap::new();
        for (k, v) in all {
            map.insert(k, v);
        }

        assert_eq!(map.get(b"alpha" as &[u8]).map(|v| v.as_slice()), Some(b"1".as_ref()));
        assert_eq!(map.get(b"beta".as_ref()).map(|v| v.as_slice()), Some(b"2".as_ref()));
        assert_eq!(map.get(b"k123".as_ref()).map(|v| v.as_slice()), Some(b"vA".as_ref()));
        assert_eq!(map.get(b"k999".as_ref()).map(|v| v.as_slice()), Some(b"vB".as_ref()));
        // Проверяем, что overflow восстановился корректно
        let ps = db.pager.meta.page_size as usize;
        let threshold = ps / 4;
        let expected = vec![0xABu8; threshold + 300];
        let got = map.get(b"big".as_ref()).expect("big must exist");
        assert_eq!(got.len(), expected.len());
        assert_eq!(&got[..], &expected[..]);
    }

    // scan_prefix
    {
        let db = Db::open_ro(&root)?;
        let kpairs = db.scan_prefix(b"k")?;
        let mut kmap = std::collections::HashMap::new();
        for (k, v) in kpairs {
            kmap.insert(k, v);
        }
        assert_eq!(kmap.len(), 2, "expected two keys with prefix 'k'");
        assert_eq!(kmap.get(b"k123".as_ref()).map(|v| v.as_slice()), Some(b"vA".as_ref()));
        assert_eq!(kmap.get(b"k999".as_ref()).map(|v| v.as_slice()), Some(b"vB".as_ref()));
    }

    // Обновление существующего ключа и проверка, что scan_prefix видит новое значение
    {
        let mut db = Db::open(&root)?;
        db.put(b"k123", b"vA2")?;
    }
    {
        let db = Db::open_ro(&root)?;
        let kpairs = db.scan_prefix(b"k1")?;
        let mut kmap = std::collections::HashMap::new();
        for (k, v) in kpairs {
            kmap.insert(k, v);
        }
        assert_eq!(kmap.len(), 1, "only 'k123' should match prefix 'k1'");
        assert_eq!(kmap.get(b"k123".as_ref()).map(|v| v.as_slice()), Some(b"vA2".as_ref()));
    }

    Ok(())
}