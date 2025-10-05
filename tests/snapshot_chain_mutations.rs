// tests/snapshot_chain_mutations.rs
//
// Запуск только этого файла:
//   cargo test --test snapshot_chain_mutations -- --nocapture
//
// Сценарий (легкий):
// - Подбираем набор ключей, которые попадают в один и тот же bucket (чтобы гарантированно
//   формировалась цепочка страниц). Значения делаем крупными, чтобы набрать несколько страниц.
// - Делаем snapshot.
// - После snapshot: обновляем часть ключей, удаляем несколько "ранних" ключей (чтобы вероятно
//   освободились ранние страницы цепочки), добавляем новые ключи в этот же bucket.
// - Под snapshot: все старые ключи видны со старыми значениями; новые ключи не видны;
//   обновленные ключи под snapshot видят старые значения.
//
// Примечание:
// - Мы не стремимся детерминированно доказать "вырезание пустых страниц", а проверяем,
//   что изменения цепочки (обновления/удаления/добавления) не ломают snapshot‑видимость.

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
    base.join(format!("qdbtest-snap-chain-{prefix}-{pid}-{t}-{id}"))
}

// Подобрать "count" ключей, которые попадают в один и тот же bucket.
fn pick_keys_same_bucket(dir: &Directory, count: usize, prefix: &str) -> Vec<String> {
    let mut keys = Vec::new();

    // Найдём целевой bucket по первому подходящему ключу
    let mut i = 0usize;
    let mut target_bucket: Option<u32> = None;

    while keys.len() < count {
        let k = format!("{}_{:04}", prefix, i);
        let b = dir.bucket_of_key(k.as_bytes());
        if let Some(tb) = target_bucket {
            if b == tb {
                keys.push(k);
            }
        } else {
            // первый ключ определяет целевой bucket
            target_bucket = Some(b);
            keys.push(k);
        }
        i += 1;
        // безопасный ограничитель на случай экстремальной неудачи
        if i > 200_000 {
            break;
        }
    }

    keys
}

#[test]
fn snapshot_chain_mutations_basic() -> Result<()> {
    // Делаем LSN-коалессацию нулевой для стабильности
    std::env::set_var("P1_WAL_COALESCE_MS", "0");

    let root = unique_root("chain");
    fs::create_dir_all(&root)?;
    init_db(&root, 4096)?;
    Directory::create(&root, 128)?;

    // Будем работать с крупными значениями, чтобы гарантированно получить несколько страниц.
    let ps = read_meta(&root)?.page_size as usize;
    let threshold = ps / 4;
    let val_len = (threshold + 600).min(ps - 64); // безопасно меньше размера страницы

    // Подготовим набор ключей в одном bucket
    let dir = Directory::open(&root)?;
    let keys = pick_keys_same_bucket(&dir, 16, "kb"); // 16 ключей, один bucket

    // Старые значения (до snapshot)
    let mut old_values = Vec::new();
    for (i, _k) in keys.iter().enumerate() {
        let mut v = vec![0u8; val_len];
        for j in 0..v.len() {
            v[j] = ((i as u8).wrapping_mul(31)).wrapping_add((j as u8).wrapping_mul(7));
        }
        old_values.push(v);
    }

    // Запишем все ключи
    {
        let mut db = Db::open(&root)?;
        for (k, v) in keys.iter().zip(old_values.iter()) {
            db.put(k.as_bytes(), v)?;
        }
    }

    // Начинаем snapshot
    let mut db = Db::open(&root)?;
    let mut snap = db.snapshot_begin()?;
    let _lsn = snap.lsn();

    // Изменения после snapshot
    // - Обновим последние 2 ключа новыми значениями
    for idx in (keys.len() - 2)..keys.len() {
        let k = &keys[idx];
        let mut nv = vec![0u8; val_len];
        for j in 0..nv.len() {
            nv[j] = 0xEE ^ ((idx as u8).wrapping_add(j as u8));
        }
        db.put(k.as_bytes(), &nv)?;
    }

    // - Удалим первые 6 ключей (не все, чтобы head не стал NO_PAGE)
    for idx in 0..6 {
        let k = &keys[idx];
        let _ = db.del(k.as_bytes())?;
    }

    // - Добавим парочку новых ключей в тот же bucket
    let new_keys = pick_keys_same_bucket(&dir, 3, "kb_new");
    for (ni, nk) in new_keys.iter().enumerate() {
        let mut nv = vec![0u8; (threshold + 200 + ni * 10).min(ps - 64)];
        for j in 0..nv.len() {
            nv[j] = 0xA5 ^ ((ni as u8).wrapping_add(j as u8));
        }
        db.put(nk.as_bytes(), &nv)?;
    }

    // Проверки под snapshot:
    // - Первые 6 удаленных ключей всё ещё видны (старые значения)
    for idx in 0..6 {
        let k = &keys[idx];
        let got = snap.get(k.as_bytes())?
            .expect("deleted after snapshot key must exist under snapshot");
        assert_eq!(
            got.as_slice(),
            old_values[idx].as_slice(),
            "snapshot must see OLD value for {}",
            k
        );
    }

    // - Последние 2 обновленных ключа под snapshot видят старые значения
    for idx in (keys.len() - 2)..keys.len() {
        let k = &keys[idx];
        let got = snap.get(k.as_bytes())?
            .expect("updated-after-snapshot key must exist under snapshot");
        assert_eq!(
            got.as_slice(),
            old_values[idx].as_slice(),
            "snapshot must see OLD value for updated key {}",
            k
        );
    }

    // - Новые ключи, появившиеся после snapshot, под snapshot не видны
    for nk in &new_keys {
        let got = snap.get(nk.as_bytes())?;
        assert!(
            got.is_none(),
            "new key '{}' created after snapshot must NOT be visible under snapshot",
            nk
        );
    }

    // Завершаем snapshot
    db.snapshot_end(&mut snap)?;

    Ok(())
}