use anyhow::{anyhow, Context, Result};
use std::path::PathBuf;

use QuiverDB::db::Db;
use QuiverDB::snapstore::{list_manifests, manifest_path, read_manifest, SnapshotManager};

/// Создать persisted‑снапшот и вывести id/путь.
pub fn exec_create(
    path: PathBuf,
    message: Option<String>,
    labels: Vec<String>,
    parent: Option<String>,
) -> Result<()> {
    // Откроем БД в RO-режиме: снимок не требует writer'а
    let db = Db::open_ro(&path).with_context(|| format!("open RO DB at {}", path.display()))?;

    // labels: Vec<String> -> Vec<&str>
    let label_refs: Vec<&str> = labels.iter().map(|s| s.as_str()).collect();

    let id =
        SnapshotManager::create_persisted(&db, message.as_deref(), &label_refs, parent.as_deref())
            .with_context(|| "create_persisted snapshot")?;

    let mpath = manifest_path(&path, &id);
    println!("snapshot: id={} manifest={}", id, mpath.display());
    Ok(())
}

/// Список всех манифестов (id), опционально JSON.
pub fn exec_list(path: PathBuf, json: bool) -> Result<()> {
    let ids =
        list_manifests(&path).with_context(|| format!("list manifests at {}", path.display()))?;
    if json {
        print_json_array(&ids);
        return Ok(());
    }
    if ids.is_empty() {
        println!("(no snapshots)");
        return Ok(());
    }
    for id in ids {
        println!("{id}");
    }
    Ok(())
}

/// Инспекция манифеста по id, опционально JSON.
pub fn exec_inspect(path: PathBuf, id: String, json: bool) -> Result<()> {
    if id.trim().is_empty() {
        return Err(anyhow!("provide snapshot id"));
    }
    let m = read_manifest(&path, &id)
        .with_context(|| format!("read manifest {} at {}", id, path.display()))?;
    if json {
        let s = serde_json::to_string_pretty(&m).unwrap_or_else(|_| "{}".to_string());
        println!("{s}");
        return Ok(());
    }

    // Human-readable
    println!("Snapshot manifest v{}", m.meta.version);
    println!("  id          = {}", m.meta.id);
    println!(
        "  parent      = {}",
        m.meta.parent.as_deref().unwrap_or("(none)")
    );
    println!("  created_ms  = {}", m.meta.created_unix_ms);
    if let Some(msg) = m.meta.message.as_deref() {
        println!("  message     = {}", msg);
    }
    if !m.meta.labels.is_empty() {
        println!("  labels      = {}", m.meta.labels.join(", "));
    }
    println!("  lsn         = {}", m.meta.lsn);
    println!("  page_size   = {}", m.meta.page_size);
    println!("  next_page_id= {}", m.meta.next_page_id);
    println!("  buckets     = {}", m.meta.buckets);
    println!("Heads (bucket -> head_pid): {}", m.heads.len());
    for h in &m.heads {
        println!("  - {:6} -> {}", h.bucket, h.head_pid);
    }
    println!("Objects (pages): {}", m.objects.len());
    for o in &m.objects {
        println!("  - page {:10}  {}  {} B", o.page_id, o.hash_hex, o.bytes);
    }

    Ok(())
}

/// NEW: Удаление persisted‑снапшота по id.
/// Поведение:
/// - уменьшает refcount у всех объектов снапшота (объект удаляется при rc==0),
/// - удаляет manifest (<snapstore_dir>/manifests/<id>.json).
pub fn exec_delete(path: PathBuf, id: String) -> Result<()> {
    if id.trim().is_empty() {
        return Err(anyhow!("provide snapshot id"));
    }
    SnapshotManager::delete_persisted(&path, &id)
        .with_context(|| format!("delete snapshot id='{}' at {}", id, path.display()))?;
    println!("snapshot-delete: OK (id='{}')", id);
    Ok(())
}

// ------------- helpers -------------

fn print_json_array(ids: &[String]) {
    print!("[");
    for (i, id) in ids.iter().enumerate() {
        if i > 0 {
            print!(",");
        }
        print!("\"{}\"", id);
    }
    println!("]");
}
