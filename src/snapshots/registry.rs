//! Persisted snapshots registry (Phase 2 — best‑effort).
//!
//! Формат: <root>/.snapshots/registry.json
//! {
//!   "entries": [
//!     {"id":"<snap_id>","lsn":123,"ts_nanos":<u128>,"ended":false},
//!     ...
//!   ]
//! }
//!
//! Замечания:
//! - Все операции best‑effort: ошибки пробрасываются, вызывающая сторона может их игнорировать.
//! - Запись выполняется атомарно через tmp+rename.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Serialize, Deserialize)]
struct Registry {
    entries: Vec<RegEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RegEntry {
    id: String,
    lsn: u64,
    ts_nanos: u128,
    ended: bool,
}

fn registry_dir(root: &Path) -> PathBuf {
    root.join(".snapshots")
}
fn registry_path(root: &Path) -> PathBuf {
    registry_dir(root).join("registry.json")
}

fn registry_load(root: &Path) -> Result<Registry> {
    let p = registry_path(root);
    if !p.exists() {
        return Ok(Registry { entries: Vec::new() });
    }
    let bytes = fs::read(&p).with_context(|| format!("read {}", p.display()))?;
    let reg: Registry = serde_json::from_slice(&bytes).context("parse registry.json")?;
    Ok(reg)
}

fn registry_save(root: &Path, reg: &Registry) -> Result<()> {
    let dir = registry_dir(root);
    fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
    let path = registry_path(root);
    let tmp = dir.join("registry.json.tmp");

    let mut f = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tmp)
        .with_context(|| format!("open {}", tmp.display()))?;

    let data = serde_json::to_vec_pretty(reg).context("serialize registry.json")?;
    f.write_all(&data)?;
    let _ = f.sync_all();

    fs::rename(&tmp, &path)
        .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
    Ok(())
}

/// Добавить запись о снапшоте (best‑effort).
pub(crate) fn registry_add(root: &Path, id: &str, lsn: u64) -> Result<()> {
    let mut reg = registry_load(root)?;
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();

    reg.entries.push(RegEntry {
        id: id.to_string(),
        lsn,
        ts_nanos: ts,
        ended: false,
    });

    registry_save(root, &reg)
}

/// Пометить снапшот как завершённый (best‑effort).
pub(crate) fn registry_mark_ended(root: &Path, id: &str) -> Result<()> {
    let mut reg = registry_load(root)?;
    for e in reg.entries.iter_mut() {
        if e.id == id {
            e.ended = true;
            break;
        }
    }
    registry_save(root, &reg)
}