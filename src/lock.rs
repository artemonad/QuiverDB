//! File-based locking for single-writer safety.
//!
//! Cross-platform (fs2) advisory locks:
//! - Exclusive: single writer, blocks other writers/readers using the same mode.
//! - Shared: for future multi-reader scenarios (не используется пока).
//!
//! Lock file path: <root>/LOCK
//! Lock is released on Drop.

use anyhow::{Context, Result};
use fs2::FileExt;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy)]
pub enum LockMode {
    Shared,
    Exclusive,
}

pub struct LockGuard {
    file: std::fs::File,
    path: PathBuf,
    mode: LockMode,
}

impl LockGuard {
    fn new(file: std::fs::File, path: PathBuf, mode: LockMode) -> Self {
        Self { file, path, mode }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn mode(&self) -> LockMode {
        self.mode
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        // fs2 unlock errors on drop are ignored deliberately.
        let _ = self.file.unlock();
    }
}

fn lock_file_path(root: &Path) -> PathBuf {
    root.join("LOCK")
}

fn open_lock_file(root: &Path) -> Result<std::fs::File> {
    let path = lock_file_path(root);
    let f = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&path)
        .with_context(|| format!("open lock file {}", path.display()))?;
    Ok(f)
}

/// Acquire a lock in the requested mode. Blocks until acquired.
pub fn acquire_lock(root: &Path, mode: LockMode) -> Result<LockGuard> {
    let file = open_lock_file(root)?;
    match mode {
        LockMode::Shared => file
            .lock_shared()
            .with_context(|| format!("lock_shared {}", lock_file_path(root).display()))?,
        LockMode::Exclusive => file
            .lock_exclusive()
            .with_context(|| format!("lock_exclusive {}", lock_file_path(root).display()))?,
    }
    Ok(LockGuard::new(file, lock_file_path(root), mode))
}

/// Try to acquire a lock in the requested mode. Returns Err if already locked.
pub fn try_acquire_lock(root: &Path, mode: LockMode) -> Result<LockGuard> {
    let file = open_lock_file(root)?;
    match mode {
        LockMode::Shared => file.try_lock_shared().with_context(|| {
            format!("try_lock_shared failed: {}", lock_file_path(root).display())
        })?,
        LockMode::Exclusive => file.try_lock_exclusive().with_context(|| {
            format!(
                "try_lock_exclusive failed: {}",
                lock_file_path(root).display()
            )
        })?,
    }
    Ok(LockGuard::new(file, lock_file_path(root), mode))
}

/// Convenience wrappers.
pub fn acquire_exclusive_lock(root: &Path) -> Result<LockGuard> {
    acquire_lock(root, LockMode::Exclusive)
}

pub fn try_acquire_exclusive_lock(root: &Path) -> Result<LockGuard> {
    try_acquire_lock(root, LockMode::Exclusive)
}

pub fn acquire_shared_lock(root: &Path) -> Result<LockGuard> {
    acquire_lock(root, LockMode::Shared)
}

pub fn try_acquire_shared_lock(root: &Path) -> Result<LockGuard> {
    try_acquire_lock(root, LockMode::Shared)
}