use anyhow::{anyhow, Context, Result};
use fs2::FileExt;
use std::fs::OpenOptions;
use std::path::PathBuf;

use QuiverDB::meta::set_clean_shutdown;
use QuiverDB::wal::Wal;

/// Выполнить WAL checkpoint: усечь WAL до заголовка под эксклюзивной блокировкой.
/// Замечания:
/// - Предпочтительно делать, когда нет активного writer’а.
/// - Требует эксклюзивный lock (<root>/LOCK). Если lock занят — вернёт ошибку.
pub fn exec(root: PathBuf) -> Result<()> {
    if !root.exists() {
        return Err(anyhow!("DB root does not exist: {}", root.display()));
    }

    // 1) Эксклюзивная блокировка <root>/LOCK (как writer)
    let lock_path = root.join("LOCK");
    let lock = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&lock_path)
        .with_context(|| format!("open lock file {}", lock_path.display()))?;
    lock.lock_exclusive()
        .with_context(|| format!("lock_exclusive {}", lock_path.display()))?;

    // 2) Усечение WAL до заголовка (идемпотентно)
    {
        let mut wal = Wal::open_for_append(&root)?;
        wal.truncate_to_header()?;
    }

    // 3) Best-effort: clean_shutdown=true (если придётся — будет переписан writer’ом)
    let _ = set_clean_shutdown(&root, true);

    println!("checkpoint: WAL truncated to header at {}", root.display());
    Ok(())
}