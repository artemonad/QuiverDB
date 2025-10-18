use anyhow::{Context, Result};
use std::path::PathBuf;

use QuiverDB::snapstore::restore_from_id;

/// CLI: snapshot-restore — восстановить БД из persisted‑снапшота.
///
/// Примеры:
///   quiverdb snapshot-restore --path ./dst --id <snapshot_id>
///   quiverdb snapshot-restore --path ./dst --src ./source_db --id <snapshot_id> --verify
///
/// Аргументы:
/// - --path: корень целевой БД (куда восстановить).
/// - --src:  корень, где расположен SnapStore источника (.snapstore/{objects,manifests}).
///           По умолчанию совпадает с --path.
/// - --id:   идентификатор снапшота (см. snapshot-list).
/// - --verify: включить базовую проверку длины страниц (по page_size).
pub fn exec(dst_root: PathBuf, src_root: Option<PathBuf>, id: String, verify: bool) -> Result<()> {
    if id.trim().is_empty() {
        anyhow::bail!("provide --id <snapshot_id>");
    }
    let src = src_root.unwrap_or_else(|| dst_root.clone());

    restore_from_id(&src, &dst_root, &id, verify).with_context(|| {
        format!(
            "restore snapshot id='{}' from {} to {}",
            id,
            src.display(),
            dst_root.display()
        )
    })?;

    println!(
        "snapshot-restore: OK (id='{}', src={}, dst={}, verify={})",
        id,
        src.display(),
        dst_root.display(),
        verify
    );
    Ok(())
}
