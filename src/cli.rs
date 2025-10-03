use anyhow::{anyhow, Result};
use clap::{Parser, Subcommand};
use std::path::{Path, PathBuf};

use crate::db::Db;
use crate::dir::Directory;
use crate::meta::read_meta;
use crate::page_rh::{
    rh_compact_inplace, rh_kv_delete_inplace, rh_kv_insert, rh_kv_list, rh_kv_lookup, rh_page_init,
    rh_page_is_kv,
};
use crate::pager::Pager;
use crate::{
    acquire_exclusive_lock, display_text, hex_dump, init_db, parse_u8_byte, wal_replay_if_any,
};

/// RAII-гард, который:
/// - в begin() ставит clean_shutdown=false (DB «грязная»)
/// - при нормальном завершении (finish или Drop) возвращает clean_shutdown=true
/// Если процесс упадёт, Drop не вызовется, и флаг останется false — при старте будет WAL replay.
struct DirtyGuard {
    root: PathBuf,
    set_clean_on_drop: bool,
}

impl DirtyGuard {
    fn begin(root: &Path) -> anyhow::Result<Self> {
        crate::meta::set_clean_shutdown(root, false)?;
        Ok(Self {
            root: root.to_path_buf(),
            set_clean_on_drop: true,
        })
    }

    fn finish(mut self) -> anyhow::Result<()> {
        if self.set_clean_on_drop {
            crate::meta::set_clean_shutdown(&self.root, true)?;
            self.set_clean_on_drop = false;
        }
        Ok(())
    }
}

impl Drop for DirtyGuard {
    fn drop(&mut self) {
        if self.set_clean_on_drop {
            let _ = crate::meta::set_clean_shutdown(&self.root, true);
        }
    }
}

#[derive(Parser, Debug)]
#[command(
    name = "p1",
    version,
    about = "Embedded KV DB with pager, WAL and directory",
    arg_required_else_help = true
)]
pub struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
pub enum Cmd {
    // Низкоуровневые общие операции
    Init {
        #[arg(long)]
        path: PathBuf,
        #[arg(long, default_value_t = 4096)]
        page_size: u32,
    },
    Status {
        #[arg(long)]
        path: PathBuf,
    },
    Alloc {
        #[arg(long)]
        path: PathBuf,
        #[arg(long, default_value_t = 1)]
        count: u32,
    },
    Write {
        #[arg(long)]
        path: PathBuf,
        #[arg(long)]
        page_id: u64,
        #[arg(long, value_parser = parse_u8_byte, default_value_t = 0xAB)]
        fill: u8,
    },
    Read {
        #[arg(long)]
        path: PathBuf,
        #[arg(long)]
        page_id: u64,
        #[arg(long, default_value_t = 64)]
        len: usize,
    },

    // Низкоуровневые команды (v2: Robin Hood)
    PagefmtRh {
        #[arg(long)]
        path: PathBuf,
        #[arg(long)]
        page_id: u64,
    },
    RhPut {
        #[arg(long)]
        path: PathBuf,
        #[arg(long)]
        page_id: u64,
        #[arg(long)]
        key: String,
        #[arg(long)]
        value: String,
    },
    RhGet {
        #[arg(long)]
        path: PathBuf,
        #[arg(long)]
        page_id: u64,
        #[arg(long)]
        key: String,
    },
    RhList {
        #[arg(long)]
        path: PathBuf,
        #[arg(long)]
        page_id: u64,
    },
    RhDel {
        #[arg(long)]
        path: PathBuf,
        #[arg(long)]
        page_id: u64,
        #[arg(long)]
        key: String,
    },
    RhCompact {
        #[arg(long)]
        path: PathBuf,
        #[arg(long)]
        page_id: u64,
    },

    // Высокоуровневые (каталог + DB API)
    DbInit {
        #[arg(long)]
        path: PathBuf,
        #[arg(long, default_value_t = 4096)]
        page_size: u32,
        #[arg(long, default_value_t = 1024)]
        buckets: u32,
    },
    DbPut {
        #[arg(long)]
        path: PathBuf,
        #[arg(long)]
        key: String,
        #[arg(long)]
        value: String,
    },
    DbGet {
        #[arg(long)]
        path: PathBuf,
        #[arg(long)]
        key: String,
    },
    DbDel {
        #[arg(long)]
        path: PathBuf,
        #[arg(long)]
        key: String,
    },
    DbStats {
        #[arg(long)]
        path: PathBuf,
    },
}

pub fn run() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        // ------- common low-level -------
        Cmd::Init { path, page_size } => {
            init_db(&path, page_size)?;
            println!("Initialized DB at {}", path.display());
        }
        Cmd::Status { path } => {
            let meta = read_meta(&path)?;
            println!("DB at {}", path.display());
            println!("  version      = {}", meta.version);
            println!("  page_size    = {} bytes", meta.page_size);
            println!("  hash_kind    = {}", meta.hash_kind);
            println!("  flags        = 0x{:08x}", meta.flags);
            println!("    tde        = {}", (meta.flags & 0x1) != 0);
            println!("  next_page_id = {}", meta.next_page_id);

            if let Ok(dir) = Directory::open(&path) {
                println!("  buckets      = {}", dir.bucket_count);
                let used = dir.count_used_buckets()?;
                println!("  used_buckets = {}", used);
            } else {
                println!("  directory    = (not initialized)");
            }
        }
        Cmd::Alloc { path, count } => {
            let _lock = acquire_exclusive_lock(&path)?;
            let guard = DirtyGuard::begin(&path)?;
            wal_replay_if_any(&path)?;
            let mut pager = Pager::open(&path)?;
            let start = pager.allocate_pages(count as u64)?;
            println!(
                "Allocated {} page(s): [{}..{}]",
                count,
                start,
                start + count as u64 - 1
            );
            guard.finish()?;
        }
        Cmd::Write { path, page_id, fill } => {
            let _lock = acquire_exclusive_lock(&path)?;
            let guard = DirtyGuard::begin(&path)?;
            wal_replay_if_any(&path)?;
            let mut pager = Pager::open(&path)?;
            pager.ensure_allocated(page_id)?;
            let ps = pager.meta.page_size as usize;
            let mut buf = vec![fill; ps];
            pager.commit_page(page_id, &mut buf)?;
            println!("Wrote page {} with fill=0x{:02x}", page_id, fill);
            guard.finish()?;
        }
        Cmd::Read { path, page_id, len } => {
            wal_replay_if_any(&path)?;
            let pager = Pager::open(&path)?;
            if page_id >= pager.meta.next_page_id {
                return Err(anyhow!(
                    "page {} not allocated yet (next_page_id = {})",
                    page_id,
                    pager.meta.next_page_id
                ));
            }
            let ps = pager.meta.page_size as usize;
            let mut buf = vec![0u8; ps];
            pager.read_page(page_id, &mut buf)?;
            let n = len.min(ps);
            println!("First {} bytes of page {}:", n, page_id);
            println!("{}", hex_dump(&buf[..n]));
        }

        // ------- low-level v2 (Robin Hood) -------
        Cmd::PagefmtRh { path, page_id } => {
            let _lock = acquire_exclusive_lock(&path)?;
            let guard = DirtyGuard::begin(&path)?;
            wal_replay_if_any(&path)?;
            let mut pager = Pager::open(&path)?;
            pager.ensure_allocated(page_id)?;
            let ps = pager.meta.page_size as usize;
            let mut buf = vec![0u8; ps];
            rh_page_init(&mut buf, page_id)?;
            pager.commit_page(page_id, &mut buf)?;
            println!("Formatted page {} as KV-RH (v2)", page_id);
            guard.finish()?;
        }
        Cmd::RhPut {
            path,
            page_id,
            key,
            value,
        } => {
            let _lock = acquire_exclusive_lock(&path)?;
            let guard = DirtyGuard::begin(&path)?;
            wal_replay_if_any(&path)?;
            let meta = read_meta(&path)?;
            let mut pager = Pager::open(&path)?;
            pager.ensure_allocated(page_id)?;
            let ps = pager.meta.page_size as usize;
            let mut buf = vec![0u8; ps];
            pager.read_page(page_id, &mut buf)?;
            if !rh_page_is_kv(&buf) {
                return Err(anyhow!(
                    "page {} is not KV-RH formatted (run 'pagefmt-rh' first)",
                    page_id
                ));
            }
            let k = key.as_bytes();
            let v = value.as_bytes();
            match rh_kv_insert(&mut buf, meta.hash_kind, k, v)? {
                true => {
                    pager.commit_page(page_id, &mut buf)?;
                    println!(
                        "Inserted key='{}' ({} B), value={} B into RH page {}",
                        display_text(k),
                        k.len(),
                        v.len(),
                        page_id
                    );
                }
                false => {
                    println!(
                        "Not enough space on RH page {} for key={} B, value={} B",
                        page_id,
                        k.len(),
                        v.len()
                    );
                }
            }
            guard.finish()?;
        }
        Cmd::RhGet { path, page_id, key } => {
            wal_replay_if_any(&path)?;
            let meta = read_meta(&path)?;
            let pager = Pager::open(&path)?;
            if page_id >= pager.meta.next_page_id {
                return Err(anyhow!(
                    "page {} not allocated yet (next_page_id = {})",
                    page_id,
                    pager.meta.next_page_id
                ));
            }
            let ps = pager.meta.page_size as usize;
            let mut buf = vec![0u8; ps];
            pager.read_page(page_id, &mut buf)?;
            if !rh_page_is_kv(&buf) {
                return Err(anyhow!(
                    "page {} is not KV-RH formatted (run 'pagefmt-rh' first)",
                    page_id
                ));
            }
            let k = key.as_bytes();
            match rh_kv_lookup(&buf, meta.hash_kind, k)? {
                Some(v) => {
                    println!(
                        "FOUND key='{}' ({} B) -> value ({} B): {}",
                        display_text(k),
                        k.len(),
                        v.len(),
                        display_text(&v)
                    );
                    println!("hex: {}", hex_dump(&v[..v.len().min(64)]));
                }
                None => println!("NOT FOUND key='{}'", display_text(k)),
            }
        }
        Cmd::RhList { path, page_id } => {
            wal_replay_if_any(&path)?;
            let pager = Pager::open(&path)?;
            if page_id >= pager.meta.next_page_id {
                return Err(anyhow!(
                    "page {} not allocated yet (next_page_id = {})",
                    page_id,
                    pager.meta.next_page_id
                ));
            }
            let ps = pager.meta.page_size as usize;
            let mut buf = vec![0u8; ps];
            pager.read_page(page_id, &mut buf)?;
            if !rh_page_is_kv(&buf) {
                return Err(anyhow!(
                    "page {} is not KV-RH formatted (run 'pagefmt-rh' first)",
                    page_id
                ));
            }
            let items = rh_kv_list(&buf)?;
            println!("RH Page {}: {} record(s)", page_id, items.len());
            for (i, (k, v)) in items.iter().enumerate() {
                println!(
                    "  {:04}: key='{}' ({} B), value='{}' ({} B)",
                    i,
                    display_text(k),
                    k.len(),
                    display_text(v),
                    v.len()
                );
            }
        }
        Cmd::RhDel { path, page_id, key } => {
            let _lock = acquire_exclusive_lock(&path)?;
            let guard = DirtyGuard::begin(&path)?;
            wal_replay_if_any(&path)?;
            let meta = read_meta(&path)?;
            let mut pager = Pager::open(&path)?;
            if page_id >= pager.meta.next_page_id {
                return Err(anyhow!(
                    "page {} not allocated yet (next_page_id = {})",
                    page_id,
                    pager.meta.next_page_id
                ));
            }
            let ps = pager.meta.page_size as usize;
            let mut buf = vec![0u8; ps];
            pager.read_page(page_id, &mut buf)?;
            if !rh_page_is_kv(&buf) {
                return Err(anyhow!(
                    "page {} is not KV-RH formatted (run 'pagefmt-rh' first)",
                    page_id
                ));
            }
            let existed = rh_kv_delete_inplace(&mut buf, meta.hash_kind, key.as_bytes())?;
            if existed {
                pager.commit_page(page_id, &mut buf)?;
                println!("Deleted key='{}' from RH page {}", key, page_id);
            } else {
                println!("Key='{}' not found on RH page {}", key, page_id);
            }
            guard.finish()?;
        }
        Cmd::RhCompact { path, page_id } => {
            let _lock = acquire_exclusive_lock(&path)?;
            let guard = DirtyGuard::begin(&path)?;
            wal_replay_if_any(&path)?;
            let meta = read_meta(&path)?;
            let mut pager = Pager::open(&path)?;
            if page_id >= pager.meta.next_page_id {
                return Err(anyhow!(
                    "page {} not allocated yet (next_page_id = {})",
                    page_id,
                    pager.meta.next_page_id
                ));
            }
            let ps = pager.meta.page_size as usize;
            let mut buf = vec![0u8; ps];
            pager.read_page(page_id, &mut buf)?;
            if !rh_page_is_kv(&buf) {
                return Err(anyhow!(
                    "page {} is not KV-RH formatted (run 'pagefmt-rh' first)",
                    page_id
                ));
            }
            rh_compact_inplace(&mut buf, meta.hash_kind)?;
            pager.commit_page(page_id, &mut buf)?;
            println!("Compacted RH page {}", page_id);
            guard.finish()?;
        }

        // ------- high-level DB API -------
        Cmd::DbInit {
            path,
            page_size,
            buckets,
        } => {
            if !path.exists() || !path.join("meta").exists() {
                init_db(&path, page_size)?;
                println!("Initialized DB at {}", path.display());
            } else {
                let meta = read_meta(&path)?;
                if meta.page_size != page_size {
                    println!(
                        "Warning: meta.page_size={} differs from requested {}. Using {}",
                        meta.page_size, page_size, meta.page_size
                    );
                }
            }
            if path.join("dir").exists() {
                anyhow::bail!("Directory already exists at {}/dir", path.display());
            }
            crate::dir::Directory::create(&path, buckets)?;
            println!("Created directory with {} buckets", buckets);
        }
        Cmd::DbPut { path, key, value } => {
            let mut db = Db::open(&path)?;
            db.put(key.as_bytes(), value.as_bytes())?;
            println!("OK");
        }
        Cmd::DbGet { path, key } => {
            let db = Db::open(&path)?;
            match db.get(key.as_bytes())? {
                Some(v) => {
                    println!("FOUND '{}': {}", key, display_text(&v));
                    println!("hex: {}", hex_dump(&v[..v.len().min(64)]));
                }
                None => println!("NOT FOUND '{}'", key),
            }
        }
        Cmd::DbDel { path, key } => {
            let mut db = Db::open(&path)?;
            let existed = db.del(key.as_bytes())?;
            if existed {
                println!("DELETED '{}'", key);
            } else {
                println!("NOT FOUND '{}'", key);
            }
        }
        Cmd::DbStats { path } => {
            let db = Db::open(&path)?;
            db.print_stats()?;
        }
    }
    Ok(())
}