use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

// Подмодули с реализацией команд
mod admin;
mod rh;
mod db_cli;

use crate::util::parse_u8_byte;

/// RAII-гард, который:
/// - в begin() ставит clean_shutdown=false (DB «грязная»)
/// - при нормальном завершении (finish или Drop) возвращает clean_shutdown=true
/// Если процесс упадёт, Drop не вызовется, и флаг останется false — при старте будет WAL replay.
pub(crate) struct DirtyGuard {
    root: PathBuf,
    set_clean_on_drop: bool,
}

impl DirtyGuard {
    pub(crate) fn begin(root: &std::path::Path) -> anyhow::Result<Self> {
        crate::meta::set_clean_shutdown(root, false)?;
        Ok(Self {
            root: root.to_path_buf(),
            set_clean_on_drop: true,
        })
    }

    pub(crate) fn finish(mut self) -> anyhow::Result<()> {
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
    name = "quiverdb",
    version,
    about = "Embedded KV DB with in-page Robin Hood index, WAL and directory",
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
        Cmd::Init { path, page_size } => admin::cmd_init(path, page_size),
        Cmd::Status { path } => admin::cmd_status(path),
        Cmd::Alloc { path, count } => admin::cmd_alloc(path, count),
        Cmd::Write { path, page_id, fill } => admin::cmd_write(path, page_id, fill),
        Cmd::Read { path, page_id, len } => admin::cmd_read(path, page_id, len),

        // ------- low-level v2 (Robin Hood) -------
        Cmd::PagefmtRh { path, page_id } => rh::cmd_pagefmt_rh(path, page_id),
        Cmd::RhPut { path, page_id, key, value } => rh::cmd_rh_put(path, page_id, key, value),
        Cmd::RhGet { path, page_id, key } => rh::cmd_rh_get(path, page_id, key),
        Cmd::RhList { path, page_id } => rh::cmd_rh_list(path, page_id),
        Cmd::RhDel { path, page_id, key } => rh::cmd_rh_del(path, page_id, key),
        Cmd::RhCompact { path, page_id } => rh::cmd_rh_compact(path, page_id),

        // ------- high-level DB API -------
        Cmd::DbInit { path, page_size, buckets } => db_cli::cmd_db_init(path, page_size, buckets),
        Cmd::DbPut { path, key, value } => db_cli::cmd_db_put(path, key, value),
        Cmd::DbGet { path, key } => db_cli::cmd_db_get(path, key),
        Cmd::DbDel { path, key } => db_cli::cmd_db_del(path, key),
        Cmd::DbStats { path } => db_cli::cmd_db_stats(path),
    }
}