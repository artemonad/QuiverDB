// src/cli.rs

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

// Подмодули с реализацией команд
pub mod admin;   // публичный, чтобы тесты могли вызывать admin::* напрямую
mod rh;
mod db_cli;
// CDC (wal-tail/ship/apply/record/replay) — публичный, чтобы использовать в интеграционных тестах
pub mod cdc;

pub use cdc::wal_apply_from_stream; // удобный реэкспорт для тестов и внешнего кода

use crate::util::parse_u8_byte;

/// RAII-гард, который:
/// - в begin() ставит clean_shutdown=false (БД «грязная»)
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

    // Free-list tooling (v0.6)
    FreeStatus {
        #[arg(long)]
        path: PathBuf,
    },
    FreePop {
        #[arg(long)]
        path: PathBuf,
        #[arg(long, default_value_t = 1)]
        count: u32,
    },
    FreePush {
        #[arg(long)]
        path: PathBuf,
        #[arg(long)]
        page_id: u64,
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
    // Новая команда: скан ключей (вся БД или по префиксу), текст/JSON
    DbScan {
        #[arg(long)]
        path: PathBuf,
        /// Optional key prefix (string; scanned as bytes)
        #[arg(long)]
        prefix: Option<String>,
        /// JSON output
        #[arg(long, default_value_t = false)]
        json: bool,
    },

    // CDC / WAL tooling
    WalTail {
        #[arg(long)]
        path: PathBuf,
        /// Follow new records (like tail -f)
        #[arg(long, default_value_t = false)]
        follow: bool,
    },
    // Расширенная версия: поддержка --since-lsn и --sink (tcp://host:port)
    WalShip {
        #[arg(long)]
        path: PathBuf,
        /// Follow new records (send continuously)
        #[arg(long, default_value_t = false)]
        follow: bool,
        /// Ship only records with lsn > N (resume by LSN)
        #[arg(long)]
        since_lsn: Option<u64>,
        /// Output sink: tcp://host:port (default: stdout)
        #[arg(long)]
        sink: Option<String>,
    },
    WalApply {
        /// Target DB path to apply incoming WAL stream from stdin
        #[arg(long)]
        path: PathBuf,
    },
    /// CDC helper: print meta.last_lsn for resume
    CdcLastLsn {
        #[arg(long)]
        path: PathBuf,
    },
    /// CDC: record WAL stream to a file (wire format), with LSN filtering
    CdcRecord {
        #[arg(long)]
        path: PathBuf,
        #[arg(long)]
        out: PathBuf,
        /// Keep only frames with lsn > from_lsn (optional)
        #[arg(long)]
        from_lsn: Option<u64>,
        /// Keep only frames with lsn <= to_lsn (optional)
        #[arg(long)]
        to_lsn: Option<u64>,
    },
    /// CDC: replay a WAL stream from file (or stdin), with LSN filtering
    CdcReplay {
        #[arg(long)]
        path: PathBuf,
        /// Optional input file (wire format). If omitted, read from stdin.
        #[arg(long)]
        input: Option<PathBuf>,
        /// Apply only frames with lsn > from_lsn (optional)
        #[arg(long)]
        from_lsn: Option<u64>,
        /// Apply only frames with lsn <= to_lsn (optional)
        #[arg(long)]
        to_lsn: Option<u64>,
    },

    // v0.8: check (CRC scan, overflow reachability) + strict/json режимы
    Check {
        #[arg(long)]
        path: PathBuf,
        /// Strict mode: non-zero exit if problems detected (dir error, CRC/IO errors, orphan overflow)
        #[arg(long, default_value_t = false)]
        strict: bool,
        /// JSON output: a single JSON object with summary
        #[arg(long, default_value_t = false)]
        json: bool,
    },

    // v0.9: repair (free orphan overflow pages)
    Repair {
        #[arg(long)]
        path: PathBuf,
    },

    // v0.9: metrics (snapshot / reset) + JSON
    Metrics {
        /// JSON output: one-line JSON with metrics snapshot
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    MetricsReset,
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

        // ------- free-list tooling -------
        Cmd::FreeStatus { path } => admin::cmd_free_status(path),
        Cmd::FreePop { path, count } => admin::cmd_free_pop(path, count),
        Cmd::FreePush { path, page_id } => admin::cmd_free_push(path, page_id),

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
        Cmd::DbScan { path, prefix, json } => db_cli::cmd_db_scan(path, prefix, json),

        // ------- CDC -------
        Cmd::WalTail { path, follow } => cdc::cmd_wal_tail(path, follow),
        Cmd::WalShip { path, follow, since_lsn, sink } =>
            cdc::cmd_wal_ship_ext(path, follow, since_lsn, sink),
        Cmd::WalApply { path } => cdc::cmd_wal_apply(path),
        Cmd::CdcLastLsn { path } => cdc::cmd_cdc_last_lsn(path),
        Cmd::CdcRecord { path, out, from_lsn, to_lsn } =>
            cdc::cmd_cdc_record(path, out, from_lsn, to_lsn),
        Cmd::CdcReplay { path, input, from_lsn, to_lsn } =>
            cdc::cmd_cdc_replay(path, input, from_lsn, to_lsn),

        // ------- v0.8: check + strict/json -------
        Cmd::Check { path, strict, json } => admin::cmd_check_strict_json(path, strict, json),

        // ------- v0.9: repair -------
        Cmd::Repair { path } => admin::cmd_repair(path),

        // ------- v0.9: metrics -------
        Cmd::Metrics { json } => admin::cmd_metrics_json(json),
        Cmd::MetricsReset => admin::cmd_metrics_reset(),
    }
}