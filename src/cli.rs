use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::{PathBuf};

// Подмодули с реализацией команд
pub mod admin;   // публичный, чтобы тесты могли вызывать admin::* напрямую
mod rh;
mod db_cli;
// CDC (wal-tail/ship/apply/record/replay) — публичный, чтобы использовать в интеграционных тестах
pub mod cdc;

pub use cdc::wal_apply_from_stream; // удобный реэкспорт для тестов и внешнего кода

use crate::util::parse_u8_byte;

// Для base64 печати сырых байт реестра при не-UTF8
use base64::Engine as _; // позволяет вызывать STANDARD.encode(...)

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
        /// JSON output (shortcut for P1_STATUS_JSON=1)
        #[arg(long, default_value_t = false)]
        json: bool,
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
        /// JSON output (shortcut for P1_DBSTATS_JSON=1)
        #[arg(long, default_value_t = false)]
        json: bool,
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
    // Расширенная версия: поддержка --since-lsn и --sink (tcp://host:port | file://path)
    WalShip {
        #[arg(long)]
        path: PathBuf,
        /// Follow new records (send continuously)
        #[arg(long, default_value_t = false)]
        follow: bool,
        /// Ship only records with lsn > N (resume by LSN)
        #[arg(long)]
        since_lsn: Option<u64>,
        /// Output sink: tcp://host:port | file://path (default: stdout)
        #[arg(long)]
        sink: Option<String>,
        /// Flush every N frames (shortcut for P1_SHIP_FLUSH_EVERY)
        #[arg(long)]
        flush_every: Option<usize>,
        /// Flush after >= B written bytes (shortcut for P1_SHIP_FLUSH_BYTES)
        #[arg(long)]
        flush_bytes: Option<usize>,
        /// Treat --since-lsn as inclusive (lsn >= N). Default is exclusive (> N).
        /// Shortcut for P1_SHIP_SINCE_INCLUSIVE=1.
        #[arg(long, default_value_t = false)]
        since_inclusive: bool,
        /// Compress stream: none|gzip|zstd (shortcut for P1_SHIP_COMPRESS)
        #[arg(long)]
        compress: Option<String>,
    },
    WalApply {
        /// Target DB path to apply incoming WAL stream from stdin
        #[arg(long)]
        path: PathBuf,
        /// Decompress input: none|gzip|zstd (shortcut for P1_APPLY_DECOMPRESS)
        #[arg(long)]
        decompress: Option<String>,
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

    // Snapshot/Backup (Phase 1: one-shot)
    Backup {
        /// DB path
        #[arg(long)]
        path: PathBuf,
        /// Output directory (will be created)
        #[arg(long)]
        out: PathBuf,
        /// Incremental backup: include only pages with lsn in (since_lsn, snapshot_lsn]
        #[arg(long)]
        since_lsn: Option<u64>,
    },

    // Restore (Phase 1) — новый CLI
    Restore {
        /// Destination DB path (will be created if missing)
        #[arg(long)]
        path: PathBuf,
        /// Backup directory (produced by `quiverdb backup`)
        #[arg(long)]
        from: PathBuf,
        /// Verify restored DB with strict check
        #[arg(long, default_value_t = false)]
        verify: bool,
    },

    // v0.8: check (CRC scan, overflow reachability) + strict/json режимы
    Check {
        #[arg(long)]
        path: PathBuf,
        /// Strict mode: non-zero exit if problems detected (dir error, CRC/IO issues, orphan overflow)
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

    // v0.9: metrics (snapshot/reset) + JSON
    Metrics {
        /// JSON output: one-line JSON with metrics snapshot
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    MetricsReset,

    // -------- Phase 2: persisted snapshots registry (view) --------
    /// List persisted snapshots from .snapshots/registry.json (best-effort).
    Snapshots {
        /// DB path (root)
        #[arg(long)]
        path: PathBuf,
        /// JSON output (prints registry.json contents as-is if present)
        #[arg(long, default_value_t = false)]
        json: bool,
    },

    // -------- Phase 2: remove persisted snapshot (dec_ref + delete sidecar) --------
    SnapRm {
        /// DB path (root)
        #[arg(long)]
        path: PathBuf,
        /// Snapshot id (directory name under .snapshots/)
        #[arg(long)]
        id: String,
    },

    // -------- Phase 2: compact snapstore (.snapstore/store.bin) --------
    SnapCompact {
        /// DB path (root)
        #[arg(long)]
        path: PathBuf,
        /// JSON output for report
        #[arg(long, default_value_t = false)]
        json: bool,
    },

    // -------- New: WAL checkpoint (manual truncate to header) --------
    /// Truncate WAL to header (manual checkpoint). Requires data_fsync=true.
    Checkpoint {
        /// DB path (root)
        #[arg(long)]
        path: PathBuf,
    },
}

pub fn run() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        // ------- common low-level -------
        Cmd::Init { path, page_size } => admin::cmd_init(path, page_size),
        Cmd::Status { path, json } => {
            if json {
                std::env::set_var("P1_STATUS_JSON", "1");
            }
            admin::cmd_status(path)
        }
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
        Cmd::DbStats { path, json } => {
            if json {
                std::env::set_var("P1_DBSTATS_JSON", "1");
            }
            db_cli::cmd_db_stats(path)
        }
        Cmd::DbScan { path, prefix, json } => db_cli::cmd_db_scan(path, prefix, json),

        // ------- CDC -------
        Cmd::WalTail { path, follow } => cdc::cmd_wal_tail(path, follow),
        Cmd::WalShip {
            path,
            follow,
            since_lsn,
            sink,
            flush_every,
            flush_bytes,
            since_inclusive,
            compress,
        } => {
            // ENV для ship‑подсистемы (batching, inclusive bound, compression)
            if let Some(n) = flush_every {
                std::env::set_var("P1_SHIP_FLUSH_EVERY", n.to_string());
            }
            if let Some(b) = flush_bytes {
                std::env::set_var("P1_SHIP_FLUSH_BYTES", b.to_string());
            }
            if since_inclusive {
                std::env::set_var("P1_SHIP_SINCE_INCLUSIVE", "1");
            }
            if let Some(cmp) = compress {
                std::env::set_var("P1_SHIP_COMPRESS", cmp.to_ascii_lowercase());
            }
            cdc::cmd_wal_ship_ext(path, follow, since_lsn, sink)
        }
        Cmd::WalApply { path, decompress } => {
            if let Some(dc) = decompress {
                std::env::set_var("P1_APPLY_DECOMPRESS", dc.to_ascii_lowercase());
            }
            cdc::cmd_wal_apply(path)
        }
        Cmd::CdcLastLsn { path } => cdc::cmd_cdc_last_lsn(path),
        Cmd::CdcRecord { path, out, from_lsn, to_lsn } =>
            cdc::cmd_cdc_record(path, out, from_lsn, to_lsn),
        Cmd::CdcReplay { path, input, from_lsn, to_lsn } =>
            cdc::cmd_cdc_replay(path, input, from_lsn, to_lsn),

        // ------- Snapshot/Backup -------
        Cmd::Backup { path, out, since_lsn } => {
            use crate::Db;
            let mut db = Db::open(&path)?;
            let mut snap = db.snapshot_begin()?;
            crate::backup::backup_to_dir(&db, &snap, &out, since_lsn)?;
            db.snapshot_end(&mut snap)?;
            println!("Backup completed at {}", out.display());
            Ok(())
        }

        // ------- Restore -------
        Cmd::Restore { path, from, verify } => {
            crate::backup::restore_from_dir(&path, &from)?;
            if verify {
                // strict + human output (json=false)
                admin::cmd_check_strict_json(path.clone(), true, false)?;
            }
            println!("Restore completed to {}", path.display());
            Ok(())
        }

        // ------- v0.8: check + strict/json -------
        Cmd::Check { path, strict, json } => admin::cmd_check_strict_json(path, strict, json),

        // ------- v0.9: repair -------
        Cmd::Repair { path } => admin::cmd_repair(path),

        // ------- v0.9: metrics -------
        Cmd::Metrics { json } => admin::cmd_metrics_json(json),
        Cmd::MetricsReset => admin::cmd_metrics_reset(),

        // ------- Phase 2: persisted snapshots registry (view) -------
        Cmd::Snapshots { path, json } => cmd_snapshots_list(path, json),

        // ------- Phase 2: remove persisted snapshot -------
        Cmd::SnapRm { path, id } => cmd_snapshot_rm(path, id),

        // ------- Phase 2: compact snapstore -------
        Cmd::SnapCompact { path, json } => cmd_snap_compact(path, json),

        // ------- New: WAL checkpoint -------
        Cmd::Checkpoint { path } => {
            use anyhow::anyhow;
            use crate::Db;
            // Open writer to ensure exclusive lock and run replay if needed.
            let db = Db::open(&path)?;
            if !db.cfg.data_fsync {
                return Err(anyhow!(
                    "checkpoint requires data_fsync=true (P1_DATA_FSYNC=1); \
                     enable it to ensure segments are durable before truncating WAL"
                ));
            }
            let mut wal = crate::wal::Wal::open_for_append(&path)?;
            wal.truncate_to_header()?;
            println!("Checkpoint: truncated WAL to header at {}", path.display());
            drop(db);
            Ok(())
        }
    }
}

// -------- Phase 2 helper: list persisted snapshots registry --------

fn cmd_snapshots_list(path: PathBuf, json: bool) -> Result<()> {
    let reg_path = path.join(".snapshots").join("registry.json");
    if !reg_path.exists() {
        if json {
            println!("{{\"entries\":[]}}");
        } else {
            println!("(no persisted snapshots)");
        }
        return Ok(());
    }

    let bytes = std::fs::read(&reg_path)
        .map_err(|e| anyhow::anyhow!("read {}: {}", reg_path.display(), e))?;

    if json {
        // Печатаем как есть
        match std::str::from_utf8(&bytes) {
            Ok(s) => println!("{}", s),
            Err(_) => {
                // На всякий: если не UTF-8, печатаем безопасно через base64
                let b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
                println!("{{\"raw_base64\":\"{}\"}}", b64);
            }
        }
        return Ok(());
    }

    // Human-readable вывод
    let v: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|e| anyhow::anyhow!("parse registry.json: {}", e))?;

    let entries = v.get("entries").and_then(|x| x.as_array()).cloned().unwrap_or_default();
    if entries.is_empty() {
        println!("(no persisted snapshots)");
        return Ok(());
    }

    println!("Persisted snapshots ({}):", entries.len());
    for e in entries {
        let id = e.get("id").and_then(|x| x.as_str()).unwrap_or("-");
        let lsn = e.get("lsn").and_then(|x| x.as_u64()).unwrap_or(0);
        let ended = e.get("ended").and_then(|x| x.as_bool()).unwrap_or(false);
        let ts = e.get("ts_nanos").and_then(|x| x.as_u64()).unwrap_or(0);
        println!("  {}  lsn={}  ended={}  ts_nanos={}", id, lsn, ended, ts);
    }
    Ok(())
}

// -------- Phase 2: remove persisted snapshot (dec_ref + delete sidecar) --------

fn cmd_snapshot_rm(path: PathBuf, id: String) -> Result<()> {
    use anyhow::anyhow;
    use byteorder::{ByteOrder, LittleEndian};
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom};
    use crate::snapshots::store::SnapStore;
    use crate::meta::read_meta;

    // 1) Check sidecar dir
    let snap_dir = path.join(".snapshots").join(&id);
    if !snap_dir.exists() {
        return Err(anyhow!("snapshot sidecar not found: {}", snap_dir.display()));
    }

    // 2) Parse hashindex.bin -> unique hashes
    let hash_path = snap_dir.join("hashindex.bin");
    let mut hashes: std::collections::HashSet<u64> = std::collections::HashSet::new();
    if hash_path.exists() {
        let mut f = OpenOptions::new().read(true).open(&hash_path)
            .map_err(|e| anyhow!("open {}: {}", hash_path.display(), e))?;
        let len = f.metadata()?.len();
        const REC: u64 = 8 + 8 + 8; // [page_id u64][hash u64][page_lsn u64]
        let mut pos = 0u64;
        let mut buf = vec![0u8; REC as usize];
        while pos + REC <= len {
            f.seek(SeekFrom::Start(pos))?;
            f.read_exact(&mut buf)?;
            let hash = LittleEndian::read_u64(&buf[8..16]);
            hashes.insert(hash);
            pos += REC;
        }
    }

    // 3) DecRef in SnapStore (best-effort)
    if !hashes.is_empty() {
        let ps = read_meta(&path)?.page_size;
        if let Ok(mut ss) = SnapStore::open(&path, ps) {
            for h in hashes {
                let _ = ss.dec_ref(h);
            }
        }
    }

    // 4) Remove sidecar directory
    let _ = std::fs::remove_dir_all(&snap_dir);

    // 5) Mark ended in registry.json (best-effort)
    let reg_path = path.join(".snapshots").join("registry.json");
    if reg_path.exists() {
        if let Ok(bytes) = std::fs::read(&reg_path) {
            if let Ok(mut v) = serde_json::from_slice::<serde_json::Value>(&bytes) {
                if let Some(arr) = v.get_mut("entries").and_then(|x| x.as_array_mut()) {
                    for ent in arr.iter_mut() {
                        if ent.get("id").and_then(|x| x.as_str()) == Some(&id) {
                            ent.as_object_mut().map(|m| m.insert("ended".to_string(), serde_json::Value::Bool(true)));
                            break;
                        }
                    }
                    if let Ok(s) = serde_json::to_string_pretty(&v) {
                        let _ = std::fs::write(&reg_path, s.as_bytes());
                    }
                }
            }
        }
    }

    println!("Removed snapshot '{}' (sidecar deleted, snapstore refs decremented best-effort)", id);
    Ok(())
}

// -------- Phase 2: compact snapstore (.snapstore/store.bin) --------

fn cmd_snap_compact(path: PathBuf, json: bool) -> Result<()> {
    use crate::snapshots::store::SnapStore;
    use crate::meta::read_meta;

    // Открыть snapstore
    let ps = read_meta(&path)?.page_size;
    let mut ss = SnapStore::open(&path, ps)?;

    let rep = ss.compact()?;
    if json {
        println!(
            "{{\"before\":{},\"after\":{},\"kept\":{},\"dropped\":{}}}",
            rep.before, rep.after, rep.kept, rep.dropped
        );
    } else {
        println!("SnapStore compact:");
        println!("  before  = {} bytes", rep.before);
        println!("  after   = {} bytes", rep.after);
        println!("  kept    = {} frame(s)", rep.kept);
        println!("  dropped = {} frame(s)", rep.dropped);
    }
    Ok(())
}