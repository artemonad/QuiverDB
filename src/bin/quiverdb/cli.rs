use clap::{Parser, Subcommand};
use std::path::PathBuf;

/// Минимальный CLI для QuiverDB 2.x (модульная версия)
#[derive(Parser, Debug)]
#[command(name = "quiverdb", version, about = "QuiverDB 2.x CLI")]
pub struct Cli {
    #[command(subcommand)]
    pub cmd: Cmd,
}

#[derive(Subcommand, Debug)]
pub enum Cmd {
    /// Initialize a new DB (meta v4 + dir v2)
    Init {
        #[arg(long)]
        path: PathBuf,
        #[arg(long, default_value_t = 65536)]
        page_size: u32,
        #[arg(long, default_value_t = 128)]
        buckets: u32,
    },

    /// Put key/value (value as string or from file)
    Put {
        #[arg(long)]
        path: PathBuf,
        #[arg(long)]
        key: String,
        /// Value as a literal string (UTF‑8). Ignored if --value-file is set.
        #[arg(long)]
        value: Option<String>,
        /// Read value bytes from a file
        #[arg(long)]
        value_file: Option<PathBuf>,
    },

    /// Get key
    Get {
        #[arg(long)]
        path: PathBuf,
        #[arg(long)]
        key: String,
        /// Optional file to write raw value into
        #[arg(long)]
        out: Option<PathBuf>,
    },

    /// Quick existence check (Bloom fast‑path if fresh)
    Exists {
        #[arg(long)]
        path: PathBuf,
        #[arg(long)]
        key: String,
    },

    /// Delete key (tombstone write)
    Del {
        #[arg(long)]
        path: PathBuf,
        #[arg(long)]
        key: String,
    },

    /// Batch operations from JSON (single WAL batch commit)
    ///
    /// JSON формат (массив объектов):
    /// [
    ///   {"op":"put","key":"alpha","value":"1"},
    ///   {"op":"put","key":"bin","value":"hex:deadbeef"},
    ///   {"op":"put","key":"f","value":"@./file.bin"},
    ///   {"op":"del","key":"alpha"}
    /// ]
    Batch {
        #[arg(long)]
        path: PathBuf,
        /// JSON-файл с операциями
        #[arg(long)]
        ops_file: Option<PathBuf>,
        /// JSON-строка с операциями (если ops_file не задан)
        #[arg(long)]
        ops_json: Option<String>,
    },

    /// Scan with optional prefix. --json prints JSON array (or JSONL with --stream).
    Scan {
        #[arg(long)]
        path: PathBuf,
        /// Optional UTF-8 prefix
        #[arg(long)]
        prefix: Option<String>,
        /// JSON output (array or JSONL with --stream)
        #[arg(long, default_value_t = false)]
        json: bool,
        /// Stream results (JSONL if --json, otherwise plain lines)
        #[arg(long, default_value_t = false)]
        stream: bool,
    },

    /// Print meta/dir/metrics summary
    ///
    /// Пример:
    ///   quiverdb status --path ./db
    ///   quiverdb status --path ./db --json
    Status {
        #[arg(long)]
        path: PathBuf,
        /// JSON output (single object)
        #[arg(long, default_value_t = false)]
        json: bool,
    },

    /// Sweep orphan OVERFLOW pages (writer-only)
    Sweep {
        #[arg(long)]
        path: PathBuf,
    },

    /// Doctor: scan all pages with CRC/IO checks (use --json for JSON)
    Doctor {
        #[arg(long)]
        path: PathBuf,
        #[arg(long, default_value_t = false)]
        json: bool,
    },

    /// Truncate WAL to header (exclusive lock required)
    ///
    /// Примечания:
    /// - Требует эксклюзивного lock на <root>/LOCK (как writer).
    /// - Безопасно вызывать при отсутствии активного writer’а.
    Checkpoint {
        #[arg(long)]
        path: PathBuf,
    },

    /// Compact chains: rebuild tail-wins pages without tombstones/expired.
    ///
    /// По умолчанию компактует всю БД. Можно указать один бакет:
    ///   quiverdb compact --path ./db --bucket 42
    /// Формат вывода: текст или JSON (--json).
    Compact {
        #[arg(long)]
        path: PathBuf,
        /// Optional bucket number to compact. If omitted, compacts all buckets.
        #[arg(long)]
        bucket: Option<u32>,
        /// JSON output
        #[arg(long, default_value_t = false)]
        json: bool,
    },

    /// Vacuum: compact all + sweep orphan OVERFLOW (writer-only)
    ///
    /// Выполняет перестройку цепочек и затем освобождает сиротские OVERFLOW страницы.
    Vacuum {
        #[arg(long)]
        path: PathBuf,
        /// JSON output
        #[arg(long, default_value_t = false)]
        json: bool,
    },

    /// Bloom side-car maintenance: rebuild bloom.bin
    ///
    /// По умолчанию перестраивает все бакеты.
    /// Можно указать конкретный бакет и параметры фильтра.
    ///   quiverdb bloom --path ./db --bucket 10 --bpb 4096 --k 6
    Bloom {
        #[arg(long)]
        path: PathBuf,
        /// Optional bucket number. If omitted, rebuilds all buckets.
        #[arg(long)]
        bucket: Option<u32>,
        /// Bytes per bucket (default 4096).
        #[arg(long)]
        bpb: Option<u32>,
        /// Number of hash functions (default 6).
        #[arg(long)]
        k: Option<u32>,
    },

    /// TDE operations
    ///
    /// Пример:
    ///   quiverdb tde-rotate --path ./db --kid mykid-v2
    ///
    /// Выполняет ротацию KID: включает TDE (если выключен), проверяет ключ из ENV
    /// и добавляет новую эпоху (since_lsn, kid) в журнал.
    TdeRotate {
        #[arg(long)]
        path: PathBuf,
        #[arg(long)]
        kid: String,
    },

    /// Auto maintenance: compact limited number of buckets and optional sweep orphan OVERFLOW.
    ///
    /// Примеры:
    ///   quiverdb auto-maint --path ./db --max-buckets 32 --sweep
    ///   quiverdb auto-maint --path ./db --max-buckets 16 --json
    AutoMaint {
        #[arg(long)]
        path: PathBuf,
        /// Максимум непустых бакетов для компактации в одном прогоне
        #[arg(long, default_value_t = 32)]
        max_buckets: u32,
        /// Выполнить sweep сиротских OVERFLOW-страниц после компактации
        #[arg(long, default_value_t = false)]
        sweep: bool,
        /// JSON output
        #[arg(long, default_value_t = false)]
        json: bool,
    },

    // -------------------- NEW: CDC commands --------------------
    /// CDC apply: применить поток WAL (file:// или tcp+psk://).
    ///
    /// Примеры:
    ///   quiverdb cdc-apply --path ./follower --from file://./wal-stream.bin
    ///   quiverdb cdc-apply --path ./follower --from tcp+psk://127.0.0.1:9099
    CdcApply {
        /// Путь к целевой БД (follower).
        #[arg(long)]
        path: PathBuf,
        /// Источник WAL: file://<path> или tcp+psk://host:port
        #[arg(long)]
        from: String,
    },

    /// CDC ship: отправить WAL поток (file:// sink или tcp+psk://).
    ///
    /// Примеры:
    ///   quiverdb cdc-ship --path ./db --to file://./wal-stream.bin
    ///   quiverdb cdc-ship --path ./db --to tcp+psk://127.0.0.1:9099 --since-lsn 12345
    CdcShip {
        /// Путь к исходной БД (producer).
        #[arg(long)]
        path: PathBuf,
        /// Приёмник: file://<path> или tcp+psk://host:port
        #[arg(long)]
        to: String,
        /// Отправлять кадры с lsn > N (или >= N при ENV P1_SHIP_SINCE_INCLUSIVE=1)
        #[arg(long)]
        since_lsn: Option<u64>,
    },

    // -------------------- NEW: Snapshots (2.2) --------------------
    /// Snapshot: create persisted snapshot (.snapstore/ + manifest)
    ///
    /// Пример:
    ///   quiverdb snapshot-create --path ./db --message "baseline" --label prod --label v2
    SnapshotCreate {
        #[arg(long)]
        path: PathBuf,
        /// Optional message to store in manifest
        #[arg(long)]
        message: Option<String>,
        /// Labels (repeatable): --label tag1 --label tag2
        #[arg(long = "label")]
        label: Vec<String>,
        /// Optional parent snapshot id
        #[arg(long)]
        parent: Option<String>,
    },

    /// Snapshot: list manifests (ids)
    ///
    /// Пример:
    ///   quiverdb snapshot-list --path ./db
    ///   quiverdb snapshot-list --path ./db --json
    SnapshotList {
        #[arg(long)]
        path: PathBuf,
        /// JSON output (array of strings)
        #[arg(long, default_value_t = false)]
        json: bool,
    },

    /// Snapshot: inspect manifest by id
    ///
    /// Пример:
    ///   quiverdb snapshot-inspect --path ./db --id <snapshot_id>
    ///   quiverdb snapshot-inspect --path ./db --id <id> --json
    SnapshotInspect {
        #[arg(long)]
        path: PathBuf,
        #[arg(long)]
        id: String,
        /// JSON output
        #[arg(long, default_value_t = false)]
        json: bool,
    },

    /// Snapshot: restore DB from persisted snapshot (SnapStore + manifest v2)
    ///
    /// Примеры:
    ///   quiverdb snapshot-restore --path ./dst --id <snapshot_id>
    ///   quiverdb snapshot-restore --path ./dst --src ./source_db --id <snapshot_id> --verify
    SnapshotRestore {
        /// Куда восстановить БД
        #[arg(long)]
        path: PathBuf,
        /// Откуда брать SnapStore (.snapstore/{objects,manifests}). По умолчанию совпадает с --path.
        #[arg(long)]
        src: Option<PathBuf>,
        /// Идентификатор снапшота
        #[arg(long)]
        id: String,
        /// Включить проверку размеров страниц (равны page_size из манифеста)
        #[arg(long, default_value_t = false)]
        verify: bool,
    },

    /// NEW: Snapshot: delete persisted snapshot by id (dec-ref objects + remove manifest)
    ///
    /// Пример:
    ///   quiverdb snapshot-delete --path ./db --id <snapshot_id>
    SnapshotDelete {
        /// Корень БД, где находится SnapStore (учитывает P1_SNAPSTORE_DIR).
        #[arg(long)]
        path: PathBuf,
        /// Идентификатор снапшота, который нужно удалить.
        #[arg(long)]
        id: String,
    },
}

impl Cli {
    pub fn parse() -> Self {
        <Cli as Parser>::parse()
    }
}
