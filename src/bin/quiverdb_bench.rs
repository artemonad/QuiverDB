use anyhow::{anyhow, Context, Result};
use clap::{Parser, ValueEnum};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use QuiverDB::bloom::BloomSidecar;
use QuiverDB::db::Db;
use QuiverDB::dir::Directory;
use QuiverDB::meta::read_meta;

/// Простой детерминированный PRNG (SplitMix64).
/// Достаточен для бенчей; не криптостойкий.
#[derive(Clone)]
struct Rng64 {
    state: u64,
}
impl Rng64 {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }
    #[inline]
    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

/// Простой прогресс‑репорт на ~10 шагов.
struct Progress<'a> {
    name: &'a str,
    total: usize,
    step: usize,
    next: usize,
    start: Instant,
    enabled: bool,
}
impl<'a> Progress<'a> {
    fn new(name: &'a str, total: usize, enabled: bool) -> Self {
        let step = std::cmp::max(1, total / 10);
        Self {
            name,
            total,
            step,
            next: step,
            start: Instant::now(),
            enabled,
        }
    }
    fn bump(&mut self, cur: usize) {
        if !self.enabled {
            return;
        }
        if cur >= self.next || cur == self.total {
            let pct = (cur as f64 / self.total.max(1) as f64) * 100.0;
            let elapsed = self.start.elapsed().as_secs_f64();
            let tput = if elapsed > 0.0 { cur as f64 / elapsed } else { 0.0 };
            println!(
                "[{:>10}] {:>7} / {:<7} ({:>5.1}%) elapsed={:.2}s, tput={:.0} ops/s",
                self.name, cur, self.total, pct, elapsed, tput
            );
            self.next = cur.saturating_add(self.step);
        }
    }
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum Profile {
    Bench,
    Balanced,
    Strict,
}

/// QuiverDB micro-benchmark CLI
///
/// Примеры:
///   quiverdb_bench --path ./benchdb --clean --json --profile bench
///   quiverdb_bench --path ./benchdb --reuse --n 100000 --value-size 64 --profile balanced --tde
#[derive(Parser, Debug)]
#[command(name = "quiverdb_bench", version, about = "QuiverDB micro-bench CLI")]
struct Opt {
    /// DB root path for the benchmark
    #[arg(long)]
    path: PathBuf,

    /// Initialize a new DB (remove if exists)
    #[arg(long, default_value_t = false)]
    clean: bool,

    /// Reuse existing DB (do not init if exists)
    #[arg(long, default_value_t = false)]
    reuse: bool,

    /// Page size for init (bytes)
    #[arg(long, default_value_t = 65536)]
    page_size: u32,

    /// Buckets count for init (уменьшено по умолчанию, чтобы packing был эффективным)
    #[arg(long, default_value_t = 32)]
    buckets: u32,

    /// Total operations for point tests (load, gets, exists)
    #[arg(long, default_value_t = 100_000)]
    n: u64,

    /// Value size for small KV (bytes)
    #[arg(long, default_value_t = 128)]
    value_size: usize,

    /// Batch size for writer batch (0 = disable batch; use single put)
    /// Увеличено по умолчанию для packing: чем больше batch относительно buckets, тем лучше.
    #[arg(long, default_value_t = 4096)]
    batch_size: usize,

    /// Number of miss probes (for read-miss & exists-miss)
    #[arg(long, default_value_t = 50_000)]
    n_miss: u64,

    /// Build/refresh bloom before read-miss (enables fast-path)
    #[arg(long, default_value_t = true)]
    with_bloom: bool,

    /// Big value size (bytes), default = 2x page-size
    #[arg(long)]
    big_size: Option<usize>,

    /// Number of big values to write/read
    #[arg(long, default_value_t = 256)]
    big_n: u64,

    /// Random seed
    #[arg(long, default_value_t = 0xA1B2_C3D4_E5F6_7788)]
    seed: u64,

    /// JSON output
    #[arg(long, default_value_t = false)]
    json: bool,

    /// Show progress for each phase
    #[arg(long, default_value_t = true)]
    progress: bool,

    /// Override packing threshold in bytes (sets ENV P1_PACK_THRESHOLD_BYTES).
    /// If not set, default pack threshold is page_size/8 inside batch writer.
    #[arg(long)]
    pack_threshold: Option<usize>,

    /// Performance profile: bench (max speed), balanced (prod-like), strict (max durability)
    #[arg(long, value_enum, default_value_t = Profile::Bench)]
    profile: Profile,

    /// Enable TDE (AES‑GCM tag in trailer) to measure overhead.
    #[arg(long, default_value_t = false)]
    tde: bool,
}

#[derive(Debug, Clone)]
struct PhaseStats {
    name: String,
    ops: u64,
    elapsed: Duration,
    p50_ms: f64,
    p90_ms: f64,
    p99_ms: f64,
    tput_ops: f64,
}

#[derive(Debug, Clone)]
struct BenchReport {
    phases: Vec<PhaseStats>,
    // sizes
    segments_bytes: u64,
    wal_bytes: u64,
    // meta/dir info
    page_size: u32,
    buckets: u32,
    next_page_id: u64,
    last_lsn: u64,
    // metrics snapshot
    metrics: QuiverDB::metrics::MetricsSnapshot,
    // NEW: effective packing threshold (if explicitly set)
    pack_threshold: Option<usize>,
}

fn main() {
    if let Err(e) = run() {
        eprintln!("bench error: {:#}", e);
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let mut opt = Opt::parse();

    apply_profile_env(opt.profile);

    if opt.tde {
        enable_tde_env()?;
    }

    if opt.clean && opt.reuse {
        return Err(anyhow!("--clean and --reuse are mutually exclusive"));
    }

    // Автоподстройка batch_size: минимально buckets * 16 для эффективного packing.
    let auto_bs_min = (opt.buckets as usize) * 16;
    if opt.batch_size < auto_bs_min {
        println!(
            "[bench] batch_size {} is too small for {} buckets; raising to {} for packing efficiency",
            opt.batch_size, opt.buckets, auto_bs_min
        );
        opt.batch_size = auto_bs_min;
    }

    if let Some(pt) = opt.pack_threshold {
        std::env::set_var("P1_PACK_THRESHOLD_BYTES", pt.to_string());
    }

    // Сброс метрик перед запуском — чтобы отчёт был только про текущий прогон.
    QuiverDB::metrics::reset();

    prepare_db(&opt)?;

    // Build keysets
    let mut rng = Rng64::new(opt.seed);
    let n = opt.n as usize;
    let mut keys: Vec<Vec<u8>> = Vec::with_capacity(n);
    for i in 0..n {
        let k = format!("k-{:016x}-{:08x}", rng.next_u64(), i as u32);
        keys.push(k.into_bytes());
    }

    // Miss keys
    let n_miss = opt.n_miss as usize;
    let mut miss_keys: Vec<Vec<u8>> = Vec::with_capacity(n_miss);
    for i in 0..n_miss {
        let k = format!("m-{:016x}-{:08x}", rng.next_u64(), i as u32);
        miss_keys.push(k.into_bytes());
    }

    // Values
    let val = vec![0xAB; opt.value_size];

    let mut phases: Vec<PhaseStats> = Vec::new();

    // Phase A: load (put)
    if opt.batch_size > 0 {
        println!("==> Phase: put_batch ({} keys, batch={})", keys.len(), opt.batch_size);
        phases.push(phase_put_batch(&opt, &keys, &val)?);
    } else {
        println!("==> Phase: put_single ({} keys)", keys.len());
        phases.push(phase_put_single(&opt, &keys, &val)?);
    }

    // Phase B: read hits (get), random order
    println!("==> Phase: get_hits ({} keys, random order)", keys.len());
    phases.push(phase_get_hits(&opt, &keys)?);

    // Phase C: Bloom rebuild (optional) + read miss
    if opt.with_bloom {
        println!("==> Phase: bloom_rebuild (all buckets)");
        phase_bloom_rebuild(&opt)?;
    }
    println!("==> Phase: get_miss ({} keys)", miss_keys.len());
    phases.push(phase_get_miss(&opt, &miss_keys)?);

    // Phase D: exists hits/miss
    println!("==> Phase: exists_hits ({} keys)", keys.len());
    phases.push(phase_exists_hits(&opt, &keys)?);
    println!("==> Phase: exists_miss ({} keys)", miss_keys.len());
    phases.push(phase_exists_miss(&opt, &miss_keys)?);

    // Phase E: big values (OVERFLOW)
    let big_size = opt.big_size.unwrap_or((opt.page_size as usize) * 2);
    println!("==> Phase: big_put ({} items, size={} B)", opt.big_n, big_size);
    phases.push(phase_big_put(&opt, big_size)?);
    println!("==> Phase: big_get ({} items)", opt.big_n);
    phases.push(phase_big_get(&opt, big_size)?);

    // Report: sizes, meta/dir, metrics snapshot
    let (segments_bytes, wal_bytes) = sizes(&opt.path)?;
    let meta = read_meta(&opt.path)?;
    let dir = Directory::open(&opt.path)?;
    let snap = QuiverDB::metrics::snapshot();

    let report = BenchReport {
        phases,
        segments_bytes,
        wal_bytes,
        page_size: meta.page_size,
        buckets: dir.bucket_count,
        next_page_id: meta.next_page_id,
        last_lsn: meta.last_lsn,
        metrics: snap,
        pack_threshold: opt.pack_threshold,
    };

    if opt.json {
        print_report_json(&report)?;
    } else {
        print_report_human(&report)?;
    }

    Ok(())
}

// ---------- profile ENV ----------

fn apply_profile_env(profile: Profile) {
    match profile {
        Profile::Bench => {
            println!("[bench] profile=bench: fsync(WAL)=off, fsync(data)=off, checksum=off, dir=inplace, page_cache=8192");
            std::env::set_var("P1_WAL_DISABLE_FSYNC", "1");
            std::env::set_var("P1_WAL_COALESCE_MS", "0");
            std::env::set_var("P1_DATA_FSYNC", "0");
            std::env::set_var("P1_DIR_ATOMIC", "0");
            std::env::set_var("P1_DIR_FSYNC", "0");
            std::env::set_var("P1_PAGE_CHECKSUM", "0");
            if std::env::var("P1_PAGE_CACHE_PAGES").is_err() {
                std::env::set_var("P1_PAGE_CACHE_PAGES", "8192");
            }
        }
        Profile::Balanced => {
            println!("[bench] profile=balanced: fsync(WAL)=on, fsync(data)=off, checksum=on, dir=atomic, page_cache=4096");
            std::env::set_var("P1_WAL_COALESCE_MS", "0");
            std::env::set_var("P1_DATA_FSYNC", "0");
            std::env::set_var("P1_DIR_ATOMIC", "1");
            if std::env::var("P1_PAGE_CACHE_PAGES").is_err() {
                std::env::set_var("P1_PAGE_CACHE_PAGES", "4096");
            }
            std::env::remove_var("P1_WAL_DISABLE_FSYNC");
        }
        Profile::Strict => {
            println!("[bench] profile=strict: fsync(WAL)=on, fsync(data)=on, checksum=on, dir=atomic, page_cache=0");
            std::env::set_var("P1_WAL_COALESCE_MS", "0");
            std::env::set_var("P1_DATA_FSYNC", "1");
            std::env::set_var("P1_DIR_ATOMIC", "1");
            std::env::remove_var("P1_WAL_DISABLE_FSYNC");
            std::env::remove_var("P1_PAGE_CHECKSUM"); // пусть будет по умолчанию (включено)
            if std::env::var("P1_PAGE_CACHE_PAGES").is_err() {
                std::env::set_var("P1_PAGE_CACHE_PAGES", "0");
            }
        }
    }
}

/// Включить TDE (AES‑GCM tag) для бенча. Если ключ не задан — установим тестовый.
fn enable_tde_env() -> Result<()> {
    println!("[bench] TDE: enabling AES-GCM trailer tag (P1_TDE_ENABLED=1)");
    std::env::set_var("P1_TDE_ENABLED", "1");
    let has_hex = std::env::var("P1_TDE_KEY_HEX").ok().filter(|s| !s.is_empty()).is_some();
    let has_b64 = std::env::var("P1_TDE_KEY_BASE64").ok().filter(|s| !s.is_empty()).is_some();
    if !has_hex && !has_b64 {
        // Тестовый 32-байтовый ключ (детерминированный)
        // 0x11 повторён 32 раза — достаточно для оценки overhead
        println!("[bench] TDE: installing test key via P1_TDE_KEY_HEX");
        let key_hex = "11".repeat(32);
        std::env::set_var("P1_TDE_KEY_HEX", key_hex);
        std::env::set_var("P1_TDE_KID", "bench");
    }
    Ok(())
}

// ---------- phases ----------

fn phase_put_single(opt: &Opt, keys: &[Vec<u8>], val: &[u8]) -> Result<PhaseStats> {
    let mut db = Db::open(&opt.path)?;
    let mut lat = Vec::with_capacity(keys.len());
    let mut prog = Progress::new("put_single", keys.len(), opt.progress);
    let start = Instant::now();
    for (i, k) in keys.iter().enumerate() {
        let t0 = Instant::now();
        db.put(&k, val)?;
        lat.push(t0.elapsed());
        prog.bump(i + 1);
    }
    let elapsed = start.elapsed();
    let stats = stats("put_single", keys.len() as u64, elapsed, &mut lat);
    print_phase_summary(&stats);
    Ok(stats)
}

fn phase_put_batch(opt: &Opt, keys: &[Vec<u8>], val: &[u8]) -> Result<PhaseStats> {
    let mut db = Db::open(&opt.path)?;
    let mut lat = Vec::new();
    let bs = opt.batch_size.max(1);
    let mut prog = Progress::new("put_batch", keys.len(), opt.progress);
    let start = Instant::now();
    for (i, chunk) in keys.chunks(bs).enumerate() {
        let t0 = Instant::now();
        db.batch(|b| {
            for k in chunk {
                b.put(k, val)?;
            }
            Ok(())
        })?;
        lat.push(t0.elapsed());
        prog.bump(std::cmp::min((i + 1) * bs, keys.len()));
    }
    let elapsed = start.elapsed();
    let stats = stats("put_batch", keys.len() as u64, elapsed, &mut lat);
    print_phase_summary(&stats);
    Ok(stats)
}

fn phase_get_hits(opt: &Opt, keys: &[Vec<u8>]) -> Result<PhaseStats> {
    // случайный порядок (Fisher–Yates)
    let mut order: Vec<usize> = (0..keys.len()).collect();
    let mut rng = Rng64::new(opt.seed ^ 0xDEADBEEFCAFEBABE);
    for i in (1..order.len()).rev() {
        let j = (rng.next_u64() as usize) % (i + 1);
        order.swap(i, j);
    }

    let db = Db::open_ro(&opt.path)?;
    let mut lat = Vec::with_capacity(keys.len());
    let mut prog = Progress::new("get_hits", keys.len(), opt.progress);
    let start = Instant::now();
    for (n, idx) in order.into_iter().enumerate() {
        let t0 = Instant::now();
        let got = db.get(&keys[idx])?;
        if got.is_none() {
            return Err(anyhow!("get_hits: missing key at idx {}", idx));
        }
        lat.push(t0.elapsed());
        prog.bump(n + 1);
    }
    let elapsed = start.elapsed();
    let stats = stats("get_hits", keys.len() as u64, elapsed, &mut lat);
    print_phase_summary(&stats);
    Ok(stats)
}

fn phase_bloom_rebuild(opt: &Opt) -> Result<()> {
    let db_ro = Db::open_ro(&opt.path)?;
    let mut sidecar = BloomSidecar::open_or_create_for_db(&db_ro, 4096, 6)?;
    println!("    [bloom] rebuilding ...");
    let t0 = Instant::now();
    sidecar.rebuild_all(&db_ro)?;
    println!("    [bloom] done in {:.2}s", t0.elapsed().as_secs_f64());
    Ok(())
}

fn phase_get_miss(opt: &Opt, miss_keys: &[Vec<u8>]) -> Result<PhaseStats> {
    let db = Db::open_ro(&opt.path)?;
    let mut lat = Vec::with_capacity(miss_keys.len());
    let mut prog = Progress::new("get_miss", miss_keys.len(), opt.progress);
    let start = Instant::now();
    for (i, k) in miss_keys.iter().enumerate() {
        let t0 = Instant::now();
        let _ = db.get(k)?; // ожидаем None
        lat.push(t0.elapsed());
        prog.bump(i + 1);
    }
    let elapsed = start.elapsed();
    let stats = stats("get_miss", miss_keys.len() as u64, elapsed, &mut lat);
    print_phase_summary(&stats);
    Ok(stats)
}

fn phase_exists_hits(opt: &Opt, keys: &[Vec<u8>]) -> Result<PhaseStats> {
    let db = Db::open_ro(&opt.path)?;
    let mut lat = Vec::with_capacity(keys.len());
    let mut prog = Progress::new("exists_hits", keys.len(), opt.progress);
    let start = Instant::now();
    for (i, k) in keys.iter().enumerate() {
        let t0 = Instant::now();
        let present = db.exists(k)?;
        if !present {
            return Err(anyhow!("exists_hits: false for present key"));
        }
        lat.push(t0.elapsed());
        prog.bump(i + 1);
    }
    let elapsed = start.elapsed();
    let stats = stats("exists_hits", keys.len() as u64, elapsed, &mut lat);
    print_phase_summary(&stats);
    Ok(stats)
}

fn phase_exists_miss(opt: &Opt, miss_keys: &[Vec<u8>]) -> Result<PhaseStats> {
    let db = Db::open_ro(&opt.path)?;
    let mut lat = Vec::with_capacity(miss_keys.len());
    let mut prog = Progress::new("exists_miss", miss_keys.len(), opt.progress);
    let start = Instant::now();
    for (i, k) in miss_keys.iter().enumerate() {
        let t0 = Instant::now();
        let present = db.exists(k)?;
        if present {
            return Err(anyhow!("exists_miss: true for missing key"));
        }
        lat.push(t0.elapsed());
        prog.bump(i + 1);
    }
    let elapsed = start.elapsed();
    let stats = stats("exists_miss", miss_keys.len() as u64, elapsed, &mut lat);
    print_phase_summary(&stats);
    Ok(stats)
}

fn phase_big_put(opt: &Opt, big_size: usize) -> Result<PhaseStats> {
    let mut db = Db::open(&opt.path)?;
    let mut lat = Vec::with_capacity(opt.big_n as usize);
    let mut prog = Progress::new("big_put", opt.big_n as usize, opt.progress);
    let start = Instant::now();
    for i in 0..opt.big_n {
        let key = format!("big-{:08x}", i).into_bytes();
        let val = vec![0xCD; big_size];
        let t0 = Instant::now();
        db.put(&key, &val)?;
        lat.push(t0.elapsed());
        prog.bump((i + 1) as usize);
    }
    let elapsed = start.elapsed();
    let stats = stats("big_put", opt.big_n, elapsed, &mut lat);
    print_phase_summary(&stats);
    Ok(stats)
}

fn phase_big_get(opt: &Opt, _big_size: usize) -> Result<PhaseStats> {
    let db = Db::open_ro(&opt.path)?;
    let mut lat = Vec::with_capacity(opt.big_n as usize);
    let mut prog = Progress::new("big_get", opt.big_n as usize, opt.progress);
    let start = Instant::now();
    for i in 0..opt.big_n {
        let key = format!("big-{:08x}", i).into_bytes();
        let t0 = Instant::now();
        let got = db.get(&key)?;
        if got.is_none() {
            return Err(anyhow!("big_get: missing big value at {}", i));
        }
        lat.push(t0.elapsed());
        prog.bump((i + 1) as usize);
    }
    let elapsed = start.elapsed();
    let stats = stats("big_get", opt.big_n, elapsed, &mut lat);
    print_phase_summary(&stats);
    Ok(stats)
}

// ---------- helpers ----------

fn print_phase_summary(p: &PhaseStats) {
    println!(
        "    {:>10} done: ops={} elapsed={:.3}s, tput={:.0} ops/s, p50={:.3}ms p90={:.3}ms p99={:.3}ms",
        p.name, p.ops, p.elapsed.as_secs_f64(), p.tput_ops, p.p50_ms, p.p90_ms, p.p99_ms
    );
}

fn stats(name: &str, ops: u64, elapsed: Duration, lat: &mut [Duration]) -> PhaseStats {
    lat.sort_unstable();
    let to_ms = |d: Duration| d.as_secs_f64() * 1000.0;
    let p = |q: f64| -> f64 {
        if lat.is_empty() {
            return 0.0;
        }
        let idx = ((lat.len() as f64 - 1.0) * q).round() as usize;
        to_ms(lat[idx])
    };
    let p50 = p(0.50);
    let p90 = p(0.90);
    let p99 = p(0.99);
    let tput = if elapsed.as_secs_f64() > 0.0 {
        ops as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };
    PhaseStats {
        name: name.to_string(),
        ops,
        elapsed,
        p50_ms: p50,
        p90_ms: p90,
        p99_ms: p99,
        tput_ops: tput,
    }
}

fn sizes(root: &Path) -> Result<(u64, u64)> {
    let mut seg = 0u64;
    let mut wal = 0u64;
    if root.exists() {
        for e in fs::read_dir(root)? {
            let p = e?.path();
            if let Ok(md) = p.metadata() {
                if md.is_file() {
                    let name = p.file_name().and_then(|os| os.to_str()).unwrap_or("");
                    if name.starts_with("data-") && name.ends_with(".p2seg") {
                        seg += md.len();
                    } else if name == "wal-000001.log" {
                        wal += md.len();
                    }
                }
            }
        }
    }
    Ok((seg, wal))
}

fn prepare_db(opt: &Opt) -> Result<()> {
    if opt.clean && opt.path.exists() {
        fs::remove_dir_all(&opt.path).with_context(|| format!("remove {}", opt.path.display()))?;
    }
    if !opt.reuse {
        if !opt.path.exists() {
            fs::create_dir_all(&opt.path)?;
        }
        // init only if meta not present
        let meta_path = opt.path.join("meta");
        if !meta_path.exists() {
            QuiverDB::Db::init(&opt.path, opt.page_size, opt.buckets)?;
        }
    }
    Ok(())
}

fn print_report_human(r: &BenchReport) -> Result<()> {
    println!("QuiverDB bench report:");
    println!("  page_size    = {}", r.page_size);
    println!("  buckets      = {}", r.buckets);
    println!("  next_page_id = {}", r.next_page_id);
    println!("  last_lsn     = {}", r.last_lsn);
    if let Some(pt) = r.pack_threshold {
        println!("  pack_threshold = {} bytes (via CLI)", pt);
    }
    println!("  segments     = {} bytes", r.segments_bytes);
    println!("  wal          = {} bytes", r.wal_bytes);
    println!("Phases:");
    for p in &r.phases {
        println!(
            "  {:>12}: ops={} elapsed={:.3}s tput={:.0} ops/s p50={:.3}ms p90={:.3}ms p99={:.3}ms",
            p.name,
            p.ops,
            p.elapsed.as_secs_f64(),
            p.tput_ops,
            p.p50_ms,
            p.p90_ms,
            p.p99_ms
        );
    }
    let m = &r.metrics;
    println!("Metrics snapshot:");
    println!("  wal_appends_total       = {}", m.wal_appends_total);
    println!("  wal_fsync_calls         = {}", m.wal_fsync_calls);
    println!("  wal_avg_batch_pages     = {:.2}", m.avg_wal_batch_pages());
    println!("  page_cache_hits         = {}", m.page_cache_hits);
    println!("  page_cache_misses       = {}", m.page_cache_misses);
    println!(
        "  bloom tests/neg/pos/skip= {}/{}/{}/{}",
        m.bloom_tests, m.bloom_negative, m.bloom_positive, m.bloom_skipped_stale
    );
    // NEW: packing metrics
    println!("  pack_pages              = {}", m.pack_pages);
    println!("  pack_records            = {}", m.pack_records);
    println!("  pack_pages_single       = {}", m.pack_pages_single);
    println!("  pack_avg_records/page   = {:.2}", m.avg_pack_records_per_page());
    Ok(())
}

fn print_report_json(r: &BenchReport) -> Result<()> {
    print!("{{");
    print!("\"page_size\":{},", r.page_size);
    print!("\"buckets\":{},", r.buckets);
    print!("\"next_page_id\":{},", r.next_page_id);
    print!("\"last_lsn\":{},", r.last_lsn);
    print!("\"segments_bytes\":{},", r.segments_bytes);
    print!("\"wal_bytes\":{},", r.wal_bytes);
    if let Some(pt) = r.pack_threshold {
        print!("\"pack_threshold\":{},", pt);
    } else {
        print!("\"pack_threshold\":null,");
    }

    print!("\"phases\":[");
    for (i, p) in r.phases.iter().enumerate() {
        if i > 0 {
            print!(",");
        }
        print!(
            "{{\"name\":\"{}\",\"ops\":{},\"elapsed_sec\":{:.6},\"tput_ops\":{:.2},\"p50_ms\":{:.3},\"p90_ms\":{:.3},\"p99_ms\":{:.3}}}",
            p.name, p.ops, p.elapsed.as_secs_f64(), p.tput_ops, p.p50_ms, p.p90_ms, p.p99_ms
        );
    }
    print!("],");

    let m = &r.metrics;
    print!("\"metrics\":{{");
    print!("\"wal_appends_total\":{},", m.wal_appends_total);
    print!("\"wal_fsync_calls\":{},", m.wal_fsync_calls);
    print!("\"wal_avg_batch_pages\":{:.2},", m.avg_wal_batch_pages());
    print!(
        "\"page_cache_hits\":{},\"page_cache_misses\":{},",
        m.page_cache_hits, m.page_cache_misses
    );
    print!(
        "\"bloom_tests\":{},\"bloom_negative\":{},\"bloom_positive\":{},\"bloom_skipped_stale\":{},",
        m.bloom_tests, m.bloom_negative, m.bloom_positive, m.bloom_skipped_stale
    );
    // NEW: packing metrics in JSON
    print!("\"pack_pages\":{},", m.pack_pages);
    print!("\"pack_records\":{},", m.pack_records);
    print!("\"pack_pages_single\":{},", m.pack_pages_single);
    print!("\"pack_avg_records_per_page\":{:.3}", m.avg_pack_records_per_page());
    print!("}}");

    println!("}}");
    Ok(())
}