use anyhow::{anyhow, Context, Result};
use clap::Parser;
use env_logger::Env;
use log::{error, info, warn};
use serde::Deserialize;
use std::fs::{File, OpenOptions};
use std::io::BufReader;
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use QuiverDB::{
    cli::wal_apply_from_stream,
    lock::acquire_exclusive_lock,
    meta::read_meta,
};

// TLS
use rustls::{ClientConfig, ClientConnection, RootCertStore, ServerConfig, ServerConnection, StreamOwned};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs1KeyDer, PrivatePkcs8KeyDer, ServerName};
use rustls_pemfile::{certs, pkcs8_private_keys, rsa_private_keys};

#[derive(Clone, Copy, Debug)]
enum Decompress {
    None,
    Gzip,
    Zstd,
}

impl std::str::FromStr for Decompress {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "none" | "" => Ok(Decompress::None),
            "gzip" => Ok(Decompress::Gzip),
            "zstd" => Ok(Decompress::Zstd),
            other => Err(anyhow!("invalid decompress '{}': use none|gzip|zstd", other)),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum TlsMode {
    None,
    Server,
    Client,
}

impl std::str::FromStr for TlsMode {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "none" | "" => Ok(TlsMode::None),
            "server" => Ok(TlsMode::Server),
            "client" => Ok(TlsMode::Client),
            other => Err(anyhow!("invalid tls mode '{}': use none|server|client", other)),
        }
    }
}

#[derive(Parser, Debug)]
#[command(
    name = "quiverdb-follower",
    version,
    about = "QuiverDB follower/applier daemon (WAL streaming)"
)]
struct Cli {
    /// DB path (follower store)
    #[arg(long)]
    path: PathBuf,

    /// Config file (TOML). CLI flags override config values.
    #[arg(long)]
    config: Option<PathBuf>,

    /// Listen address (server mode, e.g. 0.0.0.0:9999). Mutually exclusive with --connect.
    #[arg(long)]
    listen: Option<String>,

    /// Connect address (client mode, e.g. leader.host:9999). Mutually exclusive with --listen.
    #[arg(long)]
    connect: Option<String>,

    /// Decompress incoming stream: none|gzip|zstd (must match leader's wal-ship)
    #[arg(long)]
    decompress: Option<Decompress>,

    /// TLS mode: none|server|client
    #[arg(long)]
    tls: Option<TlsMode>,

    /// TLS server certificate (PEM, chain)
    #[arg(long)]
    tls_cert: Option<PathBuf>,

    /// TLS server private key (PEM, pkcs8 or rsa)
    #[arg(long)]
    tls_key: Option<PathBuf>,

    /// TLS client CA certificate (PEM)
    #[arg(long)]
    tls_ca: Option<PathBuf>,

    /// TLS client SNI/verify domain (e.g., leader.example.com)
    #[arg(long)]
    tls_domain: Option<String>,

    /// Reconnect initial backoff in ms (client mode)
    #[arg(long)]
    reconnect_ms: Option<u64>,

    /// Reconnect backoff max cap in ms (client mode)
    #[arg(long)]
    backoff_cap_ms: Option<u64>,

    /// Optional file to write follower last LSN after each session
    #[arg(long)]
    checkpoint_file: Option<PathBuf>,
}

#[derive(Debug, Default, Deserialize)]
struct FileConfig {
    path: Option<PathBuf>,
    listen: Option<String>,
    connect: Option<String>,
    decompress: Option<String>,
    tls: Option<String>,
    tls_cert: Option<PathBuf>,
    tls_key: Option<PathBuf>,
    tls_ca: Option<PathBuf>,
    tls_domain: Option<String>,
    reconnect_ms: Option<u64>,
    backoff_cap_ms: Option<u64>,
    checkpoint_file: Option<PathBuf>,
}

#[derive(Clone, Debug)]
struct Runtime {
    path: PathBuf,
    listen: Option<String>,
    connect: Option<String>,
    decompress: Decompress,
    tls: TlsMode,
    tls_cert: Option<PathBuf>,
    tls_key: Option<PathBuf>,
    tls_ca: Option<PathBuf>,
    tls_domain: Option<String>,
    reconnect_ms: u64,
    backoff_cap_ms: u64,
    checkpoint_file: Option<PathBuf>,
}

fn main() {
    init_logger();

    if let Err(e) = run() {
        error!("{:#}", e);
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let args = Cli::parse();
    let rt = build_runtime(&args)?;

    if rt.listen.is_some() && rt.connect.is_some() {
        return Err(anyhow!("--listen and --connect are mutually exclusive (effective config)"));
    }

    // Validate TLS params for the selected mode
    match rt.tls {
        TlsMode::None => {}
        TlsMode::Server => {
            if rt.listen.is_none() {
                warn!("TLS server mode requested, but not in listen mode; TLS will be ignored");
            }
            if rt.tls_cert.is_none() || rt.tls_key.is_none() {
                return Err(anyhow!("TLS server mode requires --tls-cert and --tls-key (or config equivalents)"));
            }
        }
        TlsMode::Client => {
            if rt.connect.is_none() {
                warn!("TLS client mode requested, but not in connect mode; TLS will be ignored");
            }
            if rt.tls_ca.is_none() || rt.tls_domain.is_none() {
                return Err(anyhow!("TLS client mode requires --tls-ca and --tls-domain (or config equivalents)"));
            }
        }
    }

    // Take exclusive lock for entire daemon lifetime (single-writer safety).
    let _lock = acquire_exclusive_lock(&rt.path)
        .with_context(|| format!("acquire exclusive lock at {}", rt.path.display()))?;

    match (rt.listen.clone(), rt.connect.clone()) {
        (Some(addr), None) => server_loop(&rt, &addr),
        (None, Some(addr)) => client_loop(&rt, &addr),
        (None, None) => {
            let addr = "0.0.0.0:9999".to_string();
            warn!("neither listen nor connect specified, defaulting to listen on {}", addr);
            server_loop(&rt, &addr)
        }
        _ => unreachable!(),
    }
}

fn server_loop(rt: &Runtime, bind_addr: &str) -> Result<()> {
    let listener = TcpListener::bind(bind_addr)
        .with_context(|| format!("bind listener at {}", bind_addr))?;
    info!("listening on {}", bind_addr);

    loop {
        let (stream, peer) = listener.accept().context("accept connection")?;
        info!("accepted from {}", peer);
        if let Err(e) = handle_session(rt, stream) {
            warn!("session error: {:#}", e);
        }
    }
}

fn client_loop(rt: &Runtime, addr: &str) -> Result<()> {
    let mut backoff = rt.reconnect_ms.max(100);
    let cap = rt.backoff_cap_ms.max(backoff);

    loop {
        match TcpStream::connect(addr) {
            Ok(stream) => {
                info!("connected to {}", addr);
                backoff = rt.reconnect_ms.max(100); // reset backoff after a successful connect
                if let Err(e) = handle_session(rt, stream) {
                    warn!("session error: {:#}", e);
                }
                info!("disconnected from {}", addr);
            }
            Err(e) => {
                warn!("connect to {} failed: {} (retry in {} ms)", addr, e, backoff);
                thread::sleep(Duration::from_millis(backoff));
                backoff = backoff.saturating_mul(2).min(cap);
            }
        }
    }
}

fn handle_session(rt: &Runtime, stream: TcpStream) -> Result<()> {
    stream.set_nodelay(true).ok(); // best-effort

    // Build base reader: plain TCP or TLS-wrapped stream
    match rt.tls {
        TlsMode::None => apply_with_decompress(rt, stream)?,
        TlsMode::Server => {
            // TLS server (listen mode)
            let tls_stream = tls_wrap_server(rt, stream)?;
            apply_with_decompress(rt, tls_stream)?
        }
        TlsMode::Client => {
            // TLS client (connect mode)
            let tls_stream = tls_wrap_client(rt, stream)?;
            apply_with_decompress(rt, tls_stream)?
        }
    }

    // Optional checkpoint: record follower's last_lsn to file
    if let Some(cf) = &rt.checkpoint_file {
        let m = read_meta(&rt.path)?;
        let s = m.last_lsn.to_string();
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(cf)
            .with_context(|| format!("open checkpoint file {}", cf.display()))?;
        use std::io::Write;
        f.write_all(s.as_bytes())?;
        f.sync_all().ok();
        info!("checkpoint last_lsn={} -> {}", m.last_lsn, cf.display());
    }

    Ok(())
}

// Apply stream with optional decompression (generic over reader)
fn apply_with_decompress<R: std::io::Read>(rt: &Runtime, reader: R) -> Result<()> {
    match rt.decompress {
        Decompress::None => {
            wal_apply_from_stream(&rt.path, reader)?;
        }
        Decompress::Gzip => {
            let dec = flate2::read::GzDecoder::new(reader);
            wal_apply_from_stream(&rt.path, dec)?;
        }
        Decompress::Zstd => {
            let dec = zstd::stream::read::Decoder::new(reader)
                .context("create zstd decoder")?;
            wal_apply_from_stream(&rt.path, dec)?;
        }
    }
    Ok(())
}

// TLS server wrapper
fn tls_wrap_server(rt: &Runtime, stream: TcpStream) -> Result<StreamOwned<ServerConnection, TcpStream>> {
    let cert_path = rt.tls_cert.clone().ok_or_else(|| anyhow!("tls_cert not set"))?;
    let key_path = rt.tls_key.clone().ok_or_else(|| anyhow!("tls_key not set"))?;

    // Load cert chain (Vec<Vec<u8>> -> Vec<CertificateDer>)
    let mut cert_reader = BufReader::new(File::open(&cert_path).with_context(|| format!("open cert {}", cert_path.display()))?);
    let certs_raw = certs(&mut cert_reader).map_err(|e| anyhow!("read certs: {}", e))?;
    let certs: Vec<CertificateDer<'static>> = certs_raw.into_iter().map(CertificateDer::from).collect();

    if certs.is_empty() {
        return Err(anyhow!("no certificates found in {}", cert_path.display()));
    }

    // Load private key (try pkcs8 first, then rsa), wrap into PrivateKeyDer
    let key: PrivateKeyDer<'static> = {
        let mut kr = BufReader::new(File::open(&key_path).with_context(|| format!("open key {}", key_path.display()))?);
        let mut pkcs8 = pkcs8_private_keys(&mut kr).map_err(|e| anyhow!("read pkcs8 keys: {}", e))?;
        if let Some(k) = pkcs8.into_iter().next() {
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(k))
        } else {
            let mut kr2 = BufReader::new(File::open(&key_path).with_context(|| format!("open key {}", key_path.display()))?);
            let mut rsa = rsa_private_keys(&mut kr2).map_err(|e| anyhow!("read rsa keys: {}", e))?;
            let k = rsa.into_iter().next().ok_or_else(|| anyhow!("no private keys found in {}", key_path.display()))?;
            PrivateKeyDer::Pkcs1(PrivatePkcs1KeyDer::from(k))
        }
    };

    let server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("build tls server config")?;

    let conn = ServerConnection::new(Arc::new(server_config))
        .context("create tls server connection")?;

    Ok(StreamOwned::new(conn, stream))
}

// TLS client wrapper
fn tls_wrap_client(rt: &Runtime, stream: TcpStream) -> Result<StreamOwned<ClientConnection, TcpStream>> {
    let ca_path = rt.tls_ca.clone().ok_or_else(|| anyhow!("tls_ca not set"))?;
    let dns_name = rt.tls_domain.clone().ok_or_else(|| anyhow!("tls_domain not set"))?;

    // Load CA certs and add to RootCertStore
    let mut ca_reader = BufReader::new(File::open(&ca_path).with_context(|| format!("open ca {}", ca_path.display()))?);
    let ca_raw = certs(&mut ca_reader).map_err(|e| anyhow!("read ca certs: {}", e))?;
    if ca_raw.is_empty() {
        return Err(anyhow!("no CA certificates found in {}", ca_path.display()));
    }
    let mut roots = RootCertStore::empty();
    for der in ca_raw.into_iter().map(CertificateDer::from) {
        roots.add(der).map_err(|e| anyhow!("add CA cert: {:?}", e))?;
    }

    let client_config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();

    // Use owned ServerName so it lives long enough
    let server_name = ServerName::try_from(dns_name.clone())
        .map_err(|_| anyhow!("invalid tls_domain '{}'", dns_name))?;

    let conn = ClientConnection::new(Arc::new(client_config), server_name)
        .context("create tls client connection")?;

    Ok(StreamOwned::new(conn, stream))
}

fn build_runtime(args: &Cli) -> Result<Runtime> {
    let cfg = if let Some(ref p) = args.config {
        let s = std::fs::read_to_string(p)
            .with_context(|| format!("read config {}", p.display()))?;
        let parsed: FileConfig = toml::from_str(&s).context("parse TOML config")?;
        Some(parsed)
    } else {
        None
    };

    // Helper: choose CLI > config > default
    fn pick<T: Clone>(cli: Option<T>, cfg: Option<T>, default: T) -> T {
        cli.or(cfg).unwrap_or(default)
    }
    // Helper for Option merge (no default)
    fn pick_opt<T: Clone>(cli: &Option<T>, cfg: &Option<T>) -> Option<T> {
        cli.clone().or(cfg.clone())
    }

    // Decompress
    let cfg_decomp = cfg
        .as_ref()
        .and_then(|c| c.decompress.as_ref().cloned())
        .map(|s| s.parse::<Decompress>())
        .transpose()?;
    let decomp = pick(args.decompress, cfg_decomp, Decompress::None);

    // TLS
    let cfg_tls = cfg
        .as_ref()
        .and_then(|c| c.tls.as_ref().cloned())
        .map(|s| s.parse::<TlsMode>())
        .transpose()?;
    let tls = pick(args.tls, cfg_tls, TlsMode::None);

    let tls_cert = pick_opt(&args.tls_cert, &cfg.as_ref().and_then(|c| c.tls_cert.clone()));
    let tls_key = pick_opt(&args.tls_key, &cfg.as_ref().and_then(|c| c.tls_key.clone()));
    let tls_ca = pick_opt(&args.tls_ca, &cfg.as_ref().and_then(|c| c.tls_ca.clone()));
    let tls_domain = pick_opt(&args.tls_domain, &cfg.as_ref().and_then(|c| c.tls_domain.clone()));

    // Listen/connect
    let listen = pick_opt(&args.listen, &cfg.as_ref().and_then(|c| c.listen.clone()));
    let connect = pick_opt(&args.connect, &cfg.as_ref().and_then(|c| c.connect.clone()));

    // Backoff timers
    let reconnect_ms = pick(args.reconnect_ms, cfg.as_ref().and_then(|c| c.reconnect_ms), 1000);
    let backoff_cap_ms =
        pick(args.backoff_cap_ms, cfg.as_ref().and_then(|c| c.backoff_cap_ms), 15_000);

    // Checkpoint file
    let checkpoint_file = pick_opt(
        &args.checkpoint_file,
        &cfg.as_ref().and_then(|c| c.checkpoint_file.clone()),
    );

    // Path: keep CLI mandatory
    let path = args.path.clone();

    Ok(Runtime {
        path,
        listen,
        connect,
        decompress: decomp,
        tls,
        tls_cert,
        tls_key,
        tls_ca,
        tls_domain,
        reconnect_ms,
        backoff_cap_ms,
        checkpoint_file,
    })
}

fn init_logger() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();
}