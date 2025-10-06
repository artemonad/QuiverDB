use env_logger::{Builder, Env};
use log::error;

fn init_logger() {
    // Уровень берём из RUST_LOG, иначе дефолт — info.
    // Пример: RUST_LOG=debug ./quiverdb ...
    Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();
}

fn main() {
    init_logger();

    if let Err(e) = QuiverDB::cli::run() {
        // Логируем ошибку и выходим с кодом 1.
        error!("{:?}", e);
        std::process::exit(1);
    }
}