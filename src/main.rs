use std::path::PathBuf;

use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    if std::env::args().nth(1).as_deref() != Some("ui") {
        return Err("usage: zincir ui".into());
    }
    let database_path =
        PathBuf::from(std::env::var("ZINCIR_DATABASE_PATH").unwrap_or_else(|_| "zincir.db".into()));
    zincir::ui::serve(&database_path).await.map_err(Into::into)
}
