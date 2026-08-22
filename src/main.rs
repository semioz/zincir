mod db;
mod error;
mod provider;
mod runtime;
mod tool;
mod types;

use std::path::PathBuf;
use std::sync::Arc;

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use std::time::Duration;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let database_path =
        PathBuf::from(std::env::var("ZINCIR_DATABASE_PATH").unwrap_or_else(|_| "zincir.db".into()));
    let options = SqliteConnectOptions::new()
        .filename(&database_path)
        .create_if_missing(true)
        .foreign_keys(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Full)
        .busy_timeout(Duration::from_secs(5));
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await?;

    sqlx::migrate!("./migrations").run(&pool).await?;

    let output_directory =
        PathBuf::from(std::env::var("ZINCIR_OUTPUT_DIR").unwrap_or_else(|_| "output".into()));
    let runtime = runtime::Runtime::new(
        pool.clone(),
        Arc::new(provider::StubProvider),
        Arc::new(tool::IdempotentFileExecutor {
            directory: output_directory,
        }),
    );

    // Resume mode: pick up any runs left inflight by a crashed process.
    let run_ids = if std::env::var("ZINCIR_RESUME").is_ok() {
        tracing::info!("resume mode");
        runtime.resume().await?
    } else {
        let config = types::RunConfig {
            model: "stub".into(),
            temperature: Some(0.0),
            max_tokens: None,
            system: "You are a test agent.".into(),
            input: "Write hello to the file.".into(),
            tools: vec!["write_file".into()],
        };

        let run = db::create_run(
            &pool,
            Uuid::new_v4(),
            None,
            "supervisor",
            "stub",
            &serde_json::to_value(&config)?,
        )
        .await?;
        tracing::info!(run_id = %run.id, "created run");
        runtime.run(run.id).await?;
        vec![run.id]
    };

    for run_id in run_ids {
        print_run(&pool, run_id).await?;
    }

    Ok(())
}

async fn print_run(
    pool: &sqlx::SqlitePool,
    run_id: Uuid,
) -> Result<(), Box<dyn std::error::Error>> {
    let run = db::get_run(pool, run_id).await?;
    tracing::info!(run_id = %run.id, status = ?run.status, "run");
    let events = db::get_events(pool, run.id).await?;
    for event in &events {
        tracing::info!(
            seq = event.seq,
            event_type = ?event.event_type,
            idempotency_key = ?event.idempotency_key,
            "  event"
        );
    }
    Ok(())
}
