mod db;
mod error;
mod provider;
mod runtime;
mod tool;
mod types;

use std::path::PathBuf;
use std::sync::Arc;

use sqlx::postgres::PgPoolOptions;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&db_url)
        .await?;

    sqlx::migrate!("./migrations").run(&pool).await?;

    let out_path = PathBuf::from(
        std::env::var("ZINCIR_OUTPUT_FILE").unwrap_or_else(|_| "output.txt".into()),
    );
    let runtime = runtime::Runtime::new(
        pool.clone(),
        Arc::new(provider::StubProvider),
        Arc::new(tool::FileAppendExecutor { path: out_path.clone() }),
    );

    // Resume mode: pick up any runs left inflight by a crashed process.
    if std::env::var("ZINCIR_RESUME").is_ok() {
        tracing::info!("resume mode");
        runtime.resume().await?;
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
    }

    // Print every inflight or terminal run's event log.
    let inflight = db::list_inflight_runs(&pool).await?;
    for run in &inflight {
        print_run(&pool, run.id).await?;
    }

    Ok(())
}

async fn print_run(pool: &sqlx::PgPool, run_id: Uuid) -> Result<(), Box<dyn std::error::Error>> {
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
