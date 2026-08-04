mod db;
mod error;
mod provider;
mod runtime;
mod tool;
mod types;

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

    let runtime = runtime::Runtime::new(
        pool.clone(),
        Arc::new(provider::StubProvider),
        Arc::new(tool::NoopToolExecutor),
    );

    let config = types::RunConfig {
        model: "stub".into(),
        temperature: Some(0.0),
        max_tokens: None,
        system: "You are a test agent.".into(),
        input: "Say hello.".into(),
        tools: vec![],
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

    let events = db::get_events(&pool, run.id).await?;
    for event in &events {
        tracing::info!(
            seq = event.seq,
            event_type = ?event.event_type,
            idempotency_key = ?event.idempotency_key,
            "event"
        );
    }

    let final_run = db::get_run(&pool, run.id).await?;
    tracing::info!(status = ?final_run.status, "final status");

    Ok(())
}
