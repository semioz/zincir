use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use serde_json::json;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use tokio::sync::oneshot;
use uuid::Uuid;
use zincir::{db, AgentContext, ResumeWorker, RunStatus};

const LEASE_TTL: Duration = Duration::from_millis(100);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let (database_path, temporary_directory) = database_path()?;
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(&database_path)
                .create_if_missing(true)
                .foreign_keys(true)
                .journal_mode(SqliteJournalMode::Wal)
                .busy_timeout(Duration::from_secs(5)),
        )
        .await?;
    sqlx::migrate!("./migrations").run(&pool).await?;

    let abandoned = AgentContext::create(
        pool.clone(),
        None,
        "resume-example",
        "application-owned-agent",
        json!({ "goal": "resume after the lease expires" }),
        LEASE_TTL,
    )
    .await?;
    let run_id = abandoned.run_id();
    drop(abandoned);
    tracing::info!(%run_id, "simulated process loss");

    let (completed, shutdown) = oneshot::channel();
    let completed = Arc::new(Mutex::new(Some(completed)));
    ResumeWorker::new(pool.clone(), LEASE_TTL)
        .poll_interval(Duration::from_millis(20))
        .run(
            move |ctx| {
                let completed = Arc::clone(&completed);
                async move {
                    let run_id = ctx.run_id();
                    let state = ctx.state().await?;
                    tracing::info!(
                        %run_id,
                        events = state.events.len(),
                        "worker resumed run"
                    );
                    ctx.complete().await?;
                    if let Some(completed) = completed
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .take()
                    {
                        let _ = completed.send(());
                    }
                    Ok(())
                }
            },
            async {
                let _ = shutdown.await;
            },
        )
        .await?;

    assert_eq!(
        db::get_run(&pool, run_id).await?.status,
        RunStatus::Completed
    );
    tracing::info!(%run_id, "run completed after automatic recovery");
    pool.close().await;
    if let Some(directory) = temporary_directory {
        std::fs::remove_dir_all(directory)?;
    }
    Ok(())
}

fn database_path() -> std::io::Result<(PathBuf, Option<PathBuf>)> {
    if let Some(path) = std::env::var_os("ZINCIR_DATABASE_PATH") {
        return Ok((path.into(), None));
    }
    let directory = std::env::temp_dir().join(format!("zincir-worker-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&directory)?;
    Ok((directory.join("zincir.db"), Some(directory)))
}
