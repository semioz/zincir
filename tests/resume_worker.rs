use std::{sync::Arc, time::Duration};

use serde_json::json;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use tokio::sync::mpsc;
use uuid::Uuid;
use zincir::{db, AgentContext, ResumeWorker, RunStatus};

async fn test_pool() -> (sqlx::SqlitePool, std::path::PathBuf) {
    let directory = std::env::temp_dir().join(format!("zincir-worker-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(directory.join("zincir.db"))
                .create_if_missing(true)
                .foreign_keys(true)
                .journal_mode(SqliteJournalMode::Wal)
                .busy_timeout(Duration::from_secs(5)),
        )
        .await
        .unwrap();
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();
    (pool, directory)
}

#[tokio::test]
async fn safe_recovery_skips_active_leases_and_claims_expired_ones() {
    let (pool, directory) = test_pool().await;
    let active = AgentContext::create(
        pool.clone(),
        None,
        "agent",
        "application",
        json!({}),
        Duration::from_secs(30),
    )
    .await
    .unwrap();
    let run_id = active.run_id();

    assert!(
        AgentContext::claim_next_recoverable(pool.clone(), Duration::from_secs(30))
            .await
            .unwrap()
            .is_none()
    );

    sqlx::query("UPDATE agent_runs SET lease_expires_at_ms = 0 WHERE id = ?")
        .bind(run_id)
        .execute(&pool)
        .await
        .unwrap();
    let recovered = AgentContext::claim_next_recoverable(pool.clone(), Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(recovered.run_id(), run_id);
    drop(active);
    recovered.close().await.unwrap();
    pool.close().await;
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn concurrent_workers_claim_a_run_once() {
    let (pool, directory) = test_pool().await;
    let run = db::create_run(
        &pool,
        Uuid::new_v4(),
        None,
        "agent",
        "application",
        &json!({}),
    )
    .await
    .unwrap();

    let (first, second) = tokio::join!(
        AgentContext::claim_next_recoverable(pool.clone(), Duration::from_secs(30)),
        AgentContext::claim_next_recoverable(pool.clone(), Duration::from_secs(30)),
    );
    let mut claimed = [first.unwrap(), second.unwrap()]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].run_id(), run.id);
    claimed.pop().unwrap().close().await.unwrap();
    pool.close().await;
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn worker_dispatches_and_completes_recoverable_runs() {
    let (pool, directory) = test_pool().await;
    let run = db::create_run(
        &pool,
        Uuid::new_v4(),
        None,
        "agent",
        "application",
        &json!({}),
    )
    .await
    .unwrap();
    let (completed, mut completion) = mpsc::channel(1);
    let worker = ResumeWorker::new(pool.clone(), Duration::from_secs(30))
        .poll_interval(Duration::from_millis(10))
        .max_concurrency(2);

    tokio::time::timeout(
        Duration::from_secs(2),
        worker.run(
            move |ctx| {
                let completed = completed.clone();
                async move {
                    let run_id = ctx.run_id();
                    ctx.complete().await?;
                    let _ = completed.send(run_id).await;
                    Ok(())
                }
            },
            async move {
                completion.recv().await;
            },
        ),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(
        db::get_run(&pool, run.id).await.unwrap().status,
        RunStatus::Completed
    );
    pool.close().await;
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn failed_handler_is_retried_after_lease_expiry() {
    let (pool, directory) = test_pool().await;
    let run = db::create_run(
        &pool,
        Uuid::new_v4(),
        None,
        "agent",
        "application",
        &json!({}),
    )
    .await
    .unwrap();
    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = Arc::clone(&attempts);
    let (completed, mut completion) = mpsc::channel(1);
    let worker = ResumeWorker::new(pool.clone(), Duration::from_millis(40))
        .poll_interval(Duration::from_millis(5));

    tokio::time::timeout(
        Duration::from_secs(2),
        worker.run(
            move |ctx| {
                let attempt = observed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let completed = completed.clone();
                async move {
                    if attempt == 0 {
                        return Err(zincir::Error::Tool("retry me".into()));
                    }
                    ctx.complete().await?;
                    let _ = completed.send(()).await;
                    Ok(())
                }
            },
            async move {
                completion.recv().await;
            },
        ),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(attempts.load(std::sync::atomic::Ordering::Relaxed), 2);
    assert_eq!(
        db::get_run(&pool, run.id).await.unwrap().status,
        RunStatus::Completed
    );
    pool.close().await;
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn worker_never_dispatches_terminal_runs() {
    let (pool, directory) = test_pool().await;
    let ctx = AgentContext::create(
        pool.clone(),
        None,
        "agent",
        "application",
        json!({}),
        Duration::from_secs(30),
    )
    .await
    .unwrap();
    ctx.complete().await.unwrap();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let worker = ResumeWorker::new(pool.clone(), Duration::from_secs(30))
        .poll_interval(Duration::from_millis(10));

    worker
        .run(
            move |_| {
                observed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                async { Ok(()) }
            },
            tokio::time::sleep(Duration::from_millis(40)),
        )
        .await
        .unwrap();

    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 0);
    pool.close().await;
    std::fs::remove_dir_all(directory).unwrap();
}
