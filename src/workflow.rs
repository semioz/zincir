use std::{future::Future, time::Duration};

use serde::{de::DeserializeOwned, Serialize};
use sqlx::SqlitePool;
use uuid::Uuid;

use crate::db;
use crate::error::{Error, Result};

/// Durable context for one workflow run.
pub struct WorkflowContext {
    pool: SqlitePool,
    run_id: Uuid,
    owner_id: Uuid,
}

impl WorkflowContext {
    pub fn new(pool: SqlitePool, run_id: Uuid) -> Self {
        Self {
            pool,
            run_id,
            owner_id: Uuid::new_v4(),
        }
    }

    /// Releases steps abandoned by a confirmed-dead previous process.
    pub async fn recover(&self) -> Result<()> {
        db::recover_steps(&self.pool, self.run_id).await
    }

    /// Waits until a persisted absolute wake time. Resuming waits only the
    /// remaining duration.
    pub async fn sleep(&self, name: &str, delay: Duration) -> Result<()> {
        validate_name(name)?;
        let wake_at_ms = db::schedule_timer(&self.pool, self.run_id, name, delay).await?;
        let remaining_ms = wake_at_ms.saturating_sub(chrono::Utc::now().timestamp_millis());
        if remaining_ms > 0 {
            tokio::time::sleep(Duration::from_millis(remaining_ms as u64)).await;
        }
        db::complete_timer(&self.pool, self.run_id, name).await
    }

    /// Runs a named operation once, then reuses its persisted JSON result.
    /// The operation must be idempotent if it has external side effects.
    pub async fn step<T, F, Fut>(&self, name: &str, operation: F) -> Result<T>
    where
        T: Serialize + DeserializeOwned,
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        validate_name(name)?;
        if let Some(result) = db::claim_step(&self.pool, self.run_id, name, self.owner_id).await? {
            return serde_json::from_value(result).map_err(Into::into);
        }

        let value = operation().await?;
        db::complete_step(
            &self.pool,
            self.run_id,
            name,
            self.owner_id,
            &serde_json::to_value(&value)?,
        )
        .await?;
        Ok(value)
    }
}

fn validate_name(name: &str) -> Result<()> {
    if !name.is_empty() && name.len() <= 255 {
        return Ok(());
    }
    Err(Error::InvalidState(
        "step name must be 1 to 255 bytes".into(),
    ))
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::{Duration, Instant},
    };

    use serde_json::json;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use tokio::sync::Notify;
    use uuid::Uuid;

    use super::WorkflowContext;
    use crate::db;

    async fn test_pool() -> sqlx::SqlitePool {
        let options = SqliteConnectOptions::new()
            .in_memory(true)
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        pool
    }

    async fn test_context(pool: &sqlx::SqlitePool) -> (WorkflowContext, Uuid) {
        let run_id = Uuid::new_v4();
        db::create_run(
            pool,
            run_id,
            None,
            "test",
            "stub",
            &json!({ "model": "stub", "system": "test", "input": "test" }),
        )
        .await
        .unwrap();
        (WorkflowContext::new(pool.clone(), run_id), run_id)
    }

    #[tokio::test]
    async fn sleep_uses_the_saved_wake_time_after_a_new_context() {
        let pool = test_pool().await;
        let (_, run_id) = test_context(&pool).await;
        db::schedule_timer(&pool, run_id, "nap", Duration::from_millis(50))
            .await
            .unwrap();

        let context = WorkflowContext::new(pool, run_id);
        let started = Instant::now();
        context.sleep("nap", Duration::from_secs(60)).await.unwrap();

        assert!(started.elapsed() >= Duration::from_millis(20));
        assert!(started.elapsed() < Duration::from_millis(500));
    }

    #[tokio::test]
    async fn completed_step_returns_its_saved_result_without_rerunning() {
        let pool = test_pool().await;
        let (context, run_id) = test_context(&pool).await;
        let calls = Arc::new(AtomicUsize::new(0));

        let first = context
            .step("answer", {
                let calls = Arc::clone(&calls);
                move || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, crate::error::Error>("saved answer".to_owned())
                }
            })
            .await
            .unwrap();
        let resumed_context = WorkflowContext::new(pool, run_id);
        let second: String = resumed_context
            .step("answer", || async {
                panic!("a completed step must not rerun its closure")
            })
            .await
            .unwrap();

        assert_eq!(first, "saved answer");
        assert_eq!(second, "saved answer");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn rejects_a_second_context_while_a_step_is_running() {
        let pool = test_pool().await;
        let (first_context, run_id) = test_context(&pool).await;
        let second_context = WorkflowContext::new(pool, run_id);
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());

        let first = tokio::spawn({
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            async move {
                first_context
                    .step("exclusive", move || async move {
                        started.notify_one();
                        release.notified().await;
                        Ok::<_, crate::error::Error>("first".to_owned())
                    })
                    .await
            }
        });
        started.notified().await;

        let error = second_context
            .step("exclusive", || async {
                Ok::<_, crate::error::Error>("second".to_owned())
            })
            .await
            .unwrap_err();

        assert!(matches!(error, crate::error::Error::InvalidState(_)));
        release.notify_one();
        assert_eq!(first.await.unwrap().unwrap(), "first");
    }

    #[tokio::test]
    async fn recovered_step_rejects_completion_from_its_previous_owner() {
        let pool = test_pool().await;
        let (first_context, run_id) = test_context(&pool).await;
        let recovered_context = WorkflowContext::new(pool, run_id);
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());

        let first = tokio::spawn({
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            async move {
                first_context
                    .step("fenced", move || async move {
                        started.notify_one();
                        release.notified().await;
                        Ok::<_, crate::error::Error>("stale".to_owned())
                    })
                    .await
            }
        });
        started.notified().await;

        // A real caller may only recover after confirming the previous process
        // is dead. This test intentionally races them to prove owner fencing.
        recovered_context.recover().await.unwrap();
        let recovered: String = recovered_context
            .step("fenced", || async {
                Ok::<_, crate::error::Error>("winner".to_owned())
            })
            .await
            .unwrap();

        release.notify_one();
        let stale = first.await.unwrap().unwrap_err();

        assert_eq!(recovered, "winner");
        assert!(matches!(stale, crate::error::Error::InvalidState(_)));
    }

    #[tokio::test]
    async fn recover_reclaims_a_step_left_running_by_a_failed_attempt() {
        let pool = test_pool().await;
        let (context, run_id) = test_context(&pool).await;

        let failure = context
            .step("retry", || async {
                Err::<String, _>(crate::error::Error::InvalidState("interrupted".into()))
            })
            .await
            .unwrap_err();
        assert!(matches!(failure, crate::error::Error::InvalidState(_)));

        let resumed_context = WorkflowContext::new(pool, run_id);
        let blocked = resumed_context
            .step("retry", || async {
                Ok::<_, crate::error::Error>("must not execute before recovery".to_owned())
            })
            .await
            .unwrap_err();
        assert!(matches!(blocked, crate::error::Error::InvalidState(_)));

        resumed_context.recover().await.unwrap();
        let result: String = resumed_context
            .step("retry", || async {
                Ok::<_, crate::error::Error>("recovered".to_owned())
            })
            .await
            .unwrap();

        assert_eq!(result, "recovered");
    }
}
