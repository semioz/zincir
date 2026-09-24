use std::{future::Future, sync::Arc, time::Duration};

use sqlx::SqlitePool;
use tokio::task::JoinSet;

use crate::{agent::validate_lease_ttl, AgentContext, Error, Result};

pub struct ResumeWorker {
    pool: SqlitePool,
    lease_ttl: Duration,
    poll_interval: Duration,
    max_concurrency: usize,
}

impl ResumeWorker {
    pub fn new(pool: SqlitePool, lease_ttl: Duration) -> Self {
        Self {
            pool,
            lease_ttl,
            poll_interval: Duration::from_secs(1),
            max_concurrency: 1,
        }
    }

    pub fn poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }

    pub fn max_concurrency(mut self, max_concurrency: usize) -> Self {
        self.max_concurrency = max_concurrency;
        self
    }

    /// Polls for recoverable runs until shutdown, then waits for active handlers.
    /// Handlers own their context and must complete or close it. Dropped contexts
    /// become recoverable again after their lease expires.
    pub async fn run<F, Fut, Shutdown>(self, handler: F, shutdown: Shutdown) -> Result<()>
    where
        F: Fn(AgentContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
        Shutdown: Future<Output = ()>,
    {
        validate_lease_ttl(self.lease_ttl)?;
        if self.poll_interval.is_zero() {
            return Err(Error::InvalidState(
                "resume worker poll interval must be greater than zero".into(),
            ));
        }
        if self.max_concurrency == 0 {
            return Err(Error::InvalidState(
                "resume worker concurrency must be greater than zero".into(),
            ));
        }

        let handler = Arc::new(handler);
        let mut tasks = JoinSet::new();
        let mut ticker = tokio::time::interval(self.poll_interval);
        let mut failure = None;
        tokio::pin!(shutdown);

        'worker: loop {
            tokio::select! {
                _ = &mut shutdown => break,
                _ = ticker.tick() => {
                    while tasks.len() < self.max_concurrency {
                        match AgentContext::claim_next_recoverable(
                            self.pool.clone(),
                            self.lease_ttl,
                        ).await {
                            Ok(Some(context)) => {
                                let handler = Arc::clone(&handler);
                                tasks.spawn(async move { handler(context).await });
                            }
                            Ok(None) => break,
                            Err(error) => {
                                failure = Some(error);
                                break 'worker;
                            }
                        }
                    }
                }
                result = tasks.join_next(), if !tasks.is_empty() => {
                    if let Some(result) = result {
                        report_handler_result(result);
                    }
                }
            }
        }

        while let Some(result) = tasks.join_next().await {
            report_handler_result(result);
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

fn report_handler_result(result: std::result::Result<Result<()>, tokio::task::JoinError>) {
    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::error!(%error, "resume handler failed"),
        Err(error) => tracing::error!(%error, "resume handler task failed"),
    }
}
