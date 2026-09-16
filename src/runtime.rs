use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::SqlitePool;
use tracing::{info, instrument};
use uuid::Uuid;

use crate::db;
use crate::error::Result;
use crate::provider::{LLMProvider, LlmMessage};
use crate::tool::ToolExecutor;
use crate::types::{EventType, RunConfig, RunLease, RunStatus, ToolCall};

// ponytail: renew around external calls; add a heartbeat if calls can exceed five minutes.
const RUN_LEASE_TTL: Duration = Duration::from_secs(300);

// ---------------------------------------------------------------------------
// Payload shapes stored in the events table. Local to the runtime — the
// SQLite stores JSON text; these structs define the wire shape for serialize/replay.
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
struct LlmCallPayload {
    content: Value,
    tool_calls: Vec<ToolCall>,
    stop_reason: String,
}

#[derive(Serialize, Deserialize)]
struct ToolCallPayload {
    call_id: String,
    name: String,
    args: Value,
}

#[derive(Serialize, Deserialize)]
struct ToolResultPayload {
    call_id: String,
    content: Value,
}

// ---------------------------------------------------------------------------
// Runtime
// ---------------------------------------------------------------------------

pub struct Runtime {
    pool: SqlitePool,
    provider: Arc<dyn LLMProvider>,
    tools: Arc<dyn ToolExecutor>,
}

impl Runtime {
    pub fn new(
        pool: SqlitePool,
        provider: Arc<dyn LLMProvider>,
        tools: Arc<dyn ToolExecutor>,
    ) -> Self {
        Self {
            pool,
            provider,
            tools,
        }
    }

    /// Resume all inflight runs and return their IDs.
    pub async fn resume(&self) -> Result<Vec<Uuid>> {
        let inflight = db::list_inflight_runs(&self.pool).await?;
        let run_ids = inflight.iter().map(|run| run.id).collect();
        for run in &inflight {
            info!(run_id = %run.id, "resuming run");
            self.start_run(run.id, true).await?;
        }
        Ok(run_ids)
    }

    /// Run (or resume) a single agent loop to completion.
    #[instrument(skip(self))]
    pub async fn run(&self, run_id: Uuid) -> Result<()> {
        self.start_run(run_id, false).await
    }

    async fn start_run(&self, run_id: Uuid, recover: bool) -> Result<()> {
        let run = db::get_run(&self.pool, run_id).await?;

        if matches!(run.status, RunStatus::Completed | RunStatus::Failed) {
            info!(run_id = %run_id, status = ?run.status, "run already terminal, skipping");
            return Ok(());
        }

        let owner_id = Uuid::new_v4();
        let lease = if recover {
            db::recover_run_lease(&self.pool, run_id, owner_id, RUN_LEASE_TTL).await?
        } else {
            db::acquire_run_lease(&self.pool, run_id, owner_id, RUN_LEASE_TTL).await?
        };
        let result = self.run_with_lease(run, lease).await;
        if let Err(error) = db::release_run_lease(&self.pool, &lease).await {
            if result.is_ok() {
                return Err(error);
            }
            tracing::warn!(run_id = %run_id, %error, "failed to release run lease");
        }
        result
    }

    async fn run_with_lease(&self, run: crate::types::AgentRun, mut lease: RunLease) -> Result<()> {
        let run_id = run.id;
        db::update_run_status_with_lease(&self.pool, &lease, RunStatus::Running).await?;

        let config: RunConfig = serde_json::from_value(run.config.clone())?;

        // --- Replay: reconstruct conversation from the durable event log ---
        let events = db::get_events(&self.pool, run_id).await?;
        let mut messages = vec![
            LlmMessage::system(&config.system),
            LlmMessage::user(&config.input),
        ];

        for event in &events {
            match event.event_type {
                EventType::LlmCall => {
                    let p: LlmCallPayload = serde_json::from_value(event.payload.clone())?;
                    messages.push(LlmMessage::assistant(&p.content, &p.tool_calls));
                }
                EventType::ToolResult => {
                    let p: ToolResultPayload = serde_json::from_value(event.payload.clone())?;
                    messages.push(LlmMessage::tool(&p.call_id, &p.content));
                }
                EventType::ToolCall | EventType::StateTransition => {}
            }
        }

        // --- Crash window: tool_calls that were logged but never got a result ---
        let pending = db::find_pending_tool_calls(&self.pool, run_id).await?;
        for tc in &pending {
            let p: ToolCallPayload = serde_json::from_value(tc.payload.clone())?;
            let call = ToolCall {
                id: p.call_id.clone(),
                name: p.name,
                args: p.args,
            };
            info!(call_id = %call.id, "re-executing pending tool call");
            lease = db::renew_run_lease(&self.pool, &lease, RUN_LEASE_TTL).await?;
            let result = self.tools.execute(call.clone()).await?;
            let payload = serde_json::to_value(ToolResultPayload {
                call_id: call.id.clone(),
                content: result.content.clone(),
            })?;
            db::append_event_with_lease(&self.pool, &lease, EventType::ToolResult, &payload, None)
                .await?;
            messages.push(LlmMessage::tool(&call.id, &result.content));
        }

        // --- Main loop ---
        loop {
            lease = db::renew_run_lease(&self.pool, &lease, RUN_LEASE_TTL).await?;
            let response = self.provider.complete(messages.clone(), vec![]).await?;
            let payload = serde_json::to_value(LlmCallPayload {
                content: response.content.clone(),
                tool_calls: response.tool_calls.clone(),
                stop_reason: response.stop_reason.clone(),
            })?;
            db::append_event_with_lease(&self.pool, &lease, EventType::LlmCall, &payload, None)
                .await?;

            messages.push(LlmMessage::assistant(
                &response.content,
                &response.tool_calls,
            ));

            if response.tool_calls.is_empty() {
                db::update_run_status_with_lease(&self.pool, &lease, RunStatus::Completed).await?;
                info!(run_id = %run_id, "run completed");
                break;
            }

            for call in &response.tool_calls {
                let intent = serde_json::to_value(ToolCallPayload {
                    call_id: call.id.clone(),
                    name: call.name.clone(),
                    args: call.args.clone(),
                })?;
                db::append_event_with_lease(
                    &self.pool,
                    &lease,
                    EventType::ToolCall,
                    &intent,
                    Some(&call.id),
                )
                .await?;

                lease = db::renew_run_lease(&self.pool, &lease, RUN_LEASE_TTL).await?;
                let result = self.tools.execute(call.clone()).await?;
                let result_payload = serde_json::to_value(ToolResultPayload {
                    call_id: call.id.clone(),
                    content: result.content.clone(),
                })?;
                db::append_event_with_lease(
                    &self.pool,
                    &lease,
                    EventType::ToolResult,
                    &result_payload,
                    None,
                )
                .await?;

                messages.push(LlmMessage::tool(&call.id, &result.content));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use serde_json::json;
    use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::Notify;

    struct GateProvider {
        calls: AtomicUsize,
        started: Notify,
        release: Notify,
    }

    #[async_trait]
    impl LLMProvider for GateProvider {
        async fn complete(
            &self,
            _messages: Vec<LlmMessage>,
            _tools: Vec<crate::provider::ToolSchema>,
        ) -> Result<crate::provider::Response> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                self.started.notify_one();
                self.release.notified().await;
            }
            Ok(crate::provider::Response {
                content: json!("done"),
                tool_calls: vec![],
                stop_reason: "stop".into(),
            })
        }
    }

    struct UnusedTools;

    #[async_trait]
    impl ToolExecutor for UnusedTools {
        async fn execute(&self, _call: ToolCall) -> Result<crate::types::ToolResult> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn concurrent_runtime_cannot_execute_the_same_run() {
        let directory = std::env::temp_dir().join(format!("zincir-runtime-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let path = directory.join("zincir.db");
        let options = SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(2)
            .connect_with(options)
            .await
            .unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        let run = db::create_run(
            &pool,
            Uuid::new_v4(),
            None,
            "test",
            "stub",
            &json!({
                "model": "stub",
                "system": "test",
                "input": "test",
                "tools": []
            }),
        )
        .await
        .unwrap();
        let provider = Arc::new(GateProvider {
            calls: AtomicUsize::new(0),
            started: Notify::new(),
            release: Notify::new(),
        });
        let runtime = Arc::new(Runtime::new(
            pool.clone(),
            provider.clone(),
            Arc::new(UnusedTools),
        ));

        let first_runtime = runtime.clone();
        let first = tokio::spawn(async move { first_runtime.run(run.id).await });
        provider.started.notified().await;
        let second = runtime.run(run.id).await;

        assert!(matches!(second, Err(crate::Error::InvalidState(_))));
        provider.release.notify_one();
        first.await.unwrap().unwrap();
        pool.close().await;
        std::fs::remove_dir_all(directory).unwrap();
    }
}
