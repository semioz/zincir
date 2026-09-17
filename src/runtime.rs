use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::SqlitePool;
use tracing::{info, instrument};
use uuid::Uuid;

use crate::db;
use crate::error::{Error, Result};
use crate::provider::{LLMProvider, LlmMessage, ToolSchema};
use crate::tool::ToolExecutor;
use crate::types::{
    CheckpointRecord, CheckpointState, EventType, RunConfig, RunLease, RunStatus, ToolCall,
};

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
    #[serde(default = "one_tool_call")]
    response_tool_count: usize,
}

fn one_tool_call() -> usize {
    1
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
        let Err(release_error) = db::release_run_lease(&self.pool, &lease).await else {
            return result;
        };
        if db::get_run(&self.pool, run_id)
            .await
            .is_ok_and(|run| run.status == RunStatus::Completed)
        {
            return result;
        }
        if result.is_err() {
            tracing::warn!(run_id = %run_id, error = %release_error, "failed to release run lease");
            return result;
        }
        Err(release_error)
    }

    async fn run_with_lease(&self, run: crate::types::AgentRun, mut lease: RunLease) -> Result<()> {
        let run_id = run.id;
        db::update_run_status_with_lease(&self.pool, &lease, RunStatus::Running).await?;
        let config: RunConfig = serde_json::from_value(run.config.clone())?;

        if let Some(candidate) = db::get_pending_checkpoint(&self.pool, run_id).await? {
            self.verify_candidate(&config, &mut lease, candidate)
                .await?;
        }

        let pending = db::find_pending_tool_calls(&self.pool, run_id).await?;
        for event in &pending {
            let (call, response_tool_count) = tool_call_from_event(event)?;
            if call.name == "submit_checkpoint" {
                if response_tool_count != 1 {
                    let content = invalid_checkpoint_result(
                        "submit_checkpoint must be the only tool call in a response",
                    );
                    append_checkpoint_error(&self.pool, &lease, &call.id, &content).await?;
                    continue;
                }
                match serde_json::from_value(call.args.clone()) {
                    Ok(state) => {
                        self.create_and_verify_checkpoint(&config, &mut lease, &call.id, &state)
                            .await?;
                    }
                    Err(error) => {
                        let content = invalid_checkpoint_result(&error.to_string());
                        append_checkpoint_error(&self.pool, &lease, &call.id, &content).await?;
                    }
                }
            }
        }

        let accepted = db::get_latest_accepted_checkpoint(&self.pool, run_id).await?;
        if let Some(checkpoint) = accepted.as_ref() {
            if checkpoint_completes_run(checkpoint)? {
                db::complete_run_with_lease(&self.pool, &lease).await?;
                return Ok(());
            }
        }
        let mut messages = build_context(&config, accepted.as_ref())?;
        let replay_after = accepted
            .as_ref()
            .and_then(|checkpoint| checkpoint.decision_event_seq)
            .map(|seq| seq + 1)
            .unwrap_or(-1);
        replay_events(&self.pool, run_id, replay_after, &mut messages).await?;

        for event in db::find_pending_tool_calls(&self.pool, run_id).await? {
            let (call, _) = tool_call_from_event(&event)?;
            if call.name == "submit_checkpoint" {
                continue;
            }
            info!(call_id = %call.id, "re-executing pending tool call");
            lease = db::renew_run_lease(&self.pool, &lease, RUN_LEASE_TTL).await?;
            let result = self.tools.execute(call.clone()).await?;
            append_tool_result(&self.pool, &lease, &call.id, &result.content).await?;
            messages.push(LlmMessage::tool(&call.id, &result.content));
        }

        'rounds: loop {
            lease = db::renew_run_lease(&self.pool, &lease, RUN_LEASE_TTL).await?;
            let response = self
                .provider
                .complete(messages.clone(), vec![checkpoint_tool_schema()])
                .await?;
            let payload = serde_json::to_value(LlmCallPayload {
                content: response.content.clone(),
                tool_calls: response.tool_calls.clone(),
                stop_reason: response.stop_reason.clone(),
            })?;
            let tool_count = response.tool_calls.len();
            let intents = response
                .tool_calls
                .iter()
                .map(|call| {
                    Ok((
                        serde_json::to_value(ToolCallPayload {
                            call_id: call.id.clone(),
                            name: call.name.clone(),
                            args: call.args.clone(),
                            response_tool_count: tool_count,
                        })?,
                        call.id.clone(),
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            db::append_llm_call_with_tool_intents_with_lease(
                &self.pool, &lease, &payload, &intents,
            )
            .await?;
            messages.push(LlmMessage::assistant(
                &response.content,
                &response.tool_calls,
            ));

            if response.tool_calls.is_empty() {
                return Err(Error::InvalidState(
                    "agent stopped without submitting a checkpoint".into(),
                ));
            }

            for call in &response.tool_calls {
                if call.name == "submit_checkpoint" {
                    if response.tool_calls.len() != 1 {
                        let content = invalid_checkpoint_result(
                            "submit_checkpoint must be the only tool call in a response",
                        );
                        append_checkpoint_error(&self.pool, &lease, &call.id, &content).await?;
                        messages.push(LlmMessage::tool(&call.id, &content));
                        continue;
                    }
                    let state = match serde_json::from_value::<CheckpointState>(call.args.clone()) {
                        Ok(state) => state,
                        Err(error) => {
                            let content = invalid_checkpoint_result(&error.to_string());
                            append_checkpoint_error(&self.pool, &lease, &call.id, &content).await?;
                            messages.push(LlmMessage::tool(&call.id, &content));
                            continue;
                        }
                    };
                    let checkpoint = self
                        .create_and_verify_checkpoint(&config, &mut lease, &call.id, &state)
                        .await?;
                    if checkpoint.status == "accepted" {
                        if state.remaining.is_empty() {
                            db::complete_run_with_lease(&self.pool, &lease).await?;
                            info!(run_id = %run_id, "run completed from verified checkpoint");
                            break 'rounds;
                        }
                        messages = build_context(&config, Some(&checkpoint))?;
                        continue 'rounds;
                    }
                    let content = json_checkpoint_result(false, checkpoint.verification.as_ref());
                    messages.push(LlmMessage::tool(&call.id, &content));
                    continue;
                }

                lease = db::renew_run_lease(&self.pool, &lease, RUN_LEASE_TTL).await?;
                let result = self.tools.execute(call.clone()).await?;
                append_tool_result(&self.pool, &lease, &call.id, &result.content).await?;
                messages.push(LlmMessage::tool(&call.id, &result.content));
            }
        }

        Ok(())
    }

    async fn create_and_verify_checkpoint(
        &self,
        config: &RunConfig,
        lease: &mut RunLease,
        tool_call_id: &str,
        state: &CheckpointState,
    ) -> Result<CheckpointRecord> {
        let candidate =
            db::propose_checkpoint_with_lease(&self.pool, lease, tool_call_id, state).await?;
        self.verify_candidate(config, lease, candidate).await
    }

    async fn verify_candidate(
        &self,
        config: &RunConfig,
        lease: &mut RunLease,
        candidate: CheckpointRecord,
    ) -> Result<CheckpointRecord> {
        let state: CheckpointState = serde_json::from_value(candidate.state.clone())?;
        *lease = db::renew_run_lease(&self.pool, lease, RUN_LEASE_TTL).await?;
        let verification = run_verifier(config, &state).await?;
        let accepted = verification["passed"].as_bool().unwrap_or(false);
        db::decide_checkpoint_with_lease(&self.pool, lease, candidate.id, accepted, &verification)
            .await
    }
}

fn tool_call_from_event(event: &crate::types::Event) -> Result<(ToolCall, usize)> {
    let payload: ToolCallPayload = serde_json::from_value(event.payload.clone())?;
    Ok((
        ToolCall {
            id: payload.call_id,
            name: payload.name,
            args: payload.args,
        },
        payload.response_tool_count,
    ))
}

#[cfg(test)]
async fn append_tool_call(pool: &SqlitePool, lease: &RunLease, call: &ToolCall) -> Result<()> {
    let payload = serde_json::to_value(ToolCallPayload {
        call_id: call.id.clone(),
        name: call.name.clone(),
        args: call.args.clone(),
        response_tool_count: 1,
    })?;
    db::append_event_with_lease(pool, lease, EventType::ToolCall, &payload, Some(&call.id)).await?;
    Ok(())
}

async fn append_tool_result(
    pool: &SqlitePool,
    lease: &RunLease,
    call_id: &str,
    content: &Value,
) -> Result<()> {
    db::record_tool_result_with_lease(pool, lease, call_id, content).await?;
    Ok(())
}

async fn append_checkpoint_error(
    pool: &SqlitePool,
    lease: &RunLease,
    call_id: &str,
    content: &Value,
) -> Result<()> {
    let payload = serde_json::to_value(ToolResultPayload {
        call_id: call_id.into(),
        content: content.clone(),
    })?;
    db::append_event_with_lease(
        pool,
        lease,
        EventType::ToolResult,
        &payload,
        Some(&format!("checkpoint_error:{call_id}")),
    )
    .await?;
    Ok(())
}

fn build_context(
    config: &RunConfig,
    checkpoint: Option<&CheckpointRecord>,
) -> Result<Vec<LlmMessage>> {
    let input = if let Some(checkpoint) = checkpoint {
        format!(
            "Original goal:\n{}\n\nLatest accepted checkpoint (round {}):\n{}",
            config.input,
            checkpoint.round,
            serde_json::to_string_pretty(&checkpoint.state)?
        )
    } else {
        config.input.clone()
    };
    Ok(vec![
        LlmMessage::system(&config.system),
        LlmMessage::user(&input),
    ])
}

async fn replay_events(
    pool: &SqlitePool,
    run_id: Uuid,
    after_seq: i32,
    messages: &mut Vec<LlmMessage>,
) -> Result<()> {
    for event in db::get_events(pool, run_id)
        .await?
        .into_iter()
        .filter(|event| event.seq > after_seq)
    {
        match event.event_type {
            EventType::LlmCall => {
                let payload: LlmCallPayload = serde_json::from_value(event.payload)?;
                messages.push(LlmMessage::assistant(&payload.content, &payload.tool_calls));
            }
            EventType::ToolResult => {
                let payload: ToolResultPayload = serde_json::from_value(event.payload)?;
                messages.push(LlmMessage::tool(&payload.call_id, &payload.content));
            }
            EventType::ToolCall
            | EventType::StateTransition
            | EventType::CheckpointProposed
            | EventType::CheckpointAccepted
            | EventType::CheckpointRejected => {}
        }
    }
    Ok(())
}

fn checkpoint_completes_run(checkpoint: &CheckpointRecord) -> Result<bool> {
    let state: CheckpointState = serde_json::from_value(checkpoint.state.clone())?;
    Ok(state.remaining.is_empty())
}

fn checkpoint_tool_schema() -> ToolSchema {
    ToolSchema {
        name: "submit_checkpoint".into(),
        description: "Submit semantic progress for independent verification.".into(),
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "completed": { "type": "array", "items": { "type": "string" } },
                "remaining": { "type": "array", "items": { "type": "string" } },
                "artifacts": { "type": "array", "items": { "type": "string" } },
                "failed_attempts": { "type": "array", "items": { "type": "string" } },
                "evidence": { "type": "array", "items": { "type": "string" } }
            },
            "required": [
                "completed", "remaining", "artifacts", "failed_attempts", "evidence"
            ],
            "additionalProperties": false
        }),
    }
}

async fn run_verifier(config: &RunConfig, state: &CheckpointState) -> Result<Value> {
    let (program, args) = config
        .verification_command
        .split_first()
        .ok_or_else(|| Error::InvalidState("checkpoint requires a verification_command".into()))?;
    let output = tokio::process::Command::new(program)
        .args(args)
        .env("ZINCIR_CHECKPOINT_JSON", serde_json::to_string(state)?)
        .output()
        .await?;
    Ok(serde_json::json!({
        "passed": output.status.success(),
        "command": config.verification_command,
        "exit_code": output.status.code(),
        "stdout": String::from_utf8_lossy(&output.stdout),
        "stderr": String::from_utf8_lossy(&output.stderr)
    }))
}

fn json_checkpoint_result(accepted: bool, verification: Option<&Value>) -> Value {
    serde_json::json!({
        "accepted": accepted,
        "verification": verification.cloned().unwrap_or(Value::Null)
    })
}

fn invalid_checkpoint_result(error: &str) -> Value {
    serde_json::json!({ "accepted": false, "error": error })
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
                tool_calls: vec![ToolCall {
                    id: "checkpoint_1".into(),
                    name: "submit_checkpoint".into(),
                    args: json!({
                        "completed": ["done"],
                        "remaining": [],
                        "artifacts": [],
                        "failed_attempts": [],
                        "evidence": ["test"]
                    }),
                }],
                stop_reason: "tool_use".into(),
            })
        }
    }

    struct CheckpointProvider {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl LLMProvider for CheckpointProvider {
        async fn complete(
            &self,
            messages: Vec<LlmMessage>,
            _tools: Vec<crate::provider::ToolSchema>,
        ) -> Result<crate::provider::Response> {
            let round = self.calls.fetch_add(1, Ordering::SeqCst);
            if round == 1 {
                assert_eq!(messages.len(), 2);
                assert!(messages[1]
                    .content
                    .as_str()
                    .unwrap()
                    .contains("Latest accepted checkpoint"));
            }
            Ok(crate::provider::Response {
                content: json!("checkpoint"),
                tool_calls: vec![ToolCall {
                    id: format!("checkpoint_{}", round + 1),
                    name: "submit_checkpoint".into(),
                    args: json!({
                        "completed": [format!("round {}", round + 1)],
                        "remaining": if round == 0 { json!(["finish"]) } else { json!([]) },
                        "artifacts": [],
                        "failed_attempts": [],
                        "evidence": ["verifier command"]
                    }),
                }],
                stop_reason: "tool_use".into(),
            })
        }
    }

    struct InvalidThenValidCheckpointProvider {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl LLMProvider for InvalidThenValidCheckpointProvider {
        async fn complete(
            &self,
            messages: Vec<LlmMessage>,
            _tools: Vec<crate::provider::ToolSchema>,
        ) -> Result<crate::provider::Response> {
            let attempt = self.calls.fetch_add(1, Ordering::SeqCst);
            let args = if attempt == 0 {
                json!({ "completed": [] })
            } else {
                assert_eq!(messages.last().unwrap().role, "tool");
                assert_eq!(messages.last().unwrap().content["accepted"], false);
                json!({
                    "completed": ["task"],
                    "remaining": [],
                    "artifacts": [],
                    "failed_attempts": ["invalid checkpoint"],
                    "evidence": ["test"]
                })
            };
            Ok(crate::provider::Response {
                content: json!("checkpoint"),
                tool_calls: vec![ToolCall {
                    id: format!("checkpoint_{}", attempt + 1),
                    name: "submit_checkpoint".into(),
                    args,
                }],
                stop_reason: "tool_use".into(),
            })
        }
    }

    struct NeverProvider;

    #[async_trait]
    impl LLMProvider for NeverProvider {
        async fn complete(
            &self,
            _messages: Vec<LlmMessage>,
            _tools: Vec<crate::provider::ToolSchema>,
        ) -> Result<crate::provider::Response> {
            panic!("provider must not run when a persisted checkpoint completes the run")
        }
    }

    struct CountingTools {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl ToolExecutor for CountingTools {
        async fn execute(&self, call: ToolCall) -> Result<crate::types::ToolResult> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(crate::types::ToolResult {
                call_id: call.id,
                content: json!({ "ok": true }),
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
    async fn verifier_rejects_a_nonzero_exit_status() {
        let result = run_verifier(
            &RunConfig {
                model: "stub".into(),
                temperature: None,
                max_tokens: None,
                system: "test".into(),
                input: "test".into(),
                tools: vec![],
                verification_command: vec!["false".into()],
            },
            &CheckpointState {
                completed: vec![],
                remaining: vec!["work".into()],
                artifacts: vec![],
                failed_attempts: vec![],
                evidence: vec![],
            },
        )
        .await
        .unwrap();

        assert_eq!(result["passed"], false);
        assert_eq!(result["exit_code"], 1);
    }

    #[tokio::test]
    async fn accepted_checkpoint_starts_a_fresh_context() {
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
                "input": "finish the task",
                "tools": [],
                "verification_command": ["true"]
            }),
        )
        .await
        .unwrap();
        let provider = Arc::new(CheckpointProvider {
            calls: AtomicUsize::new(0),
        });
        let runtime = Runtime::new(pool.clone(), provider.clone(), Arc::new(UnusedTools));

        runtime.run(run.id).await.unwrap();

        let checkpoint_count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM checkpoints WHERE run_id = ?")
                .bind(run.id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
        assert_eq!(checkpoint_count, 2);
        assert_eq!(
            db::get_run(&pool, run.id).await.unwrap().status,
            RunStatus::Completed
        );
        pool.close().await;
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn invalid_checkpoint_is_returned_to_the_agent_for_retry() {
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
                "input": "finish the task",
                "tools": [],
                "verification_command": ["true"]
            }),
        )
        .await
        .unwrap();
        let provider = Arc::new(InvalidThenValidCheckpointProvider {
            calls: AtomicUsize::new(0),
        });
        let runtime = Runtime::new(pool.clone(), provider.clone(), Arc::new(UnusedTools));

        runtime.run(run.id).await.unwrap();

        assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            db::get_run(&pool, run.id).await.unwrap().status,
            RunStatus::Completed
        );
        pool.close().await;
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn recovery_rejects_a_checkpoint_with_sibling_tool_calls() {
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
                "input": "finish the task",
                "tools": [],
                "verification_command": ["true"]
            }),
        )
        .await
        .unwrap();
        let lease = db::acquire_run_lease(&pool, run.id, Uuid::new_v4(), Duration::from_secs(30))
            .await
            .unwrap();
        db::update_run_status_with_lease(&pool, &lease, RunStatus::Running)
            .await
            .unwrap();
        let checkpoint = ToolCall {
            id: "mixed_checkpoint".into(),
            name: "submit_checkpoint".into(),
            args: json!({
                "completed": ["task"],
                "remaining": [],
                "artifacts": [],
                "failed_attempts": [],
                "evidence": ["test"]
            }),
        };
        let external = ToolCall {
            id: "external_call".into(),
            name: "write_file".into(),
            args: json!({}),
        };
        let llm_payload = serde_json::to_value(LlmCallPayload {
            content: json!("mixed tools"),
            tool_calls: vec![checkpoint.clone(), external.clone()],
            stop_reason: "tool_use".into(),
        })
        .unwrap();
        let intents = vec![
            (
                json!({
                    "call_id": checkpoint.id,
                    "name": checkpoint.name,
                    "args": checkpoint.args,
                    "response_tool_count": 2
                }),
                "mixed_checkpoint".into(),
            ),
            (
                json!({
                    "call_id": external.id,
                    "name": external.name,
                    "args": external.args,
                    "response_tool_count": 2
                }),
                "external_call".into(),
            ),
        ];
        db::append_llm_call_with_tool_intents_with_lease(&pool, &lease, &llm_payload, &intents)
            .await
            .unwrap();
        db::release_run_lease(&pool, &lease).await.unwrap();
        let provider = Arc::new(CheckpointProvider {
            calls: AtomicUsize::new(0),
        });
        let tools = Arc::new(CountingTools {
            calls: AtomicUsize::new(0),
        });
        let runtime = Runtime::new(pool.clone(), provider.clone(), tools.clone());

        runtime.resume().await.unwrap();

        assert_eq!(tools.calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            db::get_run(&pool, run.id).await.unwrap().status,
            RunStatus::Completed
        );
        pool.close().await;
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn resume_verifies_a_persisted_checkpoint_candidate() {
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
                "input": "finish the task",
                "tools": [],
                "verification_command": ["true"]
            }),
        )
        .await
        .unwrap();
        let lease = db::acquire_run_lease(&pool, run.id, Uuid::new_v4(), Duration::from_secs(30))
            .await
            .unwrap();
        db::update_run_status_with_lease(&pool, &lease, RunStatus::Running)
            .await
            .unwrap();
        let call = ToolCall {
            id: "checkpoint_1".into(),
            name: "submit_checkpoint".into(),
            args: json!({
                "completed": ["task"],
                "remaining": [],
                "artifacts": [],
                "failed_attempts": [],
                "evidence": ["test"]
            }),
        };
        append_tool_call(&pool, &lease, &call).await.unwrap();
        db::propose_checkpoint_with_lease(
            &pool,
            &lease,
            &call.id,
            &serde_json::from_value(call.args).unwrap(),
        )
        .await
        .unwrap();
        db::release_run_lease(&pool, &lease).await.unwrap();
        let runtime = Runtime::new(pool.clone(), Arc::new(NeverProvider), Arc::new(UnusedTools));

        runtime.resume().await.unwrap();

        let checkpoint = db::get_latest_accepted_checkpoint(&pool, run.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(checkpoint.status, "accepted");
        assert_eq!(
            db::get_run(&pool, run.id).await.unwrap().status,
            RunStatus::Completed
        );
        pool.close().await;
        std::fs::remove_dir_all(directory).unwrap();
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
                "tools": [],
                "verification_command": ["true"]
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
