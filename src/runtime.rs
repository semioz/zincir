use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgPool;
use tracing::{info, instrument};
use uuid::Uuid;

use crate::db;
use crate::error::Result;
use crate::provider::{LlmMessage, LLMProvider};
use crate::tool::ToolExecutor;
use crate::types::{EventType, RunConfig, RunStatus, ToolCall};

// ---------------------------------------------------------------------------
// Payload shapes stored in the events table. Local to the runtime — the
// DB stores JSONB, these structs define the wire shape for serialize/replay.
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
    pool: PgPool,
    provider: Arc<dyn LLMProvider>,
    tools: Arc<dyn ToolExecutor>,
}

impl Runtime {
    pub fn new(
        pool: PgPool,
        provider: Arc<dyn LLMProvider>,
        tools: Arc<dyn ToolExecutor>,
    ) -> Self {
        Self {
            pool,
            provider,
            tools,
        }
    }

    /// Resume all inflight runs — the crash-recovery entrypoint.
    pub async fn resume(&self) -> Result<()> {
        let inflight = db::list_inflight_runs(&self.pool).await?;
        for run in &inflight {
            info!(run_id = %run.id, "resuming run");
            self.run(run.id).await?;
        }
        Ok(())
    }

    /// Run (or resume) a single agent loop to completion.
    #[instrument(skip(self))]
    pub async fn run(&self, run_id: Uuid) -> Result<()> {
        let run = db::get_run(&self.pool, run_id).await?;

        if matches!(run.status, RunStatus::Completed | RunStatus::Failed) {
            info!(run_id = %run_id, status = ?run.status, "run already terminal, skipping");
            return Ok(());
        }

        db::update_run_status(&self.pool, run_id, RunStatus::Running).await?;

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
            let result = self.tools.execute(call.clone()).await?;
            let payload = serde_json::to_value(ToolResultPayload {
                call_id: call.id.clone(),
                content: result.content.clone(),
            })?;
            let seq = db::next_event_seq(&self.pool, run_id).await?;
            db::append_event(&self.pool, run_id, seq, EventType::ToolResult, &payload, None)
                .await?;
            messages.push(LlmMessage::tool(&call.id, &result.content));
        }

        // --- Main loop ---
        loop {
            let response = self.provider.complete(messages.clone(), vec![]).await?;
            let payload = serde_json::to_value(LlmCallPayload {
                content: response.content.clone(),
                tool_calls: response.tool_calls.clone(),
                stop_reason: response.stop_reason.clone(),
            })?;
            let seq = db::next_event_seq(&self.pool, run_id).await?;
            db::append_event(&self.pool, run_id, seq, EventType::LlmCall, &payload, None)
                .await?;

            messages.push(LlmMessage::assistant(&response.content, &response.tool_calls));

            if response.tool_calls.is_empty() {
                db::update_run_status(&self.pool, run_id, RunStatus::Completed).await?;
                info!(run_id = %run_id, "run completed");
                break;
            }

            for call in &response.tool_calls {
                let intent = serde_json::to_value(ToolCallPayload {
                    call_id: call.id.clone(),
                    name: call.name.clone(),
                    args: call.args.clone(),
                })?;
                let seq = db::next_event_seq(&self.pool, run_id).await?;
                db::append_event(
                    &self.pool,
                    run_id,
                    seq,
                    EventType::ToolCall,
                    &intent,
                    Some(&call.id),
                )
                .await?;

                let result = self.tools.execute(call.clone()).await?;
                let result_payload = serde_json::to_value(ToolResultPayload {
                    call_id: call.id.clone(),
                    content: result.content.clone(),
                })?;
                let seq = db::next_event_seq(&self.pool, run_id).await?;
                db::append_event(
                    &self.pool,
                    run_id,
                    seq,
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
