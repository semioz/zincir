use std::{future::Future, time::Duration};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::SqlitePool;
use uuid::Uuid;

use crate::{
    db,
    error::Result,
    types::{AgentRun, CheckpointRecord, CheckpointState, Event, RunLease, RunStatus, ToolCall},
};

#[derive(Debug)]
pub struct AgentState {
    pub run: AgentRun,
    pub checkpoint: Option<CheckpointRecord>,
    pub events: Vec<Event>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Verification {
    pub passed: bool,
    pub evidence: Value,
}

#[derive(Debug, Clone)]
pub struct PendingToolCall {
    pub call: ToolCall,
    pub response_tool_count: usize,
}

pub struct AgentContext {
    pool: SqlitePool,
    run_id: Uuid,
    lease: RunLease,
    lease_ttl: Duration,
}

impl AgentContext {
    pub async fn create(
        pool: SqlitePool,
        parent_run_id: Option<Uuid>,
        role: &str,
        provider: &str,
        config: Value,
        lease_ttl: Duration,
    ) -> Result<Self> {
        let run = db::create_run(
            &pool,
            Uuid::new_v4(),
            parent_run_id,
            role,
            provider,
            &config,
        )
        .await?;
        Self::open(pool, run.id, lease_ttl).await
    }

    pub async fn open(pool: SqlitePool, run_id: Uuid, lease_ttl: Duration) -> Result<Self> {
        Self::claim(pool, run_id, lease_ttl, false).await
    }

    /// Explicit recovery fences the previous owner. Call only after confirming
    /// that its process is dead.
    pub async fn recover(pool: SqlitePool, run_id: Uuid, lease_ttl: Duration) -> Result<Self> {
        Self::claim(pool, run_id, lease_ttl, true).await
    }

    async fn claim(
        pool: SqlitePool,
        run_id: Uuid,
        lease_ttl: Duration,
        recover: bool,
    ) -> Result<Self> {
        let owner_id = Uuid::new_v4();
        let lease = if recover {
            db::recover_run_lease(&pool, run_id, owner_id, lease_ttl).await?
        } else {
            db::acquire_run_lease(&pool, run_id, owner_id, lease_ttl).await?
        };
        if let Err(error) =
            db::update_run_status_with_lease(&pool, &lease, RunStatus::Running).await
        {
            let _ = db::release_run_lease(&pool, &lease).await;
            return Err(error);
        }
        Ok(Self {
            pool,
            run_id,
            lease,
            lease_ttl,
        })
    }

    pub fn run_id(&self) -> Uuid {
        self.run_id
    }

    pub async fn state(&self) -> Result<AgentState> {
        let run = db::get_run(&self.pool, self.run_id).await?;
        let checkpoint = db::get_latest_accepted_checkpoint(&self.pool, self.run_id).await?;
        let after_seq = checkpoint
            .as_ref()
            .and_then(|checkpoint| checkpoint.decision_event_seq)
            .map(|seq| seq + 1)
            .unwrap_or(-1);
        let events = db::get_events(&self.pool, self.run_id)
            .await?
            .into_iter()
            .filter(|event| event.seq > after_seq)
            .collect();
        Ok(AgentState {
            run,
            checkpoint,
            events,
        })
    }

    pub async fn record_response(
        &mut self,
        content: Value,
        tool_calls: &[ToolCall],
        stop_reason: &str,
    ) -> Result<()> {
        self.renew_lease().await?;
        let llm_payload = serde_json::to_value(LlmCallPayload {
            content,
            tool_calls: tool_calls.to_vec(),
            stop_reason: stop_reason.into(),
        })?;
        let response_tool_count = tool_calls.len();
        let intents = tool_calls
            .iter()
            .map(|call| {
                Ok((
                    serde_json::to_value(ToolCallPayload {
                        call_id: call.id.clone(),
                        name: call.name.clone(),
                        args: call.args.clone(),
                        response_tool_count,
                    })?,
                    call.id.clone(),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        db::append_llm_call_with_tool_intents_with_lease(
            &self.pool,
            &self.lease,
            &llm_payload,
            &intents,
        )
        .await
    }

    pub async fn pending_tool_calls(&mut self) -> Result<Vec<PendingToolCall>> {
        self.renew_lease().await?;
        db::find_pending_tool_calls_with_lease(&self.pool, &self.lease)
            .await?
            .into_iter()
            .map(|event| {
                let payload: ToolCallPayload = serde_json::from_value(event.payload)?;
                Ok(PendingToolCall {
                    call: ToolCall {
                        id: payload.call_id,
                        name: payload.name,
                        args: payload.args,
                    },
                    response_tool_count: payload.response_tool_count,
                })
            })
            .collect()
    }

    pub async fn record_tool_result(&mut self, call_id: &str, content: Value) -> Result<()> {
        self.renew_lease().await?;
        db::record_tool_result_with_lease(&self.pool, &self.lease, call_id, &content).await?;
        Ok(())
    }

    pub async fn checkpoint<F, Fut>(
        &mut self,
        tool_call_id: &str,
        state: CheckpointState,
        verifier: F,
    ) -> Result<CheckpointRecord>
    where
        F: FnOnce(CheckpointState) -> Fut,
        Fut: Future<Output = Result<Verification>>,
    {
        let candidate =
            db::propose_checkpoint_with_lease(&self.pool, &self.lease, tool_call_id, &state)
                .await?;
        self.verify_checkpoint(candidate, verifier).await
    }

    pub async fn pending_checkpoint(&mut self) -> Result<Option<CheckpointRecord>> {
        self.renew_lease().await?;
        db::get_pending_checkpoint_with_lease(&self.pool, &self.lease).await
    }

    pub async fn verify_checkpoint<F, Fut>(
        &mut self,
        candidate: CheckpointRecord,
        verifier: F,
    ) -> Result<CheckpointRecord>
    where
        F: FnOnce(CheckpointState) -> Fut,
        Fut: Future<Output = Result<Verification>>,
    {
        self.renew_lease().await?;
        let candidate =
            db::get_checkpoint_candidate_with_lease(&self.pool, &self.lease, candidate.id).await?;
        let state = serde_json::from_value(candidate.state.clone())?;
        let verification = verifier(state).await?;
        db::decide_checkpoint_with_lease(
            &self.pool,
            &self.lease,
            candidate.id,
            verification.passed,
            &serde_json::to_value(verification)?,
        )
        .await
    }

    pub async fn close(self) -> Result<()> {
        db::release_run_lease(&self.pool, &self.lease).await
    }

    pub async fn complete(mut self) -> Result<()> {
        self.renew_lease().await?;
        db::complete_run_with_lease(&self.pool, &self.lease).await
    }

    pub async fn renew_lease(&mut self) -> Result<()> {
        self.lease = db::renew_run_lease(&self.pool, &self.lease, self.lease_ttl).await?;
        Ok(())
    }
}

#[derive(Serialize)]
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
