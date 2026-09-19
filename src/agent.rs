use std::{
    future::Future,
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::SqlitePool;
use tokio::{
    sync::{oneshot, Mutex, OwnedMutexGuard},
    task::JoinHandle,
};
use uuid::Uuid;

use crate::{
    db,
    error::{Error, Result},
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

const MIN_LEASE_TTL: Duration = Duration::from_millis(30);

pub struct AgentContext {
    pool: SqlitePool,
    run_id: Uuid,
    lease: RunLease,
    lease_ttl: Duration,
    heartbeat: LeaseHeartbeat,
}

struct LeaseHeartbeat {
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
    failure: Arc<StdMutex<Option<String>>>,
    gate: Arc<Mutex<()>>,
}

impl LeaseHeartbeat {
    fn start(pool: SqlitePool, lease: RunLease, ttl: Duration) -> Self {
        let interval = ttl
            .checked_div(3)
            .filter(|interval| !interval.is_zero())
            .unwrap_or(Duration::from_millis(1));
        let (stop, mut stopped) = oneshot::channel();
        let failure = Arc::new(StdMutex::new(None));
        let gate = Arc::new(Mutex::new(()));
        let task_failure = Arc::clone(&failure);
        let task_gate = Arc::clone(&gate);
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(interval) => {
                        let _guard = task_gate.lock().await;
                        if let Err(error) = db::renew_run_lease(&pool, &lease, ttl).await {
                            *task_failure.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) =
                                Some(error.to_string());
                            break;
                        }
                    }
                    _ = &mut stopped => break,
                }
            }
        });
        Self {
            stop: Some(stop),
            task: Some(task),
            failure,
            gate,
        }
    }

    fn ensure_healthy(&self) -> Result<()> {
        if let Some(message) = self
            .failure
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
        {
            return Err(Error::InvalidState(format!(
                "lease heartbeat failed: {message}"
            )));
        }
        Ok(())
    }

    async fn stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            if let Err(error) = task.await {
                *self
                    .failure
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                    Some(format!("heartbeat task failed: {error}"));
            }
        }
    }
}

impl Drop for LeaseHeartbeat {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
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
        validate_lease_ttl(lease_ttl)?;
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
        validate_lease_ttl(lease_ttl)?;
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
        let heartbeat = LeaseHeartbeat::start(pool.clone(), lease, lease_ttl);
        Ok(Self {
            pool,
            run_id,
            lease,
            lease_ttl,
            heartbeat,
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
        let _guard = self.prepare_lease_operation().await?;
        db::append_llm_call_with_tool_intents_with_lease(
            &self.pool,
            &self.lease,
            &llm_payload,
            &intents,
        )
        .await
    }

    pub async fn pending_tool_calls(&mut self) -> Result<Vec<PendingToolCall>> {
        let _guard = self.prepare_lease_operation().await?;
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
        let _guard = self.prepare_lease_operation().await?;
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
        let candidate = {
            let _guard = self.prepare_lease_operation().await?;
            db::propose_checkpoint_with_lease(&self.pool, &self.lease, tool_call_id, &state).await?
        };
        self.verify_checkpoint(candidate, verifier).await
    }

    pub async fn pending_checkpoint(&mut self) -> Result<Option<CheckpointRecord>> {
        let _guard = self.prepare_lease_operation().await?;
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
        let candidate = {
            let _guard = self.prepare_lease_operation().await?;
            db::get_checkpoint_candidate_with_lease(&self.pool, &self.lease, candidate.id).await?
        };
        let state = serde_json::from_value(candidate.state.clone())?;
        let verification = verifier(state).await?;
        let _guard = self.prepare_lease_operation().await?;
        db::decide_checkpoint_with_lease(
            &self.pool,
            &self.lease,
            candidate.id,
            verification.passed,
            &serde_json::to_value(verification)?,
        )
        .await
    }

    pub async fn close(mut self) -> Result<()> {
        self.heartbeat.stop().await;
        let heartbeat = self.heartbeat.ensure_healthy();
        let release = db::release_run_lease(&self.pool, &self.lease).await;
        heartbeat.and(release)
    }

    pub async fn complete(mut self) -> Result<()> {
        self.heartbeat.stop().await;
        let result = async {
            self.heartbeat.ensure_healthy()?;
            self.lease = db::renew_run_lease(&self.pool, &self.lease, self.lease_ttl).await?;
            db::complete_run_with_lease(&self.pool, &self.lease).await
        }
        .await;
        if result.is_err() {
            let _ = db::release_run_lease(&self.pool, &self.lease).await;
        }
        result
    }

    pub async fn renew_lease(&mut self) -> Result<()> {
        let _guard = self.prepare_lease_operation().await?;
        Ok(())
    }

    async fn prepare_lease_operation(&mut self) -> Result<OwnedMutexGuard<()>> {
        let guard = Arc::clone(&self.heartbeat.gate).lock_owned().await;
        self.heartbeat.ensure_healthy()?;
        self.lease = db::renew_run_lease(&self.pool, &self.lease, self.lease_ttl).await?;
        Ok(guard)
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

fn validate_lease_ttl(ttl: Duration) -> Result<()> {
    if ttl < MIN_LEASE_TTL {
        return Err(Error::InvalidState(format!(
            "run lease TTL must be at least {} ms",
            MIN_LEASE_TTL.as_millis()
        )));
    }
    Ok(())
}
