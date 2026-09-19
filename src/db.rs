use std::time::Duration;

use chrono::Utc;
use serde_json::{json, Value};
use sqlx::{SqliteConnection, SqlitePool};
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::types::{
    AgentRun, CheckpointRecord, CheckpointState, Event, EventType, RunLease, RunStatus, StepRecord,
    TimerRecord,
};

pub async fn create_run(
    pool: &SqlitePool,
    id: Uuid,
    parent_run_id: Option<Uuid>,
    role: &str,
    provider: &str,
    config: &Value,
) -> Result<AgentRun> {
    sqlx::query_as::<_, AgentRun>(
        "INSERT INTO agent_runs (id, parent_run_id, role, provider, config)
         VALUES (?, ?, ?, ?, ?)
         RETURNING *",
    )
    .bind(id)
    .bind(parent_run_id)
    .bind(role)
    .bind(provider)
    .bind(config)
    .fetch_one(pool)
    .await
    .map_err(Into::into)
}

pub async fn get_run(pool: &SqlitePool, id: Uuid) -> Result<AgentRun> {
    sqlx::query_as::<_, AgentRun>("SELECT * FROM agent_runs WHERE id = ?")
        .bind(id)
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| Error::NotFound(format!("agent_run {id}")))
}

pub async fn list_runs(pool: &SqlitePool) -> Result<Vec<AgentRun>> {
    sqlx::query_as::<_, AgentRun>("SELECT * FROM agent_runs ORDER BY updated_at DESC LIMIT 100")
        .fetch_all(pool)
        .await
        .map_err(Into::into)
}

pub async fn get_steps(pool: &SqlitePool, run_id: Uuid) -> Result<Vec<StepRecord>> {
    sqlx::query_as::<_, StepRecord>(
        "SELECT name, status, owner_id, result, started_at, completed_at
         FROM steps WHERE run_id = ? ORDER BY started_at",
    )
    .bind(run_id)
    .fetch_all(pool)
    .await
    .map_err(Into::into)
}

pub async fn get_timers(pool: &SqlitePool, run_id: Uuid) -> Result<Vec<TimerRecord>> {
    sqlx::query_as::<_, TimerRecord>(
        "SELECT name, wake_at_ms, completed_at FROM timers WHERE run_id = ? ORDER BY wake_at_ms",
    )
    .bind(run_id)
    .fetch_all(pool)
    .await
    .map_err(Into::into)
}

pub async fn acquire_run_lease(
    pool: &SqlitePool,
    run_id: Uuid,
    owner_id: Uuid,
    ttl: Duration,
) -> Result<RunLease> {
    claim_run_lease(pool, run_id, owner_id, ttl, false).await
}

/// Explicit recovery fences the previous owner. The caller must first confirm
/// that the process which held the lease is dead.
pub async fn recover_run_lease(
    pool: &SqlitePool,
    run_id: Uuid,
    owner_id: Uuid,
    ttl: Duration,
) -> Result<RunLease> {
    claim_run_lease(pool, run_id, owner_id, ttl, true).await
}

async fn claim_run_lease(
    pool: &SqlitePool,
    run_id: Uuid,
    owner_id: Uuid,
    ttl: Duration,
    recover: bool,
) -> Result<RunLease> {
    let ttl_ms = duration_to_millis(ttl)?;
    let mut connection = pool.acquire().await?;
    begin_immediate(&mut connection).await?;

    let result = async {
        let status = current_status(&mut connection, run_id).await?;
        if matches!(status, RunStatus::Completed | RunStatus::Failed) {
            return Err(Error::InvalidState(format!(
                "cannot lease terminal run {run_id}"
            )));
        }

        let expires_at_ms = Utc::now()
            .timestamp_millis()
            .checked_add(ttl_ms)
            .ok_or_else(|| Error::InvalidState("run lease duration is too large".into()))?;
        let epoch: Option<i64> = sqlx::query_scalar(
            "UPDATE agent_runs
             SET lease_owner = ?, lease_epoch = lease_epoch + 1,
                 lease_expires_at_ms = ?, updated_at = CURRENT_TIMESTAMP
             WHERE id = ? AND (? OR lease_owner IS NULL)
             RETURNING lease_epoch",
        )
        .bind(owner_id)
        .bind(expires_at_ms)
        .bind(run_id)
        .bind(recover)
        .fetch_optional(&mut *connection)
        .await?;

        let epoch = epoch.ok_or_else(|| {
            Error::InvalidState(format!(
                "run {run_id} already has an owner; explicit recovery required"
            ))
        })?;
        Ok(RunLease {
            run_id,
            owner_id,
            epoch,
            expires_at_ms,
        })
    }
    .await;

    finish_transaction(&mut connection, result).await
}

pub async fn renew_run_lease(
    pool: &SqlitePool,
    lease: &RunLease,
    ttl: Duration,
) -> Result<RunLease> {
    let ttl_ms = duration_to_millis(ttl)?;
    let mut connection = pool.acquire().await?;
    begin_immediate(&mut connection).await?;

    let result = async {
        let now_ms = Utc::now().timestamp_millis();
        let expires_at_ms = now_ms
            .checked_add(ttl_ms)
            .ok_or_else(|| Error::InvalidState("run lease duration is too large".into()))?;
        let rows = sqlx::query(
            "UPDATE agent_runs SET lease_expires_at_ms = ?, updated_at = CURRENT_TIMESTAMP
             WHERE id = ? AND lease_owner = ? AND lease_epoch = ?
               AND status IN ('pending', 'running') AND lease_expires_at_ms > ?",
        )
        .bind(expires_at_ms)
        .bind(lease.run_id)
        .bind(lease.owner_id)
        .bind(lease.epoch)
        .bind(now_ms)
        .execute(&mut *connection)
        .await?
        .rows_affected();
        if rows != 1 {
            return Err(Error::InvalidState(format!(
                "run lease {}:{} is no longer active",
                lease.run_id, lease.epoch
            )));
        }
        Ok(RunLease {
            expires_at_ms,
            ..*lease
        })
    }
    .await;

    finish_transaction(&mut connection, result).await
}

pub async fn release_run_lease(pool: &SqlitePool, lease: &RunLease) -> Result<()> {
    let rows = sqlx::query(
        "UPDATE agent_runs
         SET lease_owner = NULL, lease_expires_at_ms = NULL, updated_at = CURRENT_TIMESTAMP
         WHERE id = ? AND lease_owner = ? AND lease_epoch = ?",
    )
    .bind(lease.run_id)
    .bind(lease.owner_id)
    .bind(lease.epoch)
    .execute(pool)
    .await?
    .rows_affected();
    if rows != 1 {
        return Err(Error::InvalidState(format!(
            "run lease {}:{} is no longer owned by this worker",
            lease.run_id, lease.epoch
        )));
    }
    Ok(())
}

/// SQLite permits one writer at a time. BEGIN IMMEDIATE reserves that writer
/// slot before reading the next event sequence, so sequence allocation and
/// insertion are one atomic operation.
#[cfg(test)]
async fn update_run_status(pool: &SqlitePool, id: Uuid, status: RunStatus) -> Result<()> {
    let mut connection = pool.acquire().await?;
    begin_immediate(&mut connection).await?;

    let result = async {
        let current = current_status(&mut connection, id).await?;
        if current == status {
            return Ok(());
        }

        let seq = next_event_seq(&mut connection, id).await?;
        let payload = json!({ "from": current, "to": status });
        insert_event(
            &mut connection,
            id,
            seq,
            EventType::StateTransition,
            &payload,
            None,
        )
        .await?;
        sqlx::query(
            "UPDATE agent_runs SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        )
        .bind(status)
        .bind(id)
        .execute(&mut *connection)
        .await?;
        Ok(())
    }
    .await;

    finish_transaction(&mut connection, result).await
}

pub(crate) async fn update_run_status_with_lease(
    pool: &SqlitePool,
    lease: &RunLease,
    status: RunStatus,
) -> Result<()> {
    let mut connection = pool.acquire().await?;
    begin_immediate(&mut connection).await?;

    let result = async {
        validate_run_lease(&mut connection, lease).await?;
        let current = current_status(&mut connection, lease.run_id).await?;
        if current == status {
            return Ok(());
        }

        let seq = next_event_seq(&mut connection, lease.run_id).await?;
        let payload = json!({ "from": current, "to": status });
        insert_event(
            &mut connection,
            lease.run_id,
            seq,
            EventType::StateTransition,
            &payload,
            None,
        )
        .await?;
        sqlx::query(
            "UPDATE agent_runs SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        )
        .bind(status)
        .bind(lease.run_id)
        .execute(&mut *connection)
        .await?;
        Ok(())
    }
    .await;

    finish_transaction(&mut connection, result).await
}

pub async fn propose_checkpoint_with_lease(
    pool: &SqlitePool,
    lease: &RunLease,
    tool_call_id: &str,
    state: &CheckpointState,
) -> Result<CheckpointRecord> {
    let mut connection = pool.acquire().await?;
    begin_immediate(&mut connection).await?;

    let result = async {
        validate_writable_run_lease(&mut connection, lease).await?;
        let has_pending_intent: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                 SELECT 1 FROM events intent
                 WHERE intent.run_id = ?
                   AND intent.event_type = 'tool_call'
                   AND json_extract(intent.payload, '$.call_id') = ?
                   AND json_extract(intent.payload, '$.name') = 'submit_checkpoint'
                   AND COALESCE(json_extract(intent.payload, '$.response_tool_count'), 1) = 1
                   AND NOT EXISTS (
                       SELECT 1 FROM events result
                       WHERE result.run_id = intent.run_id
                         AND result.event_type = 'tool_result'
                         AND result.seq > intent.seq
                         AND json_extract(result.payload, '$.call_id') = json_extract(intent.payload, '$.call_id')
                   )
             )",
        )
        .bind(lease.run_id)
        .bind(tool_call_id)
        .fetch_one(&mut *connection)
        .await?;
        if !has_pending_intent {
            return Err(Error::InvalidState(format!(
                "checkpoint tool call {tool_call_id:?} is not a pending standalone intent"
            )));
        }

        let pending: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                 SELECT 1 FROM checkpoints WHERE run_id = ? AND status = 'candidate'
             )",
        )
        .bind(lease.run_id)
        .fetch_one(&mut *connection)
        .await?;
        if pending {
            return Err(Error::InvalidState(format!(
                "run {} already has a checkpoint candidate",
                lease.run_id
            )));
        }

        let round: i32 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(round), 0) + 1 FROM checkpoints WHERE run_id = ?",
        )
        .bind(lease.run_id)
        .fetch_one(&mut *connection)
        .await?;
        let proposed_event_seq = next_event_seq(&mut connection, lease.run_id).await?;
        let based_on_event_seq = proposed_event_seq - 1;
        let state = serde_json::to_value(state)?;
        let checkpoint = sqlx::query_as::<_, CheckpointRecord>(
            "INSERT INTO checkpoints (
                 run_id, round, tool_call_id, based_on_event_seq, status, state
             ) VALUES (?, ?, ?, ?, 'candidate', ?)
             RETURNING *",
        )
        .bind(lease.run_id)
        .bind(round)
        .bind(tool_call_id)
        .bind(based_on_event_seq)
        .bind(&state)
        .fetch_one(&mut *connection)
        .await?;
        insert_event(
            &mut connection,
            lease.run_id,
            proposed_event_seq,
            EventType::CheckpointProposed,
            &json!({ "checkpoint_id": checkpoint.id, "round": round, "state": state }),
            Some(&format!("checkpoint:{}:proposed", checkpoint.id)),
        )
        .await?;
        Ok(checkpoint)
    }
    .await;

    finish_transaction(&mut connection, result).await
}

pub async fn decide_checkpoint_with_lease(
    pool: &SqlitePool,
    lease: &RunLease,
    checkpoint_id: i64,
    accepted: bool,
    verification: &Value,
) -> Result<CheckpointRecord> {
    let mut connection = pool.acquire().await?;
    begin_immediate(&mut connection).await?;

    let result = async {
        validate_writable_run_lease(&mut connection, lease).await?;
        let candidate = sqlx::query_as::<_, CheckpointRecord>(
            "SELECT * FROM checkpoints
             WHERE id = ? AND run_id = ? AND status = 'candidate'",
        )
        .bind(checkpoint_id)
        .bind(lease.run_id)
        .fetch_optional(&mut *connection)
        .await?
        .ok_or_else(|| {
            Error::InvalidState(format!(
                "checkpoint {checkpoint_id} is not an active candidate"
            ))
        })?;
        let status = if accepted { "accepted" } else { "rejected" };
        let event_type = if accepted {
            EventType::CheckpointAccepted
        } else {
            EventType::CheckpointRejected
        };
        let decision_event_seq = next_event_seq(&mut connection, lease.run_id).await?;
        let checkpoint = sqlx::query_as::<_, CheckpointRecord>(
            "UPDATE checkpoints
             SET status = ?, verification = ?, decision_event_seq = ?,
                 decided_at = CURRENT_TIMESTAMP
             WHERE id = ? AND status = 'candidate'
             RETURNING *",
        )
        .bind(status)
        .bind(verification)
        .bind(decision_event_seq)
        .bind(checkpoint_id)
        .fetch_one(&mut *connection)
        .await?;
        insert_event(
            &mut connection,
            lease.run_id,
            decision_event_seq,
            event_type,
            &json!({ "checkpoint_id": checkpoint_id, "verification": verification }),
            Some(&format!("checkpoint:{checkpoint_id}:{status}")),
        )
        .await?;
        insert_event(
            &mut connection,
            lease.run_id,
            decision_event_seq + 1,
            EventType::ToolResult,
            &json!({
                "call_id": candidate.tool_call_id,
                "content": { "accepted": accepted, "verification": verification }
            }),
            Some(&format!("checkpoint:{checkpoint_id}:tool_result")),
        )
        .await?;
        Ok(checkpoint)
    }
    .await;

    finish_transaction(&mut connection, result).await
}

pub async fn get_checkpoint_candidate_with_lease(
    pool: &SqlitePool,
    lease: &RunLease,
    checkpoint_id: i64,
) -> Result<CheckpointRecord> {
    let mut connection = pool.acquire().await?;
    begin_immediate(&mut connection).await?;
    let result = async {
        validate_writable_run_lease(&mut connection, lease).await?;
        sqlx::query_as::<_, CheckpointRecord>(
            "SELECT * FROM checkpoints
             WHERE id = ? AND run_id = ? AND status = 'candidate'",
        )
        .bind(checkpoint_id)
        .bind(lease.run_id)
        .fetch_optional(&mut *connection)
        .await?
        .ok_or_else(|| {
            Error::InvalidState(format!(
                "checkpoint {checkpoint_id} is not an active candidate"
            ))
        })
    }
    .await;
    finish_transaction(&mut connection, result).await
}

pub async fn get_pending_checkpoint_with_lease(
    pool: &SqlitePool,
    lease: &RunLease,
) -> Result<Option<CheckpointRecord>> {
    let mut connection = pool.acquire().await?;
    begin_immediate(&mut connection).await?;
    let result = async {
        validate_writable_run_lease(&mut connection, lease).await?;
        sqlx::query_as::<_, CheckpointRecord>(
            "SELECT * FROM checkpoints
             WHERE run_id = ? AND status = 'candidate'
             ORDER BY round DESC LIMIT 1",
        )
        .bind(lease.run_id)
        .fetch_optional(&mut *connection)
        .await
        .map_err(Into::into)
    }
    .await;
    finish_transaction(&mut connection, result).await
}

pub async fn get_pending_checkpoint(
    pool: &SqlitePool,
    run_id: Uuid,
) -> Result<Option<CheckpointRecord>> {
    sqlx::query_as::<_, CheckpointRecord>(
        "SELECT * FROM checkpoints
         WHERE run_id = ? AND status = 'candidate'
         ORDER BY round DESC LIMIT 1",
    )
    .bind(run_id)
    .fetch_optional(pool)
    .await
    .map_err(Into::into)
}

pub async fn get_latest_accepted_checkpoint(
    pool: &SqlitePool,
    run_id: Uuid,
) -> Result<Option<CheckpointRecord>> {
    sqlx::query_as::<_, CheckpointRecord>(
        "SELECT * FROM checkpoints
         WHERE run_id = ? AND status = 'accepted'
         ORDER BY round DESC LIMIT 1",
    )
    .bind(run_id)
    .fetch_optional(pool)
    .await
    .map_err(Into::into)
}

pub async fn complete_run_with_lease(pool: &SqlitePool, lease: &RunLease) -> Result<()> {
    let mut connection = pool.acquire().await?;
    begin_immediate(&mut connection).await?;
    let result = async {
        validate_writable_run_lease(&mut connection, lease).await?;
        let seq = next_event_seq(&mut connection, lease.run_id).await?;
        insert_event(
            &mut connection,
            lease.run_id,
            seq,
            EventType::StateTransition,
            &json!({ "from": RunStatus::Running, "to": RunStatus::Completed }),
            None,
        )
        .await?;
        sqlx::query(
            "UPDATE agent_runs
             SET status = 'completed', lease_owner = NULL, lease_expires_at_ms = NULL,
                 updated_at = CURRENT_TIMESTAMP
             WHERE id = ?",
        )
        .bind(lease.run_id)
        .execute(&mut *connection)
        .await?;
        Ok(())
    }
    .await;
    finish_transaction(&mut connection, result).await
}

pub async fn list_inflight_runs(pool: &SqlitePool) -> Result<Vec<AgentRun>> {
    sqlx::query_as::<_, AgentRun>(
        "SELECT * FROM agent_runs
         WHERE status IN ('pending', 'running')
         ORDER BY created_at",
    )
    .fetch_all(pool)
    .await
    .map_err(Into::into)
}

#[cfg(test)]
async fn append_event(
    pool: &SqlitePool,
    run_id: Uuid,
    event_type: EventType,
    payload: &Value,
    idempotency_key: Option<&str>,
) -> Result<Event> {
    let mut connection = pool.acquire().await?;
    begin_immediate(&mut connection).await?;

    let result = async {
        current_status(&mut connection, run_id).await?;
        let seq = next_event_seq(&mut connection, run_id).await?;
        insert_event(
            &mut connection,
            run_id,
            seq,
            event_type,
            payload,
            idempotency_key,
        )
        .await
    }
    .await;

    finish_transaction(&mut connection, result).await
}

pub async fn append_llm_call_with_tool_intents_with_lease(
    pool: &SqlitePool,
    lease: &RunLease,
    llm_payload: &Value,
    tool_intents: &[(Value, String)],
) -> Result<()> {
    let mut connection = pool.acquire().await?;
    begin_immediate(&mut connection).await?;

    let result = async {
        validate_writable_run_lease(&mut connection, lease).await?;
        let mut seq = next_event_seq(&mut connection, lease.run_id).await?;
        insert_event(
            &mut connection,
            lease.run_id,
            seq,
            EventType::LlmCall,
            llm_payload,
            None,
        )
        .await?;
        for (payload, call_id) in tool_intents {
            seq += 1;
            let idempotency_key = format!("tool_call:{call_id}");
            insert_event(
                &mut connection,
                lease.run_id,
                seq,
                EventType::ToolCall,
                payload,
                Some(&idempotency_key),
            )
            .await?;
        }
        Ok(())
    }
    .await;

    finish_transaction(&mut connection, result).await
}

pub async fn record_tool_result_with_lease(
    pool: &SqlitePool,
    lease: &RunLease,
    call_id: &str,
    content: &Value,
) -> Result<Event> {
    let mut connection = pool.acquire().await?;
    begin_immediate(&mut connection).await?;

    let result = async {
        validate_writable_run_lease(&mut connection, lease).await?;
        let intent: Option<(i32, String)> = sqlx::query_as(
            "SELECT seq, json_extract(payload, '$.name') FROM events
             WHERE run_id = ? AND event_type = 'tool_call'
               AND json_extract(payload, '$.call_id') = ?",
        )
        .bind(lease.run_id)
        .bind(call_id)
        .fetch_optional(&mut *connection)
        .await?;
        let (intent_seq, tool_name) = intent.ok_or_else(|| {
            Error::InvalidState(format!("tool call {call_id:?} has no durable intent"))
        })?;
        if tool_name == "submit_checkpoint" {
            return Err(Error::InvalidState(
                "checkpoint results must be produced by checkpoint verification".into(),
            ));
        }
        let payload = json!({ "call_id": call_id, "content": content });
        let existing = sqlx::query_as::<_, Event>(
            "SELECT * FROM events
             WHERE run_id = ? AND event_type = 'tool_result'
               AND json_extract(payload, '$.call_id') = ?
             ORDER BY seq LIMIT 1",
        )
        .bind(lease.run_id)
        .bind(call_id)
        .fetch_optional(&mut *connection)
        .await?;
        if let Some(existing) = existing {
            if existing.seq <= intent_seq || existing.payload != payload {
                return Err(Error::InvalidState(format!(
                    "tool call {call_id:?} already has a conflicting result"
                )));
            }
            return Ok(existing);
        }

        let seq = next_event_seq(&mut connection, lease.run_id).await?;
        insert_event(
            &mut connection,
            lease.run_id,
            seq,
            EventType::ToolResult,
            &payload,
            Some(&format!("tool_result:{call_id}")),
        )
        .await
    }
    .await;

    finish_transaction(&mut connection, result).await
}

pub async fn append_event_with_lease(
    pool: &SqlitePool,
    lease: &RunLease,
    event_type: EventType,
    payload: &Value,
    idempotency_key: Option<&str>,
) -> Result<Event> {
    let mut connection = pool.acquire().await?;
    begin_immediate(&mut connection).await?;

    let result = async {
        validate_writable_run_lease(&mut connection, lease).await?;
        let seq = next_event_seq(&mut connection, lease.run_id).await?;
        insert_event(
            &mut connection,
            lease.run_id,
            seq,
            event_type,
            payload,
            idempotency_key,
        )
        .await
    }
    .await;

    finish_transaction(&mut connection, result).await
}

async fn validate_writable_run_lease(
    connection: &mut SqliteConnection,
    lease: &RunLease,
) -> Result<()> {
    validate_run_lease(connection, lease).await?;
    if current_status(connection, lease.run_id).await? != RunStatus::Running {
        return Err(Error::InvalidState(format!(
            "run {} is not writable",
            lease.run_id
        )));
    }
    Ok(())
}

async fn validate_run_lease(connection: &mut SqliteConnection, lease: &RunLease) -> Result<()> {
    let valid: bool = sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1 FROM agent_runs
             WHERE id = ? AND lease_owner = ? AND lease_epoch = ?
               AND lease_expires_at_ms > ?
         )",
    )
    .bind(lease.run_id)
    .bind(lease.owner_id)
    .bind(lease.epoch)
    .bind(Utc::now().timestamp_millis())
    .fetch_one(connection)
    .await?;
    if !valid {
        return Err(Error::InvalidState(format!(
            "run lease {}:{} is no longer active",
            lease.run_id, lease.epoch
        )));
    }
    Ok(())
}

async fn begin_immediate(connection: &mut SqliteConnection) -> Result<()> {
    sqlx::query("BEGIN IMMEDIATE")
        .execute(connection)
        .await
        .map(|_| ())
        .map_err(Into::into)
}

async fn finish_transaction<T>(connection: &mut SqliteConnection, result: Result<T>) -> Result<T> {
    match result {
        Ok(value) => {
            sqlx::query("COMMIT").execute(connection).await?;
            Ok(value)
        }
        Err(error) => {
            let _ = sqlx::query("ROLLBACK").execute(connection).await;
            Err(error)
        }
    }
}

async fn current_status(connection: &mut SqliteConnection, run_id: Uuid) -> Result<RunStatus> {
    sqlx::query_scalar("SELECT status FROM agent_runs WHERE id = ?")
        .bind(run_id)
        .fetch_optional(connection)
        .await?
        .ok_or_else(|| Error::NotFound(format!("agent_run {run_id}")))
}

async fn next_event_seq(connection: &mut SqliteConnection, run_id: Uuid) -> Result<i32> {
    sqlx::query_scalar("SELECT COALESCE(MAX(seq), -1) + 1 FROM events WHERE run_id = ?")
        .bind(run_id)
        .fetch_one(connection)
        .await
        .map_err(Into::into)
}

async fn insert_event(
    connection: &mut SqliteConnection,
    run_id: Uuid,
    seq: i32,
    event_type: EventType,
    payload: &Value,
    idempotency_key: Option<&str>,
) -> Result<Event> {
    sqlx::query_as::<_, Event>(
        "INSERT INTO events (run_id, seq, event_type, payload, idempotency_key)
         VALUES (?, ?, ?, ?, ?)
         RETURNING *",
    )
    .bind(run_id)
    .bind(seq)
    .bind(event_type)
    .bind(payload)
    .bind(idempotency_key)
    .fetch_one(connection)
    .await
    .map_err(Into::into)
}

pub async fn claim_step(
    pool: &SqlitePool,
    run_id: Uuid,
    name: &str,
    owner_id: Uuid,
) -> Result<Option<Value>> {
    let mut connection = pool.acquire().await?;
    begin_immediate(&mut connection).await?;

    let result = async {
        current_status(&mut connection, run_id).await?;
        let existing: Option<(String, Option<Value>)> =
            sqlx::query_as("SELECT status, result FROM steps WHERE run_id = ? AND name = ?")
                .bind(run_id)
                .bind(name)
                .fetch_optional(&mut *connection)
                .await?;

        match existing {
            Some((status, Some(result))) if status == "completed" => Ok(Some(result)),
            Some((status, _)) if status == "running" => Err(Error::InvalidState(format!(
                "step {name:?} for run {run_id} is already running"
            ))),
            Some(_) => {
                sqlx::query(
                    "UPDATE steps
                     SET status = 'running', owner_id = ?, started_at = CURRENT_TIMESTAMP
                     WHERE run_id = ? AND name = ? AND status = 'pending'",
                )
                .bind(owner_id)
                .bind(run_id)
                .bind(name)
                .execute(&mut *connection)
                .await?;
                Ok(None)
            }
            None => {
                sqlx::query(
                    "INSERT INTO steps (run_id, name, status, owner_id)
                     VALUES (?, ?, 'running', ?)",
                )
                .bind(run_id)
                .bind(name)
                .bind(owner_id)
                .execute(&mut *connection)
                .await?;
                Ok(None)
            }
        }
    }
    .await;

    finish_transaction(&mut connection, result).await
}

pub async fn complete_step(
    pool: &SqlitePool,
    run_id: Uuid,
    name: &str,
    owner_id: Uuid,
    result: &Value,
) -> Result<()> {
    let mut connection = pool.acquire().await?;
    begin_immediate(&mut connection).await?;

    let completion = async {
        let rows = sqlx::query(
            "UPDATE steps
             SET status = 'completed', result = ?, completed_at = CURRENT_TIMESTAMP
             WHERE run_id = ? AND name = ? AND status = 'running' AND owner_id = ?",
        )
        .bind(result)
        .bind(run_id)
        .bind(name)
        .bind(owner_id)
        .execute(&mut *connection)
        .await?
        .rows_affected();
        if rows != 1 {
            return Err(Error::InvalidState(format!(
                "cannot complete step {name:?} for run {run_id}: claim was lost"
            )));
        }
        Ok(())
    }
    .await;

    finish_transaction(&mut connection, completion).await
}

fn duration_to_millis(delay: Duration) -> Result<i64> {
    let milliseconds = delay.as_millis();
    let rounded = if delay.subsec_nanos().is_multiple_of(1_000_000) {
        milliseconds
    } else {
        milliseconds
            .checked_add(1)
            .ok_or_else(|| Error::InvalidState("timer delay is too large".into()))?
    };
    i64::try_from(rounded).map_err(|_| Error::InvalidState("timer delay is too large".into()))
}

pub async fn schedule_timer(
    pool: &SqlitePool,
    run_id: Uuid,
    name: &str,
    delay: Duration,
) -> Result<i64> {
    let delay_ms = duration_to_millis(delay)?;
    let wake_at_ms = Utc::now()
        .timestamp_millis()
        .checked_add(delay_ms)
        .ok_or_else(|| Error::InvalidState("timer wake time is too large".into()))?;
    let mut connection = pool.acquire().await?;
    begin_immediate(&mut connection).await?;

    let timer = async {
        current_status(&mut connection, run_id).await?;
        if let Some(existing) =
            sqlx::query_scalar("SELECT wake_at_ms FROM timers WHERE run_id = ? AND name = ?")
                .bind(run_id)
                .bind(name)
                .fetch_optional(&mut *connection)
                .await?
        {
            return Ok(existing);
        }

        sqlx::query("INSERT INTO timers (run_id, name, wake_at_ms) VALUES (?, ?, ?)")
            .bind(run_id)
            .bind(name)
            .bind(wake_at_ms)
            .execute(&mut *connection)
            .await?;
        Ok(wake_at_ms)
    }
    .await;

    finish_transaction(&mut connection, timer).await
}

pub async fn complete_timer(pool: &SqlitePool, run_id: Uuid, name: &str) -> Result<()> {
    sqlx::query(
        "UPDATE timers
         SET completed_at = COALESCE(completed_at, CURRENT_TIMESTAMP)
         WHERE run_id = ? AND name = ?",
    )
    .bind(run_id)
    .bind(name)
    .execute(pool)
    .await?;
    Ok(())
}

/// Releases interrupted running steps after the caller has confirmed that the
/// previous workflow process is no longer active.
pub async fn recover_steps(pool: &SqlitePool, run_id: Uuid) -> Result<()> {
    let mut connection = pool.acquire().await?;
    begin_immediate(&mut connection).await?;

    let recovery = async {
        current_status(&mut connection, run_id).await?;
        sqlx::query(
            "UPDATE steps
             SET status = 'pending', owner_id = NULL
             WHERE run_id = ? AND status = 'running'",
        )
        .bind(run_id)
        .execute(&mut *connection)
        .await?;
        Ok(())
    }
    .await;

    finish_transaction(&mut connection, recovery).await
}

pub async fn get_events(pool: &SqlitePool, run_id: Uuid) -> Result<Vec<Event>> {
    sqlx::query_as::<_, Event>("SELECT * FROM events WHERE run_id = ? ORDER BY seq")
        .bind(run_id)
        .fetch_all(pool)
        .await
        .map_err(Into::into)
}

pub async fn find_pending_tool_calls_with_lease(
    pool: &SqlitePool,
    lease: &RunLease,
) -> Result<Vec<Event>> {
    let mut connection = pool.acquire().await?;
    begin_immediate(&mut connection).await?;
    let result = async {
        validate_writable_run_lease(&mut connection, lease).await?;
        sqlx::query_as::<_, Event>(
            "SELECT e.* FROM events e
             WHERE e.run_id = ?
               AND e.event_type = 'tool_call'
               AND NOT EXISTS (
                 SELECT 1 FROM events e2
                 WHERE e2.run_id = e.run_id
                   AND e2.event_type = 'tool_result'
                   AND e2.seq > e.seq
                   AND json_extract(e2.payload, '$.call_id') = json_extract(e.payload, '$.call_id')
               )
             ORDER BY e.seq",
        )
        .bind(lease.run_id)
        .fetch_all(&mut *connection)
        .await
        .map_err(Into::into)
    }
    .await;
    finish_transaction(&mut connection, result).await
}

/// tool_call events with no matching tool_result — the crash window.
pub async fn find_pending_tool_calls(pool: &SqlitePool, run_id: Uuid) -> Result<Vec<Event>> {
    sqlx::query_as::<_, Event>(
        "SELECT e.* FROM events e
         WHERE e.run_id = ?
           AND e.event_type = 'tool_call'
           AND NOT EXISTS (
             SELECT 1 FROM events e2
             WHERE e2.run_id = e.run_id
               AND e2.event_type = 'tool_result'
               AND e2.seq > e.seq
               AND json_extract(e2.payload, '$.call_id') = json_extract(e.payload, '$.call_id')
           )
         ORDER BY e.seq",
    )
    .bind(run_id)
    .fetch_all(pool)
    .await
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
    use std::time::Duration;

    async fn test_pool() -> (SqlitePool, std::path::PathBuf) {
        let directory = std::env::temp_dir().join(format!("zincir-db-{}", Uuid::new_v4()));
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
        (pool, directory)
    }

    async fn test_run(pool: &SqlitePool) -> AgentRun {
        create_run(
            pool,
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
        .unwrap()
    }

    #[test]
    fn timer_delays_round_up_to_a_millisecond() {
        assert_eq!(duration_to_millis(Duration::from_nanos(1)).unwrap(), 1);
        assert_eq!(duration_to_millis(Duration::from_millis(1)).unwrap(), 1);
        assert_eq!(duration_to_millis(Duration::from_micros(1_001)).unwrap(), 2);
    }

    #[tokio::test]
    async fn provider_response_and_tool_intents_are_appended_atomically() {
        let (pool, directory) = test_pool().await;
        let run = test_run(&pool).await;
        let lease = acquire_run_lease(&pool, run.id, Uuid::new_v4(), Duration::from_secs(30))
            .await
            .unwrap();
        update_run_status_with_lease(&pool, &lease, RunStatus::Running)
            .await
            .unwrap();
        let intents = vec![
            (
                json!({ "call_id": "call_1", "name": "one", "args": {} }),
                "call_1".into(),
            ),
            (
                json!({ "call_id": "call_2", "name": "two", "args": {} }),
                "call_2".into(),
            ),
        ];

        append_llm_call_with_tool_intents_with_lease(
            &pool,
            &lease,
            &json!({ "content": "tools", "tool_calls": [], "stop_reason": "tool_use" }),
            &intents,
        )
        .await
        .unwrap();

        let events = get_events(&pool, run.id).await.unwrap();
        assert_eq!(events.len(), 4);
        assert_eq!(events[1].event_type, EventType::LlmCall);
        assert_eq!(events[2].event_type, EventType::ToolCall);
        assert_eq!(events[3].event_type, EventType::ToolCall);
        assert_eq!(
            events[2].idempotency_key.as_deref(),
            Some("tool_call:call_1")
        );
        assert_eq!(
            events[3].idempotency_key.as_deref(),
            Some("tool_call:call_2")
        );
        pool.close().await;
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn accepted_checkpoint_records_evidence_and_event_lineage() {
        let (pool, directory) = test_pool().await;
        let run = test_run(&pool).await;
        let lease = acquire_run_lease(&pool, run.id, Uuid::new_v4(), Duration::from_secs(30))
            .await
            .unwrap();
        update_run_status_with_lease(&pool, &lease, RunStatus::Running)
            .await
            .unwrap();
        append_llm_call_with_tool_intents_with_lease(
            &pool,
            &lease,
            &json!({ "content": "checkpoint", "tool_calls": [], "stop_reason": "tool_use" }),
            &[(
                json!({
                    "call_id": "checkpoint_1",
                    "name": "submit_checkpoint",
                    "args": {},
                    "response_tool_count": 1
                }),
                "checkpoint_1".into(),
            )],
        )
        .await
        .unwrap();
        let state = crate::types::CheckpointState {
            completed: vec!["inspected repository".into()],
            remaining: vec!["implement change".into()],
            artifacts: vec!["git:abc123".into()],
            failed_attempts: vec![],
            evidence: vec!["cargo test".into()],
        };

        let candidate = propose_checkpoint_with_lease(&pool, &lease, "checkpoint_1", &state)
            .await
            .unwrap();
        let accepted = decide_checkpoint_with_lease(
            &pool,
            &lease,
            candidate.id,
            true,
            &json!({ "command": ["cargo", "test"], "exit_code": 0 }),
        )
        .await
        .unwrap();
        let latest = get_latest_accepted_checkpoint(&pool, run.id)
            .await
            .unwrap()
            .unwrap();
        let events = get_events(&pool, run.id).await.unwrap();

        assert_eq!(accepted.status, "accepted");
        assert_eq!(latest.id, accepted.id);
        assert_eq!(latest.state, serde_json::to_value(state).unwrap());
        assert_eq!(latest.based_on_event_seq, 2);
        assert_eq!(latest.decision_event_seq, Some(4));
        assert_eq!(events[3].event_type, EventType::CheckpointProposed);
        assert_eq!(events[4].event_type, EventType::CheckpointAccepted);
        assert_eq!(events[5].event_type, EventType::ToolResult);
        pool.close().await;
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn resolved_checkpoint_intent_cannot_be_proposed() {
        let (pool, directory) = test_pool().await;
        let run = test_run(&pool).await;
        let lease = acquire_run_lease(&pool, run.id, Uuid::new_v4(), Duration::from_secs(30))
            .await
            .unwrap();
        update_run_status_with_lease(&pool, &lease, RunStatus::Running)
            .await
            .unwrap();
        append_llm_call_with_tool_intents_with_lease(
            &pool,
            &lease,
            &json!({ "content": "invalid checkpoint", "tool_calls": [], "stop_reason": "tool_use" }),
            &[(
                json!({
                    "call_id": "checkpoint_1",
                    "name": "submit_checkpoint",
                    "args": {},
                    "response_tool_count": 1
                }),
                "checkpoint_1".into(),
            )],
        )
        .await
        .unwrap();
        append_event_with_lease(
            &pool,
            &lease,
            EventType::ToolResult,
            &json!({ "call_id": "checkpoint_1", "content": { "accepted": false } }),
            Some("checkpoint_error:checkpoint_1"),
        )
        .await
        .unwrap();

        let result = propose_checkpoint_with_lease(
            &pool,
            &lease,
            "checkpoint_1",
            &CheckpointState {
                completed: vec![],
                remaining: vec!["work".into()],
                artifacts: vec![],
                failed_attempts: vec![],
                evidence: vec![],
            },
        )
        .await;

        assert!(matches!(result, Err(Error::InvalidState(_))));
        pool.close().await;
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn completion_records_terminal_state_and_releases_lease_atomically() {
        let (pool, directory) = test_pool().await;
        let run = test_run(&pool).await;
        let lease = acquire_run_lease(&pool, run.id, Uuid::new_v4(), Duration::from_secs(30))
            .await
            .unwrap();
        update_run_status_with_lease(&pool, &lease, RunStatus::Running)
            .await
            .unwrap();

        complete_run_with_lease(&pool, &lease).await.unwrap();

        let row: (RunStatus, Option<Uuid>, Option<i64>) = sqlx::query_as(
            "SELECT status, lease_owner, lease_expires_at_ms FROM agent_runs WHERE id = ?",
        )
        .bind(run.id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row, (RunStatus::Completed, None, None));
        let stale_write = append_event_with_lease(
            &pool,
            &lease,
            EventType::LlmCall,
            &json!({ "late": true }),
            None,
        )
        .await;
        assert!(matches!(stale_write, Err(Error::InvalidState(_))));
        pool.close().await;
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn active_run_lease_blocks_another_owner() {
        let (pool, directory) = test_pool().await;
        let run = test_run(&pool).await;
        let owner = Uuid::new_v4();

        let lease = acquire_run_lease(&pool, run.id, owner, Duration::from_secs(30))
            .await
            .unwrap();
        let error = acquire_run_lease(&pool, run.id, Uuid::new_v4(), Duration::from_secs(30))
            .await
            .unwrap_err();

        assert_eq!(lease.run_id, run.id);
        assert_eq!(lease.owner_id, owner);
        assert_eq!(lease.epoch, 1);
        assert!(matches!(error, Error::InvalidState(_)));
        pool.close().await;
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn expired_owned_lease_requires_explicit_recovery() {
        let (pool, directory) = test_pool().await;
        let run = test_run(&pool).await;
        let first = acquire_run_lease(&pool, run.id, Uuid::new_v4(), Duration::from_secs(30))
            .await
            .unwrap();
        sqlx::query("UPDATE agent_runs SET lease_expires_at_ms = 0 WHERE id = ?")
            .bind(run.id)
            .execute(&pool)
            .await
            .unwrap();

        let blocked = acquire_run_lease(&pool, run.id, Uuid::new_v4(), Duration::from_secs(30))
            .await
            .unwrap_err();
        let recovered = recover_run_lease(&pool, run.id, Uuid::new_v4(), Duration::from_secs(30))
            .await
            .unwrap();

        assert!(matches!(blocked, Error::InvalidState(_)));
        assert_eq!(recovered.epoch, first.epoch + 1);
        pool.close().await;
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn active_run_lease_can_be_renewed() {
        let (pool, directory) = test_pool().await;
        let run = test_run(&pool).await;
        let lease = acquire_run_lease(&pool, run.id, Uuid::new_v4(), Duration::from_secs(30))
            .await
            .unwrap();

        let renewed = renew_run_lease(&pool, &lease, Duration::from_secs(60))
            .await
            .unwrap();

        assert_eq!(renewed.epoch, lease.epoch);
        assert!(renewed.expires_at_ms > lease.expires_at_ms);
        pool.close().await;
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn renewal_cannot_resurrect_a_lease_that_expires_behind_a_writer() {
        let (pool, directory) = test_pool().await;
        let run = test_run(&pool).await;
        let lease = acquire_run_lease(&pool, run.id, Uuid::new_v4(), Duration::from_millis(80))
            .await
            .unwrap();
        let mut blocker = pool.acquire().await.unwrap();
        begin_immediate(&mut blocker).await.unwrap();
        let renewal_pool = pool.clone();
        let renewal = tokio::spawn(async move {
            renew_run_lease(&renewal_pool, &lease, Duration::from_secs(1)).await
        });
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(120)).await;
        sqlx::query("COMMIT").execute(&mut *blocker).await.unwrap();

        let result = renewal.await.unwrap();

        assert!(matches!(result, Err(Error::InvalidState(_))));
        drop(blocker);
        pool.close().await;
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn released_run_lease_can_be_acquired_by_another_owner() {
        let (pool, directory) = test_pool().await;
        let run = test_run(&pool).await;
        let first = acquire_run_lease(&pool, run.id, Uuid::new_v4(), Duration::from_secs(30))
            .await
            .unwrap();

        release_run_lease(&pool, &first).await.unwrap();
        let second = acquire_run_lease(&pool, run.id, Uuid::new_v4(), Duration::from_secs(30))
            .await
            .unwrap();

        assert_eq!(second.epoch, first.epoch + 1);
        pool.close().await;
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn a_new_lease_fences_writes_from_the_previous_owner() {
        let (pool, directory) = test_pool().await;
        let run = test_run(&pool).await;
        let first = acquire_run_lease(&pool, run.id, Uuid::new_v4(), Duration::from_secs(30))
            .await
            .unwrap();
        update_run_status_with_lease(&pool, &first, RunStatus::Running)
            .await
            .unwrap();
        sqlx::query("UPDATE agent_runs SET lease_expires_at_ms = 0 WHERE id = ?")
            .bind(run.id)
            .execute(&pool)
            .await
            .unwrap();
        let second = recover_run_lease(&pool, run.id, Uuid::new_v4(), Duration::from_secs(30))
            .await
            .unwrap();

        let stale = append_event_with_lease(
            &pool,
            &first,
            EventType::LlmCall,
            &json!({ "writer": "stale" }),
            None,
        )
        .await
        .unwrap_err();
        append_event_with_lease(
            &pool,
            &second,
            EventType::LlmCall,
            &json!({ "writer": "current" }),
            None,
        )
        .await
        .unwrap();

        assert_eq!(second.epoch, first.epoch + 1);
        assert!(matches!(stale, Error::InvalidState(_)));
        assert_eq!(get_events(&pool, run.id).await.unwrap().len(), 2);
        pool.close().await;
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn stale_run_lease_cannot_change_status() {
        let (pool, directory) = test_pool().await;
        let run = test_run(&pool).await;
        let first = acquire_run_lease(&pool, run.id, Uuid::new_v4(), Duration::from_secs(30))
            .await
            .unwrap();
        sqlx::query("UPDATE agent_runs SET lease_expires_at_ms = 0 WHERE id = ?")
            .bind(run.id)
            .execute(&pool)
            .await
            .unwrap();
        recover_run_lease(&pool, run.id, Uuid::new_v4(), Duration::from_secs(30))
            .await
            .unwrap();

        let error = update_run_status_with_lease(&pool, &first, RunStatus::Running)
            .await
            .unwrap_err();

        assert!(matches!(error, Error::InvalidState(_)));
        assert_eq!(
            get_run(&pool, run.id).await.unwrap().status,
            RunStatus::Pending
        );
        pool.close().await;
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn concurrent_appends_get_unique_sequences() {
        let (pool, directory) = test_pool().await;
        let run = test_run(&pool).await;
        let first_payload = json!({ "writer": 1 });
        let second_payload = json!({ "writer": 2 });

        let (first, second) = tokio::join!(
            append_event(&pool, run.id, EventType::LlmCall, &first_payload, None),
            append_event(&pool, run.id, EventType::LlmCall, &second_payload, None),
        );
        let mut sequences = vec![first.unwrap().seq, second.unwrap().seq];
        sequences.sort_unstable();

        assert_eq!(sequences, vec![0, 1]);
        pool.close().await;
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn status_change_records_one_transition_atomically() {
        let (pool, directory) = test_pool().await;
        let run = test_run(&pool).await;

        update_run_status(&pool, run.id, RunStatus::Running)
            .await
            .unwrap();
        update_run_status(&pool, run.id, RunStatus::Running)
            .await
            .unwrap();

        let updated = get_run(&pool, run.id).await.unwrap();
        let events = get_events(&pool, run.id).await.unwrap();

        assert_eq!(updated.status, RunStatus::Running);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, EventType::StateTransition);
        assert_eq!(
            events[0].payload,
            json!({ "from": "pending", "to": "running" })
        );
        pool.close().await;
        std::fs::remove_dir_all(directory).unwrap();
    }
}
