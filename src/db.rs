use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::types::{AgentRun, Event, EventType, Message, RunStatus};

// ---------------------------------------------------------------------------
// agent_runs
// ---------------------------------------------------------------------------

pub async fn create_run(
    pool: &PgPool,
    id: Uuid,
    parent_run_id: Option<Uuid>,
    role: &str,
    provider: &str,
    config: &Value,
) -> Result<AgentRun> {
    let run = sqlx::query_as::<_, AgentRun>(
        "INSERT INTO agent_runs (id, parent_run_id, role, provider, config)
         VALUES ($1, $2, $3, $4, $5)
         RETURNING *",
    )
    .bind(id)
    .bind(parent_run_id)
    .bind(role)
    .bind(provider)
    .bind(config)
    .fetch_one(pool)
    .await?;
    Ok(run)
}

pub async fn get_run(pool: &PgPool, id: Uuid) -> Result<AgentRun> {
    sqlx::query_as::<_, AgentRun>("SELECT * FROM agent_runs WHERE id = $1")
        .bind(id)
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| Error::NotFound(format!("agent_run {id}")))
}

pub async fn update_run_status(
    pool: &PgPool,
    id: Uuid,
    status: RunStatus,
) -> Result<()> {
    sqlx::query("UPDATE agent_runs SET status = $1, updated_at = now() WHERE id = $2")
        .bind(status)
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn list_inflight_runs(pool: &PgPool) -> Result<Vec<AgentRun>> {
    sqlx::query_as::<_, AgentRun>(
        "SELECT * FROM agent_runs
         WHERE status IN ('pending', 'running')
         ORDER BY created_at",
    )
    .fetch_all(pool)
    .await
    .map_err(Into::into)
}

// ---------------------------------------------------------------------------
// events
// ---------------------------------------------------------------------------

pub async fn append_event(
    pool: &PgPool,
    run_id: Uuid,
    seq: i32,
    event_type: EventType,
    payload: &Value,
    idempotency_key: Option<&str>,
) -> Result<Event> {
    let event = sqlx::query_as::<_, Event>(
        "INSERT INTO events (run_id, seq, event_type, payload, idempotency_key)
         VALUES ($1, $2, $3, $4, $5)
         RETURNING *",
    )
    .bind(run_id)
    .bind(seq)
    .bind(event_type)
    .bind(payload)
    .bind(idempotency_key)
    .fetch_one(pool)
    .await?;
    Ok(event)
}

/// Next per-run sequence number.
/// ponytail: read-modify-write is safe single-writer (v0.1 single-node).
/// Multi-writer would need SELECT ... FOR UPDATE or an advisory lock.
pub async fn next_event_seq(pool: &PgPool, run_id: Uuid) -> Result<i32> {
    let next: i32 =
        sqlx::query_scalar("SELECT COALESCE(MAX(seq), -1) + 1 FROM events WHERE run_id = $1")
            .bind(run_id)
            .fetch_one(pool)
            .await?;
    Ok(next)
}

pub async fn get_events(pool: &PgPool, run_id: Uuid) -> Result<Vec<Event>> {
    sqlx::query_as::<_, Event>("SELECT * FROM events WHERE run_id = $1 ORDER BY seq")
        .bind(run_id)
        .fetch_all(pool)
        .await
        .map_err(Into::into)
}

/// tool_call events with no matching tool_result — the crash window.
/// On resume these are re-executed (safe only if the tool is idempotent
/// or the idempotency_key is respected by the tool).
pub async fn find_pending_tool_calls(pool: &PgPool, run_id: Uuid) -> Result<Vec<Event>> {
    sqlx::query_as::<_, Event>(
        "SELECT e.* FROM events e
         WHERE e.run_id = $1
           AND e.event_type = 'tool_call'
           AND NOT EXISTS (
             SELECT 1 FROM events e2
             WHERE e2.run_id = e.run_id
               AND e2.event_type = 'tool_result'
               AND e2.payload->>'call_id' = e.idempotency_key
           )
         ORDER BY e.seq",
    )
    .bind(run_id)
    .fetch_all(pool)
    .await
    .map_err(Into::into)
}

// ---------------------------------------------------------------------------
// messages — v0.2 multi-agent. Queries exist so the schema is exercised.
// ---------------------------------------------------------------------------

pub async fn list_pending_messages(pool: &PgPool, to_run_id: Uuid) -> Result<Vec<Message>> {
    sqlx::query_as::<_, Message>(
        "SELECT * FROM messages WHERE to_run_id = $1 AND delivered = false ORDER BY id",
    )
    .bind(to_run_id)
    .fetch_all(pool)
    .await
    .map_err(Into::into)
}

pub async fn mark_message_delivered(pool: &PgPool, id: i64) -> Result<()> {
    sqlx::query("UPDATE messages SET delivered = true WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}
