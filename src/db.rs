use serde_json::{json, Value};
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::types::{AgentRun, Event, EventType, RunStatus};

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

pub async fn update_run_status(pool: &PgPool, id: Uuid, status: RunStatus) -> Result<()> {
    let mut transaction = pool.begin().await?;
    let current = lock_run(&mut transaction, id).await?;
    if current == status {
        transaction.commit().await?;
        return Ok(());
    }

    let seq = next_event_seq(&mut transaction, id).await?;
    let payload = json!({ "from": current, "to": status });
    insert_event(
        &mut transaction,
        id,
        seq,
        EventType::StateTransition,
        &payload,
        None,
    )
    .await?;
    sqlx::query("UPDATE agent_runs SET status = $1, updated_at = now() WHERE id = $2")
        .bind(status)
        .bind(id)
        .execute(&mut *transaction)
        .await?;
    transaction.commit().await?;
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
    event_type: EventType,
    payload: &Value,
    idempotency_key: Option<&str>,
) -> Result<Event> {
    let mut transaction = pool.begin().await?;
    lock_run(&mut transaction, run_id).await?;
    let seq = next_event_seq(&mut transaction, run_id).await?;
    let event = insert_event(
        &mut transaction,
        run_id,
        seq,
        event_type,
        payload,
        idempotency_key,
    )
    .await?;
    transaction.commit().await?;
    Ok(event)
}

async fn lock_run(connection: &mut PgConnection, run_id: Uuid) -> Result<RunStatus> {
    sqlx::query_scalar("SELECT status FROM agent_runs WHERE id = $1 FOR UPDATE")
        .bind(run_id)
        .fetch_optional(connection)
        .await?
        .ok_or_else(|| Error::NotFound(format!("agent_run {run_id}")))
}

async fn next_event_seq(connection: &mut PgConnection, run_id: Uuid) -> Result<i32> {
    sqlx::query_scalar("SELECT COALESCE(MAX(seq), -1) + 1 FROM events WHERE run_id = $1")
        .bind(run_id)
        .fetch_one(connection)
        .await
        .map_err(Into::into)
}

async fn insert_event(
    connection: &mut PgConnection,
    run_id: Uuid,
    seq: i32,
    event_type: EventType,
    payload: &Value,
    idempotency_key: Option<&str>,
) -> Result<Event> {
    sqlx::query_as::<_, Event>(
        "INSERT INTO events (run_id, seq, event_type, payload, idempotency_key)
         VALUES ($1, $2, $3, $4, $5)
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    async fn test_run(pool: &PgPool) -> AgentRun {
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

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires PostgreSQL"]
    async fn concurrent_appends_get_unique_sequences(pool: PgPool) {
        let run = test_run(&pool).await;
        let first_payload = json!({ "writer": 1 });
        let second_payload = json!({ "writer": 2 });

        let (first, second) = tokio::join!(
            append_event(&pool, run.id, EventType::LlmCall, &first_payload, None,),
            append_event(&pool, run.id, EventType::LlmCall, &second_payload, None,),
        );
        let mut sequences = vec![first.unwrap().seq, second.unwrap().seq];
        sequences.sort_unstable();

        assert_eq!(sequences, vec![0, 1]);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires PostgreSQL"]
    async fn status_change_records_one_transition_atomically(pool: PgPool) {
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
    }
}
