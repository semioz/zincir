use std::time::Duration;

use chrono::Utc;
use serde_json::{json, Value};
use sqlx::{SqliteConnection, SqlitePool};
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::types::{AgentRun, Event, EventType, RunStatus};

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

/// SQLite permits one writer at a time. BEGIN IMMEDIATE reserves that writer
/// slot before reading the next event sequence, so sequence allocation and
/// insertion are one atomic operation.
pub async fn update_run_status(pool: &SqlitePool, id: Uuid, status: RunStatus) -> Result<()> {
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

pub async fn append_event(
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
               AND json_extract(e2.payload, '$.call_id') = e.idempotency_key
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
