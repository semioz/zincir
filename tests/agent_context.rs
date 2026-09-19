use std::time::Duration;

use serde_json::json;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use uuid::Uuid;
use zincir::{db, AgentContext, CheckpointState, Error, RunStatus, ToolCall, Verification};

async fn wait_for_heartbeat_failure(ctx: &mut AgentContext) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match ctx.renew_lease().await {
                Err(Error::InvalidState(message)) if message.contains("heartbeat") => return,
                _ => tokio::task::yield_now().await,
            }
        }
    })
    .await
    .expect("heartbeat did not report failure");
}

#[tokio::test]
async fn custom_agent_loop_uses_durable_checkpoint_api() {
    let directory = std::env::temp_dir().join(format!("zincir-agent-sdk-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let options = SqliteConnectOptions::new()
        .filename(directory.join("zincir.db"))
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
    let mut ctx = AgentContext::create(
        pool.clone(),
        None,
        "custom-agent",
        "bring-your-own-provider",
        json!({ "goal": "finish the task" }),
        Duration::from_secs(30),
    )
    .await
    .unwrap();
    let run_id = ctx.run_id();
    let checkpoint_call = ToolCall {
        id: "checkpoint_1".into(),
        name: "submit_checkpoint".into(),
        args: json!({}),
    };

    ctx.record_response(
        json!("work complete"),
        std::slice::from_ref(&checkpoint_call),
        "tool_use",
    )
    .await
    .unwrap();
    let checkpoint = ctx
        .checkpoint(
            &checkpoint_call.id,
            CheckpointState {
                completed: vec!["task".into()],
                remaining: vec![],
                artifacts: vec!["workspace:abc123".into()],
                failed_attempts: vec![],
                evidence: vec!["tests passed".into()],
            },
            |state| {
                let completed = state.remaining.is_empty();
                async move {
                    Ok(Verification {
                        passed: completed,
                        evidence: json!({ "command": ["cargo", "test"], "exit_code": 0 }),
                    })
                }
            },
        )
        .await
        .unwrap();

    assert_eq!(checkpoint.status, "accepted");
    let state = ctx.state().await.unwrap();
    assert_eq!(state.checkpoint.unwrap().id, checkpoint.id);
    assert!(state.events.is_empty());
    ctx.complete().await.unwrap();
    assert_eq!(
        db::get_run(&pool, run_id).await.unwrap().status,
        RunStatus::Completed
    );

    pool.close().await;
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn tool_results_require_a_pending_intent_and_are_idempotent() {
    let directory = std::env::temp_dir().join(format!("zincir-agent-sdk-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let options = SqliteConnectOptions::new()
        .filename(directory.join("zincir.db"))
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
    let mut ctx = AgentContext::create(
        pool.clone(),
        None,
        "custom-agent",
        "bring-your-own-provider",
        json!({ "goal": "finish the task" }),
        Duration::from_secs(30),
    )
    .await
    .unwrap();

    let missing = ctx
        .record_tool_result("call_1", json!({ "ok": true }))
        .await;
    assert!(matches!(missing, Err(Error::InvalidState(_))));

    let call = ToolCall {
        id: "call_1".into(),
        name: "custom_tool".into(),
        args: json!({}),
    };
    ctx.record_response(json!("use tool"), std::slice::from_ref(&call), "tool_use")
        .await
        .unwrap();
    assert_eq!(ctx.pending_tool_calls().await.unwrap().len(), 1);

    ctx.record_tool_result(&call.id, json!({ "ok": true }))
        .await
        .unwrap();
    ctx.record_tool_result(&call.id, json!({ "ok": true }))
        .await
        .unwrap();
    let conflicting = ctx
        .record_tool_result(&call.id, json!({ "ok": false }))
        .await;
    assert!(matches!(conflicting, Err(Error::InvalidState(_))));
    assert!(ctx.pending_tool_calls().await.unwrap().is_empty());

    let namespaced_calls = [
        ToolCall {
            id: "x".into(),
            name: "custom_tool".into(),
            args: json!({}),
        },
        ToolCall {
            id: "tool_result:x".into(),
            name: "custom_tool".into(),
            args: json!({}),
        },
    ];
    ctx.record_response(json!("two tools"), &namespaced_calls, "tool_use")
        .await
        .unwrap();
    ctx.record_tool_result("x", json!({ "ok": true }))
        .await
        .unwrap();
    ctx.record_tool_result("tool_result:x", json!({ "ok": true }))
        .await
        .unwrap();

    let checkpoint_call = ToolCall {
        id: "checkpoint_1".into(),
        name: "submit_checkpoint".into(),
        args: json!({}),
    };
    ctx.record_response(
        json!("checkpoint"),
        std::slice::from_ref(&checkpoint_call),
        "tool_use",
    )
    .await
    .unwrap();
    let bypass = ctx
        .record_tool_result(&checkpoint_call.id, json!({ "accepted": true }))
        .await;
    assert!(matches!(bypass, Err(Error::InvalidState(_))));
    ctx.checkpoint(
        &checkpoint_call.id,
        CheckpointState {
            completed: vec![],
            remaining: vec!["work".into()],
            artifacts: vec![],
            failed_attempts: vec![],
            evidence: vec![],
        },
        |_| async {
            Ok(Verification {
                passed: true,
                evidence: json!({}),
            })
        },
    )
    .await
    .unwrap();

    let missing_checkpoint = ctx
        .checkpoint(
            "missing_checkpoint",
            CheckpointState {
                completed: vec![],
                remaining: vec!["work".into()],
                artifacts: vec![],
                failed_attempts: vec![],
                evidence: vec![],
            },
            |_| async {
                Ok(Verification {
                    passed: true,
                    evidence: json!({}),
                })
            },
        )
        .await;
    assert!(matches!(missing_checkpoint, Err(Error::InvalidState(_))));

    ctx.close().await.unwrap();
    pool.close().await;
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn fenced_context_cannot_obtain_pending_work() {
    let directory = std::env::temp_dir().join(format!("zincir-agent-sdk-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let options = SqliteConnectOptions::new()
        .filename(directory.join("zincir.db"))
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
    let mut first = AgentContext::create(
        pool.clone(),
        None,
        "custom-agent",
        "bring-your-own-provider",
        json!({ "goal": "finish the task" }),
        Duration::from_secs(30),
    )
    .await
    .unwrap();
    let run_id = first.run_id();
    first
        .record_response(
            json!("use tool"),
            &[ToolCall {
                id: "call_1".into(),
                name: "custom_tool".into(),
                args: json!({}),
            }],
            "tool_use",
        )
        .await
        .unwrap();
    let mut recovered = AgentContext::recover(pool.clone(), run_id, Duration::from_secs(30))
        .await
        .unwrap();

    let stale = first.pending_tool_calls().await;

    assert!(matches!(stale, Err(Error::InvalidState(_))));
    assert_eq!(recovered.pending_tool_calls().await.unwrap().len(), 1);
    recovered.close().await.unwrap();
    pool.close().await;
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn heartbeat_keeps_the_lease_alive_during_long_work() {
    let directory = std::env::temp_dir().join(format!("zincir-heartbeat-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let pool = SqlitePoolOptions::new()
        .max_connections(2)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(directory.join("zincir.db"))
                .create_if_missing(true)
                .foreign_keys(true)
                .journal_mode(SqliteJournalMode::Wal),
        )
        .await
        .unwrap();
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();
    let mut ctx = AgentContext::create(
        pool.clone(),
        None,
        "custom-agent",
        "bring-your-own-provider",
        json!({}),
        Duration::from_millis(120),
    )
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(350)).await;

    assert!(ctx.pending_tool_calls().await.is_ok());
    ctx.close().await.unwrap();
    pool.close().await;
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn heartbeat_failure_is_reported_before_more_writes() {
    let directory = std::env::temp_dir().join(format!("zincir-heartbeat-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let pool = SqlitePoolOptions::new()
        .max_connections(3)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(directory.join("zincir.db"))
                .create_if_missing(true)
                .foreign_keys(true)
                .journal_mode(SqliteJournalMode::Wal),
        )
        .await
        .unwrap();
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();
    let mut first = AgentContext::create(
        pool.clone(),
        None,
        "custom-agent",
        "bring-your-own-provider",
        json!({}),
        Duration::from_millis(300),
    )
    .await
    .unwrap();
    let recovered = AgentContext::recover(pool.clone(), first.run_id(), Duration::from_secs(30))
        .await
        .unwrap();
    wait_for_heartbeat_failure(&mut first).await;

    let error = first
        .record_response(json!("late response"), &[], "end_turn")
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        Error::InvalidState(message) if message.contains("heartbeat")
    ));
    recovered.close().await.unwrap();
    pool.close().await;
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn agent_context_rejects_impractical_lease_ttls() {
    let directory = std::env::temp_dir().join(format!("zincir-heartbeat-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let pool = SqlitePoolOptions::new()
        .max_connections(2)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(directory.join("zincir.db"))
                .create_if_missing(true)
                .foreign_keys(true)
                .journal_mode(SqliteJournalMode::Wal),
        )
        .await
        .unwrap();
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();

    let result = AgentContext::create(
        pool.clone(),
        None,
        "custom-agent",
        "bring-your-own-provider",
        json!({}),
        Duration::from_millis(1),
    )
    .await;
    let run_count: i64 = sqlx::query_scalar("SELECT count(*) FROM agent_runs")
        .fetch_one(&pool)
        .await
        .unwrap();

    assert!(matches!(result, Err(Error::InvalidState(_))));
    assert_eq!(run_count, 0);
    pool.close().await;
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn completion_releases_ownership_after_heartbeat_failure() {
    let directory = std::env::temp_dir().join(format!("zincir-heartbeat-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let pool = SqlitePoolOptions::new()
        .max_connections(2)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(directory.join("zincir.db"))
                .create_if_missing(true)
                .foreign_keys(true)
                .journal_mode(SqliteJournalMode::Wal),
        )
        .await
        .unwrap();
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();
    let mut ctx = AgentContext::create(
        pool.clone(),
        None,
        "custom-agent",
        "bring-your-own-provider",
        json!({}),
        Duration::from_millis(300),
    )
    .await
    .unwrap();
    let run_id = ctx.run_id();
    sqlx::query(
        "CREATE TRIGGER fail_lease_renewal
         BEFORE UPDATE OF lease_expires_at_ms ON agent_runs
         BEGIN SELECT RAISE(FAIL, 'forced heartbeat failure'); END",
    )
    .execute(&pool)
    .await
    .unwrap();
    wait_for_heartbeat_failure(&mut ctx).await;
    sqlx::query("DROP TRIGGER fail_lease_renewal")
        .execute(&pool)
        .await
        .unwrap();

    let result = ctx.complete().await;
    let owner: Option<Uuid> = sqlx::query_scalar("SELECT lease_owner FROM agent_runs WHERE id = ?")
        .bind(run_id)
        .fetch_one(&pool)
        .await
        .unwrap();

    assert!(matches!(
        result,
        Err(Error::InvalidState(message)) if message.contains("heartbeat")
    ));
    assert_eq!(owner, None);
    pool.close().await;
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn verifier_cannot_persist_when_post_verification_renewal_fails() {
    let directory = std::env::temp_dir().join(format!("zincir-heartbeat-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let pool = SqlitePoolOptions::new()
        .max_connections(3)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(directory.join("zincir.db"))
                .create_if_missing(true)
                .foreign_keys(true)
                .journal_mode(SqliteJournalMode::Wal),
        )
        .await
        .unwrap();
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();
    let mut ctx = AgentContext::create(
        pool.clone(),
        None,
        "custom-agent",
        "bring-your-own-provider",
        json!({}),
        Duration::from_millis(300),
    )
    .await
    .unwrap();
    let run_id = ctx.run_id();
    let call = ToolCall {
        id: "checkpoint_1".into(),
        name: "submit_checkpoint".into(),
        args: json!({}),
    };
    ctx.record_response(json!("checkpoint"), std::slice::from_ref(&call), "tool_use")
        .await
        .unwrap();
    let (started, verifier_started) = tokio::sync::oneshot::channel();
    let (release_verifier, released) = tokio::sync::oneshot::channel();
    let checkpoint = tokio::spawn(async move {
        let result = ctx
            .checkpoint(
                &call.id,
                CheckpointState {
                    completed: vec!["done".into()],
                    remaining: vec![],
                    artifacts: vec![],
                    failed_attempts: vec![],
                    evidence: vec![],
                },
                move |_| async move {
                    let _ = started.send(());
                    released.await.unwrap();
                    Ok(Verification {
                        passed: true,
                        evidence: json!({}),
                    })
                },
            )
            .await;
        (ctx, result)
    });
    verifier_started.await.unwrap();
    sqlx::query(
        "CREATE TRIGGER fail_lease_renewal
         BEFORE UPDATE OF lease_expires_at_ms ON agent_runs
         BEGIN SELECT RAISE(FAIL, 'forced heartbeat failure'); END",
    )
    .execute(&pool)
    .await
    .unwrap();
    release_verifier.send(()).unwrap();

    let (ctx, result) = checkpoint.await.unwrap();
    sqlx::query("DROP TRIGGER fail_lease_renewal")
        .execute(&pool)
        .await
        .unwrap();
    let candidates: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM checkpoints WHERE run_id = ? AND status = 'candidate'",
    )
    .bind(run_id)
    .fetch_one(&pool)
    .await
    .unwrap();

    assert!(result.is_err());
    assert_eq!(candidates, 1);
    let _ = ctx.close().await;
    pool.close().await;
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn custom_agent_recovers_an_unfinished_checkpoint() {
    let directory = std::env::temp_dir().join(format!("zincir-agent-sdk-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let options = SqliteConnectOptions::new()
        .filename(directory.join("zincir.db"))
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
        "custom-agent",
        "bring-your-own-provider",
        &json!({ "goal": "finish the task" }),
    )
    .await
    .unwrap();
    let mut first = AgentContext::open(pool.clone(), run.id, Duration::from_secs(30))
        .await
        .unwrap();
    let checkpoint_call = ToolCall {
        id: "checkpoint_1".into(),
        name: "submit_checkpoint".into(),
        args: json!({}),
    };
    first
        .record_response(
            json!("candidate"),
            std::slice::from_ref(&checkpoint_call),
            "tool_use",
        )
        .await
        .unwrap();
    let result = first
        .checkpoint(
            &checkpoint_call.id,
            CheckpointState {
                completed: vec!["task".into()],
                remaining: vec![],
                artifacts: vec![],
                failed_attempts: vec![],
                evidence: vec![],
            },
            |_| async { Err(Error::Tool("verifier interrupted".into())) },
        )
        .await;
    assert!(matches!(result, Err(Error::Tool(_))));
    let mut candidate = first.pending_checkpoint().await.unwrap().unwrap();
    candidate.state = json!({
        "completed": ["forged"],
        "remaining": [],
        "artifacts": [],
        "failed_attempts": [],
        "evidence": []
    });
    first.close().await.unwrap();

    let mut recovered = AgentContext::recover(pool.clone(), run.id, Duration::from_secs(30))
        .await
        .unwrap();
    let accepted = recovered
        .verify_checkpoint(candidate, |state| async move {
            assert_eq!(state.completed, vec!["task"]);
            Ok(Verification {
                passed: true,
                evidence: json!({ "recovered": true }),
            })
        })
        .await
        .unwrap();

    assert_eq!(accepted.status, "accepted");
    recovered.complete().await.unwrap();
    pool.close().await;
    std::fs::remove_dir_all(directory).unwrap();
}
