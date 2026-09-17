use std::time::Duration;

use serde_json::json;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use uuid::Uuid;
use zincir::{db, AgentContext, CheckpointState, Error, RunStatus, ToolCall, Verification};

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
