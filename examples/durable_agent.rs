#[cfg(not(unix))]
compile_error!("The durable agent example currently requires Unix");

use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use serde_json::{json, Value};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use tokio::{
    fs::{self, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt},
};
use uuid::Uuid;
use zincir::{db, AgentContext, CheckpointRecord, CheckpointState, Result, ToolCall, Verification};

const LEASE_TTL: Duration = Duration::from_secs(300);

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let database_path =
        PathBuf::from(std::env::var("ZINCIR_DATABASE_PATH").unwrap_or_else(|_| "zincir.db".into()));
    let options = SqliteConnectOptions::new()
        .filename(&database_path)
        .create_if_missing(true)
        .foreign_keys(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Full)
        .busy_timeout(Duration::from_secs(5));
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await?;
    sqlx::migrate!("./migrations").run(&pool).await?;

    let output_directory =
        PathBuf::from(std::env::var("ZINCIR_OUTPUT_DIR").unwrap_or_else(|_| "output".into()));
    let run_ids = if std::env::var("ZINCIR_RESUME").is_ok() {
        let runs = db::list_inflight_runs(&pool).await?;
        let ids = runs.iter().map(|run| run.id).collect::<Vec<_>>();
        for run in runs {
            run_agent(
                AgentContext::recover(pool.clone(), run.id, LEASE_TTL).await?,
                &output_directory,
            )
            .await?;
        }
        ids
    } else {
        let context = AgentContext::create(
            pool.clone(),
            None,
            "example-agent",
            "bring-your-own-provider",
            json!({ "goal": "Write hello to the file." }),
            LEASE_TTL,
        )
        .await?;
        let run_id = context.run_id();
        run_agent(context, &output_directory).await?;
        vec![run_id]
    };

    for run_id in run_ids {
        let run = db::get_run(&pool, run_id).await?;
        tracing::info!(run_id = %run.id, status = ?run.status, "run");
        for event in db::get_events(&pool, run_id).await? {
            tracing::info!(seq = event.seq, event_type = ?event.event_type, "  event");
        }
    }
    Ok(())
}

async fn run_agent(mut ctx: AgentContext, output_directory: &Path) -> Result<()> {
    if let Some(checkpoint) = ctx.state().await?.checkpoint {
        if checkpoint_is_complete(&checkpoint)? {
            return ctx.complete().await;
        }
    }

    if let Some(candidate) = ctx.pending_checkpoint().await? {
        let checkpoint = verify_candidate(&mut ctx, candidate, output_directory).await?;
        if checkpoint_is_complete(&checkpoint)? {
            return ctx.complete().await;
        }
    }

    for pending in ctx.pending_tool_calls().await? {
        match pending.call.name.as_str() {
            "write_file" => {
                let result = execute_write_file(&pending.call, output_directory).await?;
                ctx.record_tool_result(&pending.call.id, result).await?;
            }
            "submit_checkpoint" if pending.response_tool_count == 1 => {
                let state = serde_json::from_value(pending.call.args)?;
                let checkpoint =
                    create_checkpoint(&mut ctx, &pending.call.id, state, output_directory).await?;
                if checkpoint_is_complete(&checkpoint)? {
                    return ctx.complete().await;
                }
            }
            _ => {}
        }
    }

    let durable = ctx.state().await?;
    let wrote_file = durable.events.iter().any(|event| {
        event.event_type.as_str() == "tool_result" && event.payload["call_id"] == "call_1"
    });
    if !wrote_file {
        let call = ToolCall {
            id: "call_1".into(),
            name: "write_file".into(),
            args: json!({ "content": "hello" }),
        };
        ctx.record_response(
            json!("I'll write the file."),
            std::slice::from_ref(&call),
            "tool_use",
        )
        .await?;
        let result = execute_write_file(&call, output_directory).await?;
        ctx.record_tool_result(&call.id, result).await?;
    }

    let state = CheckpointState {
        completed: vec!["Wrote the requested file".into()],
        remaining: vec![],
        artifacts: vec![output_directory
            .join("call_1.json")
            .to_string_lossy()
            .into_owned()],
        failed_attempts: vec![],
        evidence: vec!["write_file returned ok".into()],
    };
    let call = ToolCall {
        id: "checkpoint_1".into(),
        name: "submit_checkpoint".into(),
        args: serde_json::to_value(&state)?,
    };
    ctx.record_response(
        json!("The requested work is complete."),
        std::slice::from_ref(&call),
        "tool_use",
    )
    .await?;
    let checkpoint = create_checkpoint(&mut ctx, &call.id, state, output_directory).await?;
    pause("ZINCIR_PAUSE_AFTER_CHECKPOINT_MS", &call.id).await;
    if checkpoint_is_complete(&checkpoint)? {
        ctx.complete().await?;
    } else {
        ctx.close().await?;
    }
    Ok(())
}

async fn create_checkpoint(
    ctx: &mut AgentContext,
    call_id: &str,
    state: CheckpointState,
    output_directory: &Path,
) -> Result<CheckpointRecord> {
    let output_directory = output_directory.to_owned();
    ctx.checkpoint(call_id, state, move |state| async move {
        verify(&state, &output_directory).await
    })
    .await
}

async fn verify_candidate(
    ctx: &mut AgentContext,
    candidate: CheckpointRecord,
    output_directory: &Path,
) -> Result<CheckpointRecord> {
    let output_directory = output_directory.to_owned();
    ctx.verify_checkpoint(candidate, move |state| async move {
        verify(&state, &output_directory).await
    })
    .await
}

async fn verify(state: &CheckpointState, output_directory: &Path) -> Result<Verification> {
    let artifact = output_directory.join("call_1.json");
    let exists = fs::try_exists(&artifact).await?;
    Ok(Verification {
        passed: exists && state.remaining.is_empty(),
        evidence: json!({ "artifact": artifact, "exists": exists }),
    })
}

fn checkpoint_is_complete(checkpoint: &CheckpointRecord) -> Result<bool> {
    let state: CheckpointState = serde_json::from_value(checkpoint.state.clone())?;
    Ok(checkpoint.status == "accepted" && state.remaining.is_empty())
}

async fn execute_write_file(call: &ToolCall, directory: &Path) -> Result<Value> {
    fs::create_dir_all(directory).await?;
    let result_path = directory.join(format!("{}.json", call.id));
    if let Some(result) = read_json(&result_path).await? {
        return Ok(result);
    }

    pause("ZINCIR_PAUSE_BEFORE_TOOL_MS", &call.id).await;
    let result = json!({
        "ok": true,
        "content": call.args.get("content").cloned().unwrap_or_default()
    });
    let temporary_path = directory.join(format!(".{}.{}.tmp", call.id, Uuid::new_v4()));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary_path)
        .await?;
    file.write_all(&serde_json::to_vec(&result)?).await?;
    file.sync_all().await?;
    let published = match fs::hard_link(&temporary_path, &result_path).await {
        Ok(()) => result,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            read_json(&result_path).await?.ok_or_else(|| {
                zincir::Error::Tool(format!("result disappeared: {}", result_path.display()))
            })?
        }
        Err(error) => return Err(error.into()),
    };
    let _ = fs::remove_file(&temporary_path).await;
    pause("ZINCIR_PAUSE_AFTER_TOOL_MS", &call.id).await;
    Ok(published)
}

async fn read_json(path: &Path) -> Result<Option<Value>> {
    let mut file = match OpenOptions::new().read(true).open(path).await {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).await?;
    Ok(Some(serde_json::from_slice(&bytes)?))
}

async fn pause(variable: &str, call_id: &str) {
    let Some(milliseconds) = std::env::var(variable)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
    else {
        return;
    };
    if milliseconds > 0 {
        tracing::info!(%call_id, milliseconds, variable, "pausing tool execution");
        tokio::time::sleep(Duration::from_millis(milliseconds)).await;
    }
}
