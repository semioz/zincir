use std::{net::SocketAddr, path::Path as FilePath, time::Duration};

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::get,
    Router,
};
use chrono::{DateTime, Utc};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    SqlitePool,
};
use uuid::Uuid;

use crate::{
    db,
    error::{Error, Result},
    types::{AgentRun, Event, StepRecord, TimerRecord},
};

pub async fn serve(database_path: &FilePath) -> Result<()> {
    let pool = open_read_only_pool(database_path).await?;
    validate_schema(&pool).await?;
    let address = parse_loopback_address(
        &std::env::var("ZINCIR_UI_ADDRESS").unwrap_or_else(|_| "127.0.0.1:8787".into()),
    )?;
    let listener = tokio::net::TcpListener::bind(address).await?;
    tracing::info!(%address, "Zincir Inspector listening");
    axum::serve(listener, router(pool)).await?;
    Ok(())
}

fn parse_loopback_address(value: &str) -> Result<SocketAddr> {
    let address = value
        .parse::<SocketAddr>()
        .map_err(|error| Error::InvalidState(format!("invalid ZINCIR_UI_ADDRESS: {error}")))?;
    if address.ip().is_loopback() {
        Ok(address)
    } else {
        Err(Error::InvalidState(
            "ZINCIR_UI_ADDRESS must use a loopback address".into(),
        ))
    }
}

async fn open_read_only_pool(database_path: &FilePath) -> Result<SqlitePool> {
    let options = SqliteConnectOptions::new()
        .filename(database_path)
        .read_only(true)
        .busy_timeout(Duration::from_secs(5));
    SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await
        .map_err(Into::into)
}

async fn validate_schema(pool: &SqlitePool) -> Result<()> {
    let table_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master
         WHERE type = 'table' AND name IN ('agent_runs', 'events', 'steps', 'timers')",
    )
    .fetch_one(pool)
    .await?;
    if table_count == 4 {
        Ok(())
    } else {
        Err(Error::InvalidState(
            "database schema is incomplete; run `cargo run` to apply migrations".into(),
        ))
    }
}

fn router(pool: SqlitePool) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/runs/{id}", get(run_detail))
        .with_state(pool)
}

async fn index(State(pool): State<SqlitePool>) -> std::result::Result<Html<String>, UiError> {
    let runs = db::list_runs(&pool).await?;
    Ok(Html(page("Runs", &render_run_list(&runs))))
}

async fn run_detail(
    State(pool): State<SqlitePool>,
    Path(run_id): Path<Uuid>,
) -> std::result::Result<Html<String>, UiError> {
    let run = db::get_run(&pool, run_id).await?;
    let events = db::get_events(&pool, run_id).await?;
    let steps = db::get_steps(&pool, run_id).await?;
    let timers = db::get_timers(&pool, run_id).await?;
    Ok(Html(page(
        &format!("Run {run_id}"),
        &render_run_detail(&run, &events, &steps, &timers),
    )))
}

struct UiError(Error);

impl From<Error> for UiError {
    fn from(error: Error) -> Self {
        Self(error)
    }
}

impl IntoResponse for UiError {
    fn into_response(self) -> Response {
        let status = if matches!(self.0, Error::NotFound(_)) {
            StatusCode::NOT_FOUND
        } else {
            StatusCode::INTERNAL_SERVER_ERROR
        };
        let body = format!(
            "<h1>{status}</h1><p>{}</p><p><a href=\"/\">Runs</a></p>",
            escape(&self.0.to_string())
        );
        (status, Html(page("Zincir Inspector", &body))).into_response()
    }
}

fn render_run_list(runs: &[AgentRun]) -> String {
    if runs.is_empty() {
        return "<p class=\"empty\">No durable runs yet.</p>".into();
    }

    let rows = runs
        .iter()
        .map(|run| {
            format!(
                "<tr><td><a href=\"/runs/{id}\"><code>{id}</code></a></td><td><span class=\"status {status}\">{status}</span></td><td>{role}</td><td>{provider}</td><td>{updated}</td></tr>",
                id = run.id,
                status = run.status.as_str(),
                role = escape(&run.role),
                provider = escape(&run.provider),
                updated = format_time(run.updated_at),
            )
        })
        .collect::<String>();
    format!(
        "<table><thead><tr><th>Run</th><th>Status</th><th>Role</th><th>Provider</th><th>Updated</th></tr></thead><tbody>{rows}</tbody></table>"
    )
}

fn render_run_detail(
    run: &AgentRun,
    events: &[Event],
    steps: &[StepRecord],
    timers: &[TimerRecord],
) -> String {
    format!(
        "<p><a href=\"/\">← All runs</a></p>
         <h1><code>{id}</code></h1>
         <dl><dt>Status</dt><dd><span class=\"status {status}\">{status}</span></dd><dt>Role</dt><dd>{role}</dd><dt>Provider</dt><dd>{provider}</dd><dt>Created</dt><dd>{created}</dd><dt>Updated</dt><dd>{updated}</dd></dl>
         <h2>Events</h2>{events}
         <h2>Steps</h2>{steps}
         <h2>Timers</h2>{timers}
         <h2>Configuration</h2><pre>{config}</pre>",
        id = run.id,
        status = run.status.as_str(),
        role = escape(&run.role),
        provider = escape(&run.provider),
        created = format_time(run.created_at),
        updated = format_time(run.updated_at),
        events = render_events(events),
        steps = render_steps(steps),
        timers = render_timers(timers),
        config = render_json(&run.config),
    )
}

fn render_events(events: &[Event]) -> String {
    if events.is_empty() {
        return "<p class=\"empty\">No events.</p>".into();
    }
    events
        .iter()
        .map(|event| {
            format!(
                "<details><summary>#{seq} · <strong>{event_type}</strong> · {created}</summary><pre>{payload}</pre></details>",
                seq = event.seq,
                event_type = event.event_type.as_str(),
                created = format_time(event.created_at),
                payload = render_json(&event.payload),
            )
        })
        .collect()
}

fn render_steps(steps: &[StepRecord]) -> String {
    if steps.is_empty() {
        return "<p class=\"empty\">No named steps.</p>".into();
    }
    let rows = steps
        .iter()
        .map(|step| {
            format!(
                "<tr><td><code>{name}</code></td><td>{status}</td><td>{started}</td><td>{completed}</td><td><pre>{result}</pre></td></tr>",
                name = escape(&step.name),
                status = escape(&step.status),
                started = format_time(step.started_at),
                completed = step.completed_at.map(format_time).unwrap_or_else(|| "—".into()),
                result = step.result.as_ref().map(render_json).unwrap_or_else(|| "—".into()),
            )
        })
        .collect::<String>();
    format!(
        "<table><thead><tr><th>Name</th><th>Status</th><th>Started</th><th>Completed</th><th>Result</th></tr></thead><tbody>{rows}</tbody></table>"
    )
}

fn render_timers(timers: &[TimerRecord]) -> String {
    if timers.is_empty() {
        return "<p class=\"empty\">No durable timers.</p>".into();
    }
    let rows = timers
        .iter()
        .map(|timer| {
            format!(
                "<tr><td><code>{name}</code></td><td>{wake_at}</td><td>{completed}</td></tr>",
                name = escape(&timer.name),
                wake_at = format_timestamp(timer.wake_at_ms),
                completed = timer
                    .completed_at
                    .map(format_time)
                    .unwrap_or_else(|| "—".into()),
            )
        })
        .collect::<String>();
    format!(
        "<table><thead><tr><th>Name</th><th>Wake at</th><th>Completed</th></tr></thead><tbody>{rows}</tbody></table>"
    )
}

fn page(title: &str, content: &str) -> String {
    format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"><title>{}</title><style>{}</style></head><body><main><header><a class=\"brand\" href=\"/\">zincir</a><span>Inspector</span></header>{content}</main></body></html>",
        escape(title),
        CSS
    )
}

fn render_json(value: &serde_json::Value) -> String {
    escape(&serde_json::to_string_pretty(value).unwrap_or_else(|_| "null".into()))
}

fn format_time(time: DateTime<Utc>) -> String {
    time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

fn format_timestamp(milliseconds: i64) -> String {
    DateTime::from_timestamp_millis(milliseconds)
        .map(format_time)
        .unwrap_or_else(|| milliseconds.to_string())
}

fn escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#39;"),
            character => escaped.push(character),
        }
    }
    escaped
}

const CSS: &str = r#"
:root { color-scheme: dark; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; background: #101411; color: #e7eee8; }
body { margin: 0; } main { max-width: 1160px; margin: 0 auto; padding: 32px 20px 64px; }
header { display: flex; gap: 10px; align-items: baseline; margin-bottom: 36px; color: #9bb0a0; } .brand { color: #b9ec75; font-size: 1.4rem; font-weight: 700; text-decoration: none; }
h1 { font-size: 1.25rem; overflow-wrap: anywhere; } h2 { margin-top: 36px; font-size: 1rem; color: #b9ec75; } a { color: #b9ec75; }
table { width: 100%; border-collapse: collapse; font-size: .84rem; } th { color: #9bb0a0; font-weight: 500; text-align: left; } th, td { border-bottom: 1px solid #27332a; padding: 12px 8px; vertical-align: top; } code, pre { font-family: inherit; } pre { margin: 10px 0 0; white-space: pre-wrap; overflow-wrap: anywhere; color: #c8d4ca; } details { border-bottom: 1px solid #27332a; padding: 11px 4px; } summary { cursor: pointer; } dl { display: grid; grid-template-columns: 100px 1fr; gap: 10px 18px; } dt { color: #9bb0a0; } dd { margin: 0; overflow-wrap: anywhere; }
.status { display: inline-block; border-radius: 99px; padding: 2px 8px; background: #29362c; } .status.running { background: #334628; color: #d5ff8c; } .status.failed { background: #512c2c; color: #ffb5b5; } .status.completed { background: #1f473d; color: #a7f5d7; } .empty { color: #9bb0a0; }
@media (max-width: 700px) { main { padding: 20px 12px; } table { display: block; overflow-x: auto; } }
"#;

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use serde_json::json;
    use uuid::Uuid;

    use super::render_run_list;
    use crate::types::{AgentRun, RunStatus};

    #[test]
    fn inspector_rejects_non_loopback_addresses() {
        assert!(super::parse_loopback_address("127.0.0.1:8787").is_ok());
        assert!(super::parse_loopback_address("0.0.0.0:8787").is_err());
    }

    #[tokio::test]
    async fn inspector_rejects_a_missing_database() {
        let path = std::env::temp_dir().join(format!("zincir-missing-{}.db", Uuid::new_v4()));

        assert!(super::open_read_only_pool(&path).await.is_err());
        assert!(!path.exists());
    }

    #[test]
    fn run_list_escapes_dynamic_values_and_links_to_the_run() {
        let run = AgentRun {
            id: Uuid::nil(),
            parent_run_id: None,
            role: "<script>alert(1)</script>".into(),
            status: RunStatus::Running,
            provider: "stub".into(),
            config: json!({}),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let html = render_run_list(&[run]);

        assert!(html.contains("href=\"/runs/00000000-0000-0000-0000-000000000000\""));
        assert!(html.contains("running"));
        assert!(html.contains("&lt;script&gt;alert(1)&lt;/script&gt;"));
        assert!(!html.contains("<script>"));
    }
}
