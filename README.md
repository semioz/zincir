# zincir

*A durable execution runtime for multi-agent, tool-using AI workflows. Provider-agnostic, tool-agnostic.*

zincir is a Rust library + runtime that makes multi-agent, tool-calling workflows survive crashes, retries, and long-running execution without losing state or re-running completed work. It sits below your agent logic, not inside a specific agent framework: any LLM provider, any tool set, any orchestration pattern can run on top of it.

## Status

Early / actively built in public. v0.1 — single durable agent loop, compiles, not yet proven against a real provider or a crash-fuzz suite.

## Capabilities

### What works today (v0.1)

- **Durable event log.** Every LLM call and tool call is checkpointed to Postgres before execution moves on. The log is append-only, per-run, sequence-ordered.
- **Crash-recovery resume.** `Runtime::resume()` finds all runs in `pending`/`running` state and replays each from its event log — no lost state, no re-deriving from scratch.
- **Exactly-once tool issuance.** Tool calls are recorded as intent (`tool_call` event with an `idempotency_key`) *before* execution. On resume, an intent with no matching result is detected and re-driven. The DB enforces one intent per key per run.
- **Provider-agnostic core.** `LLMProvider` trait with a `StubProvider` that runs the loop end-to-end with no API key. Real provider impls (Claude, OpenAI-compatible, Ollama) are on the roadmap.
- **Tool-agnostic core.** `ToolExecutor` trait with a `NoopToolExecutor`. The runtime never assumes a tool format — tools are JSON-schema-shaped, MCP or hand-written both fit.
- **Replay from config.** Per-run `config` (model, temperature, system prompt, input, enabled tools) is stored in `agent_runs.config` JSONB, so a recorded run is reconstructable from the row alone.
- **Schema migrations.** `migrations/0001_init.sql` — `agent_runs`, `events`, `messages`. Run via `sqlx::migrate`.

### What's wired but not exercised

- **Multi-agent messaging.** The `messages` table and `list_pending_messages` / `mark_message_delivered` queries exist; the supervisor/worker fan-out loop that uses them is v0.2.
- **`Runtime::resume()` across many runs.** Works for one; not yet tested under crash-fuzz with N concurrent runs.

### Non-goals (v1)

- Not a distributed cluster — single-node plus Postgres, no separate orchestration service.
- Not a visual workflow builder or DSL — plain code, like Restate/DBOS's approach, not YAML.
- Not a hosted product — a library and a lightweight runtime, self-hosted.
- Not a Python-framework wrapper — agents are written in Rust on zincir's primitives. A Python SDK is a future possibility, not a v1 claim.

## Architecture

### Data model (Postgres)

```sql
CREATE TABLE agent_runs (
    id              UUID PRIMARY KEY,
    parent_run_id   UUID NULL REFERENCES agent_runs(id),
    role            TEXT NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('pending','running','completed','failed')),
    provider        TEXT NOT NULL,
    config          JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE events (
    id              BIGSERIAL PRIMARY KEY,
    run_id          UUID NOT NULL REFERENCES agent_runs(id) ON DELETE CASCADE,
    seq             INT NOT NULL,
    event_type      TEXT NOT NULL CHECK (event_type IN ('llm_call','tool_call','tool_result','state_transition')),
    payload         JSONB NOT NULL,
    idempotency_key TEXT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(run_id, seq)
);
CREATE UNIQUE INDEX idx_events_idem ON events(run_id, idempotency_key) WHERE idempotency_key IS NOT NULL;

CREATE TABLE messages (
    id              BIGSERIAL PRIMARY KEY,
    from_run_id     UUID NOT NULL REFERENCES agent_runs(id) ON DELETE CASCADE,
    to_run_id       UUID NOT NULL REFERENCES agent_runs(id) ON DELETE CASCADE,
    payload         JSONB NOT NULL,
    delivered       BOOLEAN NOT NULL DEFAULT false,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

- `agent_runs` — one row per agent instance (an "actor"). `parent_run_id` makes it a tree.
- `events` — append-only replay log per agent. `tool_call` and `tool_result` are separate rows; the gap between them is the crash window.
- `messages` — durable inter-agent messaging. No direct agent-to-agent calls; the table is the source of truth, `LISTEN/NOTIFY` is only a wakeup hint.

### Provider abstraction

```rust
#[async_trait]
pub trait LLMProvider: Send + Sync {
    async fn complete(&self, messages: Vec<LlmMessage>, tools: Vec<ToolSchema>) -> Result<Response>;
}
```

Swapping providers is a config change, not a code change. The runtime core never references a specific vendor's API shape.

### Tool execution

```rust
#[async_trait]
pub trait ToolExecutor: Send + Sync {
    async fn execute(&self, call: ToolCall) -> Result<ToolResult>;
}
```

v0.1 ships `NoopToolExecutor`. The roadmap default is in-process execution, with an opt-in container sandbox for code-exec / untrusted tools — not container-per-call, which is too heavy for the common case.

### Multi-agent coordination

Supervisor/worker, not peer-to-peer:

- A supervisor spawns children as new `agent_runs` rows, each with its own independent event log.
- Agents never call each other directly — coordination goes through the `messages` table.
- One child failing sets only its own status to `failed`; siblings continue. The supervisor decides: retry, proceed with partial results, or abort.
- Fan-out is concurrency-limited so N children don't unboundedly hit the provider or tool sandbox at once.

## What "durable" has to prove

- Kill the process mid-run (`kill -9`), restart, resume from the last checkpointed step — no duplicate tool side effects, no lost state.
- A crash-fuzz test suite (random kill points across many runs) passes, not just one manual kill.
- A recorded run replays and reproduces the exact recorded trace — for crash-recovery, audit, and debugging. This is a trace-replay guarantee, not a claim that the LLM re-derives the same intelligence.

## Tool idempotency contract

zincir guarantees **at-most-once issuance within a run** and **dedup by `idempotency_key` on resume**. It does *not* guarantee exactly-once execution for non-idempotent tools. Tools that do external side effects (emails, payments, DB writes) must be idempotent or accept an idempotency key the tool server respects. The runtime records intent before issuance and detects orphaned intents on resume; what happens inside the tool is the tool's contract.

## Tech stack

Rust, `tokio`, `sqlx` (raw SQL over Postgres, no ORM), `serde`, `tracing` / `tracing-subscriber`, OpenTelemetry for tracing (planned). No Docker dependency for v0.1 — the container sandbox is a later opt-in.

## Run

```bash
createdb zincir
DATABASE_URL=postgres://localhost/zincir cargo run
```

The demo creates a supervisor run with the stub provider, runs one loop iteration, and prints the event log. No API key required.

## Roadmap

- **v0.1** — single durable agent loop: checkpoint, crash-kill-resume proof *(current — compiles, not yet proven)*
- **v0.2** — multi-agent fan-out/fan-in, durable messaging, supervisor loop
- **v0.3** — provider abstraction proven against 2+ real providers
- **v0.4** — tracing/observability, crash-fuzz test suite, snapshot table to bound replay cost
- **v1.0** — stable primitives, crash-fuzz green, used in one real system

## License

MIT (placeholder — confirm before publishing).
