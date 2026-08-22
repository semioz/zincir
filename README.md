# zincir

*A durable execution runtime for tool-using AI workflows.*

Zincir is a Rust runtime that persists agent progress in a local SQLite database so interrupted runs can reconstruct recorded state and continue. It is a single-machine runtime, not a distributed workflow engine.

## Status

v0.1 foundation:

- The crate compiles and the single-agent stub loop runs against a local SQLite file.
- LLM responses and tool intent/results are recorded in a per-run event log.
- Inflight runs can reconstruct their conversation and resume.
- Deterministic tests verify process-kill recovery both before and after the demo tool's atomic side effect.
- Multi-agent coordination, real providers, harness integrations, and sandboxing are not implemented.

## Current capabilities

### Durable run state

Each agent has an `agent_runs` row containing its status, provider label, and JSON configuration. The configuration includes the initial system prompt and input needed to reconstruct the conversation.

### Event replay

The runtime treats `events` as an append-only, sequence-ordered log:

- `llm_call` records a completed provider response.
- `tool_call` records tool intent before execution.
- `tool_result` records the returned result after execution.
- `state_transition` records status changes such as `pending → running` and `running → completed`.

On resume, Zincir rebuilds messages from the run configuration and recorded events.

### Transactional event ordering

Before assigning an event sequence, Zincir starts a SQLite `BEGIN IMMEDIATE` transaction. SQLite allows one writer at a time, so sequence allocation and event insertion are atomic. Status changes update `agent_runs` and append their `state_transition` event in the same transaction. WAL mode allows readers to continue while a writer is active.

### Pending tool recovery

A `tool_call` without a matching `tool_result` is considered pending and is executed again during recovery. This is **at-least-once tool execution**, not exactly-once execution.

Effect-once behavior requires cooperation from the tool: it must be idempotent, transactional with its destination, or enforce the supplied tool-call ID as an idempotency key. Zincir cannot make arbitrary external side effects exactly-once by itself.

### Provider and tool boundaries

The runtime depends on two traits:

```rust
#[async_trait]
pub trait LLMProvider: Send + Sync {
    async fn complete(
        &self,
        messages: Vec<LlmMessage>,
        tools: Vec<ToolSchema>,
    ) -> Result<Response>;
}

#[async_trait]
pub trait ToolExecutor: Send + Sync {
    async fn execute(&self, call: ToolCall) -> Result<ToolResult>;
}
```

The demo binary currently wires `StubProvider` and `IdempotentFileExecutor` directly. The executor stores one atomic result file per tool-call ID and reuses it on retry. Provider selection from stored configuration and real provider implementations are future work.

### Multi-agent schema

The schema includes parent/child run relationships and a durable `messages` table. The supervisor loop, message delivery, and fan-out/fan-in behavior are not implemented yet.

## Architecture

```text
RunConfig + agent_runs
        │
        ▼
     Runtime
        │
        ├── LLMProvider
        ├── ToolExecutor
        └── SQLite event log
```

SQLite contains three tables:

- `agent_runs` — agent identity, parent, status, provider label, and configuration.
- `events` — ordered replay history per run.
- `messages` — reserved for durable inter-agent messaging.

See `migrations/0001_init.sql` for the complete schema.

## Run the demo

Requirements: Linux or macOS and Rust. No database server is required.

```bash
RUST_LOG=info cargo run
```

This creates `zincir.db` in the current directory. Use `ZINCIR_DATABASE_PATH` to place it elsewhere:

```bash
ZINCIR_DATABASE_PATH=~/.local/share/zincir/zincir.db RUST_LOG=info cargo run
```

The demo:

1. Creates one run with valid durable configuration.
2. Calls the stub provider, which requests a `write_file` tool.
3. Records the tool intent.
4. Atomically publishes `output/call_1.json`.
5. Records the tool result.
6. Calls the stub provider again and completes the run.
7. Prints that run and its event log.

To resume existing `pending` or `running` runs instead of creating a new one:

```bash
ZINCIR_RESUME=1 RUST_LOG=info cargo run
```

`ZINCIR_OUTPUT_DIR` overrides the demo output directory. `ZINCIR_PAUSE_BEFORE_TOOL_MS` and `ZINCIR_PAUSE_AFTER_TOOL_MS` expose both crash boundaries for testing.

## Crash recovery test

```bash
./tests/crash_kill_resume.sh
```

The acceptance script requires the `sqlite3` CLI (preinstalled on macOS; install your distribution's SQLite package on Linux). It creates temporary SQLite databases, kills the exact binary before the side effect, then repeats after the atomic side effect but before `tool_result` persistence. Each scenario resumes the exact run, checks event order and status, verifies one observable effect, and confirms a second resume is a no-op.

SQLite concurrency tests run with the normal Rust suite:

```bash
cargo test
```

## Durability contract

Zincir currently guarantees only what it records:

- A unique, transactionally allocated event sequence within each run.
- One durable tool intent per `(run_id, idempotency_key)`.
- Replay of persisted LLM responses and tool results.
- At-least-once recovery of tool intents without results.

It does not currently guarantee:

- Exactly-once external side effects.
- That an in-progress provider call will not be repeated after a crash.
- Deterministic re-generation by an LLM.
- Distributed ownership of the same run across machines.
- Concurrent runtime execution of the same run; SQLite serializes database writes but does not lease the full agent loop.

## Roadmap

1. Add run leases before supporting concurrent runtime execution.
2. Add real provider implementations.
3. Add durable harness integrations such as OpenCode and Claude Code.
4. Add multi-agent supervision and messaging.
5. Add tracing, snapshots, and a Postgres backend when multi-machine execution is needed.

## Non-goals for v1

- Hosted service or visual workflow builder.
- General distributed cluster in the SQLite backend.
- Automatic VM or GPU provisioning.
- Guaranteeing exactly-once behavior for non-idempotent external systems.

## License

MIT.
