# zincir

*A durable execution runtime for tool-using AI workflows.*

Zincir is a Rust runtime that persists agent progress in Postgres so interrupted runs can reconstruct recorded state and continue. It is currently a single-node prototype, not a production workflow engine.

## Status

v0.1 foundation:

- The crate compiles and the single-agent stub loop runs against Postgres.
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
- `state_transition` is reserved but not emitted yet.

On resume, Zincir rebuilds messages from the run configuration and recorded events.

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
        └── Postgres event log
```

Postgres contains three tables:

- `agent_runs` — agent identity, parent, status, provider label, and configuration.
- `events` — ordered replay history per run.
- `messages` — reserved for durable inter-agent messaging.

See `migrations/0001_init.sql` for the complete schema.

## Run the demo

Requirements: Linux or macOS, Rust, and a running Postgres database.

```bash
createdb zincir
DATABASE_URL=postgres://localhost/zincir RUST_LOG=info cargo run
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
DATABASE_URL=postgres://localhost/zincir ZINCIR_RESUME=1 RUST_LOG=info cargo run
```

`ZINCIR_OUTPUT_DIR` overrides the demo output directory. `ZINCIR_PAUSE_BEFORE_TOOL_MS` and `ZINCIR_PAUSE_AFTER_TOOL_MS` expose both crash boundaries for testing.

## Crash recovery test

The test uses a dedicated database named `zincir_test` and resets its `public` schema. It refuses to run against any other database name.

```bash
createdb zincir_test
ZINCIR_TEST_DATABASE_URL=postgres://localhost/zincir_test \
  ./tests/crash_kill_resume.sh
```

The script builds Zincir and runs two scenarios. It kills the exact binary before the side effect, then repeats after the atomic side effect but before `tool_result` persistence. Each scenario resumes the exact run, checks event order and status, verifies one observable effect, and confirms a second resume is a no-op.

## Durability contract

Zincir currently guarantees only what it records:

- A unique event sequence within each run.
- One durable tool intent per `(run_id, idempotency_key)`.
- Replay of persisted LLM responses and tool results.
- At-least-once recovery of tool intents without results.

It does not currently guarantee:

- Exactly-once external side effects.
- That an in-progress provider call will not be repeated after a crash.
- Deterministic re-generation by an LLM.
- Distributed or concurrent ownership of the same run.

## Roadmap

1. Make event appends and status changes transactional.
2. Add run leases before supporting concurrent workers.
3. Add real provider implementations.
4. Add durable harness integrations such as OpenCode and Claude Code.
5. Add multi-agent supervision and messaging.
6. Add tracing, snapshots, and concurrency controls when measurements require them.

## Non-goals for v1

- Hosted service or visual workflow builder.
- General distributed cluster.
- Automatic VM or GPU provisioning.
- Guaranteeing exactly-once behavior for non-idempotent external systems.

## License

MIT.
