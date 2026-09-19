# zincir

Durable, resumable execution for long-horizon AI agents.

Zincir is an embedded Rust SDK for developers building their own long-horizon agents. Bring your own model client, tools, prompts, and agent loop; Zincir persists execution in local SQLite so interrupted runs can recover without blindly repeating completed work. It keeps the event log as ground truth while exposing verified progress for fresh contexts. It is a single-machine durability layer, not an agent framework or distributed workflow engine.

## Who it is for

Use Zincir when you are implementing an agent that can call tools, make costly model requests, or run long enough that a process restart is a normal failure mode:

- coding agents that modify repositories and run tests;
- research or document-processing agents that call external APIs;
- batch inference, evaluations, and data-preparation jobs;
- internal agent platforms that need an inspectable local source of truth.

Zincir is not yet the central, multi-machine workflow service for a company. It provides the durable local execution layer that such a platform can build on.

## Direction: durable, verified rounds

Process durability is necessary but insufficient for long-horizon work. An agent can remain alive while its growing conversation becomes noisy, contradictory, or confused. Zincir is evolving toward execution in bounded rounds:

```text
original goal
     │
     ▼
agent round ── tool calls ──► checkpoint candidate
                                      │
                              independent verification
                                ┌─────┴─────┐
                             accepted    rejected
                                │           │
                                ▼           ▼
                      durable progress   retry/replan
                                │
                                ▼
                       fresh agent context
```

The append-only event log remains authoritative. An accepted checkpoint is an agent-facing semantic view containing completed work, remaining work, artifacts, failed attempts, and evidence. Fresh rounds should start from the original goal and the latest accepted checkpoint rather than replaying an indefinitely growing conversation.

A checkpoint must not become trusted merely because the executor produced it. Acceptance should record the verifier decision, supporting evidence, artifact identity, and lineage to the event sequence it summarizes.

## Status

Current foundation:

- `AgentContext` lets custom agent loops record responses, recover tools, verify checkpoints, and reconstruct durable state.
- The crate also includes `examples/durable_agent.rs`, a small custom loop with an idempotent demo tool.
- Model responses and tool intent/results are recorded in a per-run event log.
- Inflight runs expose their durable state for application-controlled recovery.
- Expiring run leases, automatic heartbeats, and fencing tokens prevent concurrent contexts from persisting work for the same run.
- Applications submit semantic checkpoint candidates and verify them with async Rust closures.
- Accepted checkpoints let applications construct fresh contexts from verified progress.
- Deterministic tests verify process-kill recovery before and after the demo tool's atomic side effect and after checkpoint acceptance.
- Multi-agent coordination, an automatic resume worker, and sandboxing are not implemented; model and tool integrations belong to the application.

## Current capabilities

### Durable run state

Each agent has an `agent_runs` row containing its status, provider label, and application-defined JSON configuration.

### Event replay

Zincir treats `events` as an append-only, sequence-ordered log:

- `llm_call` records a completed provider response.
- `tool_call` records tool intent before execution.
- `tool_result` records the returned result after execution.
- `state_transition` records status changes such as `pending → running` and `running → completed`.
- `checkpoint_proposed`, `checkpoint_accepted`, and `checkpoint_rejected` record checkpoint lineage and verifier decisions.

On resume, `AgentContext::state()` returns the latest accepted checkpoint plus subsequent events so the application can construct its next model context.

### Transactional event ordering

Before assigning an event sequence, Zincir starts a SQLite `BEGIN IMMEDIATE` transaction. SQLite allows one writer at a time, so sequence allocation and event insertion are atomic. Status changes update `agent_runs` and append their `state_transition` event in the same transaction. WAL mode allows readers to continue while a writer is active.

### Pending tool recovery

A `tool_call` without a matching `tool_result` is considered pending and is executed again during recovery. This is **at-least-once tool execution**, not exactly-once execution.

Effect-once behavior requires cooperation from the tool: it must be idempotent, transactional with its destination, or enforce the supplied tool-call ID as an idempotency key. Zincir cannot make arbitrary external side effects exactly-once by itself.

### Embedded agent SDK

`AgentContext` owns the durable run lease while your application owns the loop. It renews that lease automatically in the background and surfaces heartbeat failure before accepting more writes. Lease TTLs below 30 ms are rejected; production values should leave ample room for scheduler and database contention. It can create or recover a run, atomically record a model response with all tool intents, list pending tools, persist tool results, submit or resume checkpoint verification, expose state after the latest accepted checkpoint, and complete or release the run.

```rust
let mut ctx = AgentContext::create(
    pool,
    None,
    "research-agent",
    "my-provider",
    serde_json::json!({ "goal": "Compare the proposals" }),
    Duration::from_secs(30),
).await?;

let durable = ctx.state().await?;
let response = my_agent.run(durable).await?;
ctx.record_response(response.content, &response.tool_calls, "tool_use").await?;
```

Checkpoint verification is supplied by application code as an async closure. If it is interrupted after the candidate is persisted, `AgentContext::recover()` and `pending_checkpoint()` expose it for another verification attempt.

### Reference example

`examples/durable_agent.rs` is a complete custom loop built directly on `AgentContext`. It records a synthetic model response, executes an idempotent file tool, submits a semantic checkpoint, verifies the artifact with an async Rust closure, and demonstrates recovery around the tool effect and checkpoint acceptance. It is example code, not a model or tool abstraction required by the SDK.

### Verified semantic checkpoints

A custom loop submits completed work, remaining work, artifacts, failed attempts, and evidence through `AgentContext::checkpoint()`. Zincir persists the candidate before invoking the application-provided verifier closure and stores its accepted or rejected decision with evidence.

A verifier returns `Verification { passed, evidence }`. The application decides whether accepted remaining work starts a fresh model context and whether an accepted checkpoint with no remaining work completes the run. A candidate left unfinished by a crash is exposed through `pending_checkpoint()` for another verification attempt.

### Durable named steps

`WorkflowContext::step()` stores the completed JSON result for a named step. Repeating the same step name after a restart returns the saved value without rerunning its closure:

```rust
let report: String = ctx.step("generate-report", || async {
    generate_report().await
}).await?;
```

A second context is refused while a step is `running`; it cannot silently execute the same closure. After the caller has confirmed the previous workflow process is dead, call `ctx.recover().await?` to release unfinished steps before retrying them.

Step closures with external side effects must still be idempotent: Zincir can crash after the effect completes but before its result is persisted.

### Durable sleep

`WorkflowContext::sleep()` saves an absolute wake time before waiting:

```rust
ctx.sleep("rate-limit-backoff", Duration::from_secs(60)).await?;
```

A restarted context uses that saved timestamp and waits only the remaining time. This waits in the calling process; scanning and resuming due timers in a background worker is not implemented yet.

### Multi-agent schema

The schema includes parent/child run relationships and a durable `messages` table. The supervisor loop, message delivery, and fan-out/fan-in behavior are not implemented yet.

## Architecture

```text
Your model + tools + agent loop
              │
              ▼
         AgentContext
              │
              └── SQLite runs + events + checkpoints
```

SQLite contains six tables:

- `agent_runs` — agent identity, parent, status, provider label, configuration, and lease state.
- `events` — ordered replay and checkpoint history per run.
- `checkpoints` — semantic state, verification result, status, and event lineage per round.
- `steps` — named workflow step claims and completed JSON results.
- `timers` — named absolute wake times and completion timestamps.
- `messages` — reserved for durable inter-agent messaging.

See `migrations/` for the complete schema.

## Run the demo

Requirements: Linux or macOS and Rust. No database server is required.

```bash
RUST_LOG=info cargo run --example durable_agent
```

This creates `zincir.db` in the current directory. Use `ZINCIR_DATABASE_PATH` to place it elsewhere:

```bash
ZINCIR_DATABASE_PATH=~/.local/share/zincir/zincir.db RUST_LOG=info \
  cargo run --example durable_agent
```

The demo:

1. Creates one run with application-defined configuration.
2. The example loop records a synthetic response requesting `write_file`.
3. Records the tool intent atomically with that response.
4. Atomically publishes `output/call_1.json`.
5. Records the tool result.
6. The loop records a synthetic checkpoint request.
7. Runs its verifier closure and accepts the checkpoint.
8. Completes the run because the verified checkpoint has no remaining work.
9. Prints that run and its event log.

To resume existing `pending` or `running` runs instead of creating a new one:

```bash
ZINCIR_RESUME=1 RUST_LOG=info cargo run --example durable_agent
```

Explicit resume fences the previous lease owner, so use it only after confirming that the previous process is dead. `ZINCIR_OUTPUT_DIR` overrides the demo output directory. `ZINCIR_PAUSE_BEFORE_TOOL_MS`, `ZINCIR_PAUSE_AFTER_TOOL_MS`, and `ZINCIR_PAUSE_AFTER_CHECKPOINT_MS` expose the tested crash boundaries.

## Inspector UI

Start the local, read-only run inspector:

```bash
cargo run -- ui
```

Open <http://127.0.0.1:8787>. It lists the latest 100 runs and shows each run's events, named steps, durable timers, and stored configuration. Set `ZINCIR_UI_ADDRESS` to use another loopback address or port; public network addresses are rejected.

The Inspector opens SQLite read-only: it neither creates a database nor applies migrations. Run the demo or your application first to create and migrate the database.

The Inspector deliberately has no Resume, Recover, or Cancel controls yet. Those actions need an explicit recovery policy and authorization before they are safe to expose from a browser.

## Crash recovery test

```bash
./tests/crash_kill_resume.sh
```

The acceptance script requires the `sqlite3` CLI (preinstalled on macOS; install your distribution's SQLite package on Linux). It creates temporary SQLite databases and kills the exact binary before the tool side effect, after the atomic side effect but before `tool_result` persistence, and after checkpoint acceptance but before run completion. Each scenario resumes the exact run, checks event order and status, verifies one observable effect, and confirms a second resume is a no-op.

SQLite concurrency tests run with the normal Rust suite:

```bash
cargo test
```

## Durability contract

Zincir currently guarantees only what it records:

- A unique, transactionally allocated event sequence within each run.
- One durable tool intent per `(run_id, idempotency_key)`.
- One persisted result per `(run_id, step name)`.
- Replay of persisted LLM responses and tool results.
- At-least-once recovery of tool intents without results.
- Transactional checkpoint proposal/decision events and one verifier result per checkpoint.
- Fresh-context continuation from the latest accepted checkpoint.

It does not currently guarantee:

- Exactly-once external side effects.
- That an in-progress provider call will not be repeated after a crash.
- Deterministic re-generation by an LLM.
- Distributed ownership of the same run across machines.

## Roadmap

1. Stabilize `AgentContext` and add provider-agnostic custom-agent examples.
2. Bind coding checkpoints to a generated workspace revision instead of agent-supplied artifact labels.
3. Add an automatic resume worker.
4. Add budgets for tokens, tool calls, and wall time, plus stuck/spin detection.
5. Add durable signals and human approval waits.
6. Add child runs and durable spawn/join after the single-agent path is proven.
7. Add Postgres only when multi-machine execution is required.

The target demonstration is a small custom agent that survives an injected process kill, restores verified progress, continues in a fresh context, and completes without duplicating external effects. The accompanying benchmark will vary three dimensions independently: crash durability, context strategy, and verification policy.

## Non-goals for v1

- Being the planner, model client, tool framework, or intelligence of the long-horizon agent.
- Wrapping end-user agent CLIs as the primary product.
- Hosted service or visual workflow builder.
- General distributed cluster in the SQLite backend.
- Generic multi-agent orchestration before the single-agent recovery path is proven.
- Automatic VM or GPU provisioning.
- Guaranteeing exactly-once behavior for non-idempotent external systems.
- Replacing a company's existing identity, secrets, observability, or deployment systems.

## License

MIT.
