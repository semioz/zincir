#!/usr/bin/env bash
# Verifies recovery when Zincir crashes after persisting tool intent but before
# applying the side effect. This test is destructive and only runs against a
# database named zincir_test.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

DB_URL="${ZINCIR_TEST_DATABASE_URL:-postgres://localhost:5432/zincir_test}"
DB_NAME="${DB_URL%%\?*}"
DB_NAME="${DB_NAME##*/}"
if [[ "$DB_NAME" != "zincir_test" ]]; then
    echo "Refusing to reset database '$DB_NAME'; expected zincir_test" >&2
    exit 2
fi

export DATABASE_URL="$DB_URL"
export RUST_LOG="info"
export ZINCIR_PAUSE_TOOL_MS=60000

BIN="$ROOT_DIR/target/debug/zincir"
OUT_FILE="$(mktemp -t zincir-out.XXXXXX)"
START_LOG="$(mktemp -t zincir-start.XXXXXX)"
RESUME_LOG="$(mktemp -t zincir-resume.XXXXXX)"
export ZINCIR_OUTPUT_FILE="$OUT_FILE"
CHILD_PID=""

cleanup() {
    if [[ -n "$CHILD_PID" ]] && kill -0 "$CHILD_PID" 2>/dev/null; then
        kill -9 "$CHILD_PID" 2>/dev/null || true
        wait "$CHILD_PID" 2>/dev/null || true
    fi
    rm -f "$OUT_FILE" "$START_LOG" "$RESUME_LOG"
}
trap cleanup EXIT

fail() {
    echo "FAIL: $1" >&2
    echo "--- initial run ---" >&2
    cat "$START_LOG" >&2 || true
    echo "--- resume run ---" >&2
    cat "$RESUME_LOG" >&2 || true
    exit 1
}

sql() {
    psql "$DB_URL" -XAtq -v ON_ERROR_STOP=1 -c "$1"
}

echo "== checking dedicated test database =="
sql "SELECT 1" >/dev/null || fail "cannot connect to $DB_URL"

# Reset migration and runtime state without dropping the database itself.
sql "DROP SCHEMA public CASCADE; CREATE SCHEMA public;" >/dev/null

echo "== building zincir =="
cargo build --quiet

echo "== starting run =="
"$BIN" >"$START_LOG" 2>&1 &
CHILD_PID=$!

# Poll persisted state instead of guessing how long compilation/startup takes.
RUN_ID=""
for _ in {1..200}; do
    RUN_ID="$(sql "
        SELECT e.run_id
        FROM events e
        JOIN agent_runs r ON r.id = e.run_id
        WHERE e.event_type = 'tool_call' AND r.status = 'running'
        ORDER BY e.created_at DESC
        LIMIT 1;
    " 2>/dev/null || true)"

    if [[ -n "$RUN_ID" ]]; then
        break
    fi
    if ! kill -0 "$CHILD_PID" 2>/dev/null; then
        wait "$CHILD_PID" 2>/dev/null || true
        CHILD_PID=""
        fail "zincir exited before persisting tool intent"
    fi
    sleep 0.1
done

[[ -n "$RUN_ID" ]] || fail "timed out waiting for tool intent"
[[ ! -s "$OUT_FILE" ]] || fail "tool side effect happened before the crash point"

echo "== killing zincir after tool intent (run $RUN_ID) =="
kill -9 "$CHILD_PID"
wait "$CHILD_PID" 2>/dev/null || true
CHILD_PID=""

STATUS_BEFORE="$(sql "SELECT status FROM agent_runs WHERE id = '$RUN_ID';")"
[[ "$STATUS_BEFORE" == "running" ]] || fail "expected running before resume, got $STATUS_BEFORE"

echo "== resuming =="
unset ZINCIR_PAUSE_TOOL_MS
if ! ZINCIR_RESUME=1 "$BIN" >"$RESUME_LOG" 2>&1; then
    fail "resume process failed"
fi

echo "== verifying exact run =="
STATUS="$(sql "SELECT status FROM agent_runs WHERE id = '$RUN_ID';")"
TOOL_CALLS="$(sql "SELECT count(*) FROM events WHERE run_id = '$RUN_ID' AND event_type = 'tool_call';")"
TOOL_RESULTS="$(sql "SELECT count(*) FROM events WHERE run_id = '$RUN_ID' AND event_type = 'tool_result';")"
EVENT_TYPES="$(sql "SELECT string_agg(event_type, ',' ORDER BY seq) FROM events WHERE run_id = '$RUN_ID';")"
LINE_COUNT="$(wc -l <"$OUT_FILE" | tr -d ' ')"

[[ "$STATUS" == "completed" ]] || fail "expected completed, got $STATUS"
[[ "$TOOL_CALLS" == "1" ]] || fail "expected 1 tool_call, got $TOOL_CALLS"
[[ "$TOOL_RESULTS" == "1" ]] || fail "expected 1 tool_result, got $TOOL_RESULTS"
[[ "$EVENT_TYPES" == "llm_call,tool_call,tool_result,llm_call" ]] ||
    fail "unexpected event order: $EVENT_TYPES"
[[ "$LINE_COUNT" == "1" ]] || fail "expected one pre-effect recovery write, got $LINE_COUNT"

echo "PASS: persisted intent survived kill -9 and run $RUN_ID resumed to completion"
