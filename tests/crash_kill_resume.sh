#!/usr/bin/env bash
# v0.1 acceptance test: crash-kill-resume with no duplicate side effects.
#
# Proves: a tool call that was logged as intent but killed before its result
#          got written is re-driven exactly once on resume — the output file
#          ends with exactly one line, and the run reaches Completed.
#
# Requires: a running Postgres reachable via DATABASE_URL.
set -euo pipefail

DB_URL="${DATABASE_URL:-postgres://localhost:5432/zincir}"
export DATABASE_URL="$DB_URL"

OUT_FILE="$(mktemp -t zincir-out.XXXXXX)"
export ZINCIR_OUTPUT_FILE="$OUT_FILE"
# Widen the crash window: the executor sleeps 5s before the side effect.
export ZINCIR_PAUSE_TOOL_MS=5000
export RUST_LOG="info"

cleanup() {
    kill -9 "${CHILD_PID:-}" 2>/dev/null || true
    wait "${CHILD_PID:-}" 2>/dev/null || true
}
trap cleanup EXIT

echo "== setup: fresh database =="
dropdb --if-exists zincir 2>/dev/null || true
createdb zincir

echo "== 1. start fresh run, kill -9 during the tool pause =="
cargo run --quiet &
CHILD_PID=$!

# Wait for the tool_call event (intent) to land, then kill mid-sleep.
# cargo startup ~1s + 5s sleep => at t=2s the process is guaranteed mid-sleep,
# the tool_call event is written, and the file side-effect has NOT happened.
sleep 2
echo "   killing pid $CHILD_PID mid-tool-call"
kill -9 "$CHILD_PID"
wait "$CHILD_PID" 2>/dev/null || true
CHILD_PID=""

echo "   file contents after kill:"
if [[ -s "$OUT_FILE" ]]; then
    cat "$OUT_FILE"
    LINES_AFTER_KILL=$(wc -l < "$OUT_FILE" | tr -d ' ')
    echo "   line count: $LINES_AFTER_KILL"
else
    echo "   (empty)"
    LINES_AFTER_KILL=0
fi

echo "== 2. resume — should re-drive the orphaned tool_call exactly once =="
export ZINCIR_RESUME=1
# No pause this time so the test runs fast; the side effect is idempotent
# in shape (one append per issuance) and resume dedups by idempotency_key
# at the *event* level. The file is our external proof.
unset ZINCIR_PAUSE_TOOL_MS

cargo run --quiet

echo "   file contents after resume:"
cat "$OUT_FILE"
LINES_AFTER_RESUME=$(wc -l < "$OUT_FILE" | tr -d ' ')
echo "   line count: $LINES_AFTER_RESUME"

echo "== 3. assertions =="
fail=0
if [[ "$LINES_AFTER_RESUME" -ne 1 ]]; then
    echo "FAIL: expected exactly 1 line in output file, got $LINES_AFTER_RESUME"
    echo "      (>1 means the tool side effect was duplicated across crash-resume)"
    fail=1
fi

STATUS=$(psql "$DB_URL" -tAc \
    "SELECT status FROM agent_runs WHERE parent_run_id IS NULL ORDER BY created_at LIMIT 1")
if [[ "$STATUS" != "completed" ]]; then
    echo "FAIL: expected run status 'completed', got '$STATUS'"
    fail=1
fi

TOOL_CALLS=$(psql "$DB_URL" -tAc \
    "SELECT count(*) FROM events WHERE event_type = 'tool_call'")
TOOL_RESULTS=$(psql "$DB_URL" -tAc \
    "SELECT count(*) FROM events WHERE event_type = 'tool_result'")
if [[ "$TOOL_CALLS" -ne 1 ]] || [[ "$TOOL_RESULTS" -ne 1 ]]; then
    echo "FAIL: expected 1 tool_call + 1 tool_result, got $TOOL_CALLS/$TOOL_RESULTS"
    fail=1
fi

rm -f "$OUT_FILE"

if [[ "$fail" -eq 0 ]]; then
    echo
    echo "PASS: kill -9 → resume → exactly-once side effect, run completed."
    exit 0
else
    exit 1
fi