#!/usr/bin/env bash
# Verifies crash recovery before and after an idempotent tool side effect.
# Destructive: only runs against a database named zincir_test.
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
BIN="$ROOT_DIR/target/debug/zincir"
TMP_ROOT="$(mktemp -d -t zincir-crash.XXXXXX)"
CHILD_PID=""
START_LOG=""
RESUME_LOG=""

cleanup() {
    if [[ -n "$CHILD_PID" ]] && kill -0 "$CHILD_PID" 2>/dev/null; then
        kill -9 "$CHILD_PID" 2>/dev/null || true
        wait "$CHILD_PID" 2>/dev/null || true
    fi
    rm -rf "$TMP_ROOT"
}
trap cleanup EXIT

fail() {
    echo "FAIL: $1" >&2
    if [[ -n "$START_LOG" ]]; then
        echo "--- initial run ---" >&2
        cat "$START_LOG" >&2 || true
    fi
    if [[ -n "$RESUME_LOG" ]]; then
        echo "--- resume run ---" >&2
        cat "$RESUME_LOG" >&2 || true
    fi
    exit 1
}

sql() {
    psql "$DB_URL" -XAtq -v ON_ERROR_STOP=1 -c "$1"
}

reset_database() {
    sql "DROP SCHEMA public CASCADE; CREATE SCHEMA public;" >/dev/null
}

file_inode() {
    if [[ "$(uname -s)" == "Darwin" ]]; then
        stat -f '%i' "$1"
    else
        stat -c '%i' "$1"
    fi
}

find_running_tool_call() {
    sql "
        SELECT e.run_id
        FROM events e
        JOIN agent_runs r ON r.id = e.run_id
        WHERE e.event_type = 'tool_call' AND r.status = 'running'
        ORDER BY e.created_at DESC
        LIMIT 1;
    " 2>/dev/null || true
}

wait_for_crash_point() {
    local crash_point="$1"
    local effect_file="$2"
    local run_id=""

    for _ in {1..200}; do
        run_id="$(find_running_tool_call)"
        if [[ -n "$run_id" ]]; then
            if [[ "$crash_point" == "before-effect" && ! -e "$effect_file" ]]; then
                printf '%s' "$run_id"
                return
            fi
            if [[ "$crash_point" == "after-effect" && -e "$effect_file" ]]; then
                printf '%s' "$run_id"
                return
            fi
        fi

        if ! kill -0 "$CHILD_PID" 2>/dev/null; then
            wait "$CHILD_PID" 2>/dev/null || true
            CHILD_PID=""
            fail "zincir exited before reaching $crash_point"
        fi
        sleep 0.1
    done

    fail "timed out waiting for $crash_point"
}

assert_run_completed_once() {
    local run_id="$1"
    local effect_file="$2"
    local expected_inode="${3:-}"

    local status tool_calls tool_results event_types effect_count
    status="$(sql "SELECT status FROM agent_runs WHERE id = '$run_id';")"
    tool_calls="$(sql "SELECT count(*) FROM events WHERE run_id = '$run_id' AND event_type = 'tool_call';")"
    tool_results="$(sql "SELECT count(*) FROM events WHERE run_id = '$run_id' AND event_type = 'tool_result';")"
    event_types="$(sql "SELECT string_agg(event_type, ',' ORDER BY seq) FROM events WHERE run_id = '$run_id';")"
    effect_count="$(find "$(dirname "$effect_file")" -maxdepth 1 -type f -name 'call_1.json' | wc -l | tr -d ' ')"

    [[ "$status" == "completed" ]] || fail "expected completed, got $status"
    [[ "$tool_calls" == "1" ]] || fail "expected 1 tool_call, got $tool_calls"
    [[ "$tool_results" == "1" ]] || fail "expected 1 tool_result, got $tool_results"
    [[ "$event_types" == "llm_call,tool_call,tool_result,llm_call" ]] ||
        fail "unexpected event order: $event_types"
    [[ "$effect_count" == "1" ]] || fail "expected one effect file, got $effect_count"
    if [[ -n "$expected_inode" ]]; then
        local actual_inode
        actual_inode="$(file_inode "$effect_file")"
        [[ "$actual_inode" == "$expected_inode" ]] ||
            fail "cached effect file was replaced during resume"
    fi
}

run_case() {
    local crash_point="$1"
    local case_dir="$TMP_ROOT/$crash_point"
    local effect_file="$case_dir/call_1.json"
    mkdir -p "$case_dir"

    START_LOG="$case_dir/start.log"
    RESUME_LOG="$case_dir/resume.log"
    export ZINCIR_OUTPUT_DIR="$case_dir"
    unset ZINCIR_RESUME ZINCIR_PAUSE_BEFORE_TOOL_MS ZINCIR_PAUSE_AFTER_TOOL_MS

    if [[ "$crash_point" == "before-effect" ]]; then
        export ZINCIR_PAUSE_BEFORE_TOOL_MS=60000
    else
        export ZINCIR_PAUSE_AFTER_TOOL_MS=60000
    fi

    reset_database

    echo "== starting $crash_point case =="
    "$BIN" >"$START_LOG" 2>&1 &
    CHILD_PID=$!

    local run_id
    run_id="$(wait_for_crash_point "$crash_point" "$effect_file")"

    echo "== killing zincir at $crash_point (run $run_id) =="
    kill -9 "$CHILD_PID"
    wait "$CHILD_PID" 2>/dev/null || true
    CHILD_PID=""

    local status_before effect_inode=""
    status_before="$(sql "SELECT status FROM agent_runs WHERE id = '$run_id';")"
    [[ "$status_before" == "running" ]] ||
        fail "expected running before resume, got $status_before"
    if [[ -e "$effect_file" ]]; then
        effect_inode="$(file_inode "$effect_file")"
    fi

    unset ZINCIR_PAUSE_BEFORE_TOOL_MS ZINCIR_PAUSE_AFTER_TOOL_MS
    echo "== resuming $crash_point case =="
    if ! ZINCIR_RESUME=1 "$BIN" >"$RESUME_LOG" 2>&1; then
        fail "resume process failed"
    fi

    assert_run_completed_once "$run_id" "$effect_file" "$effect_inode"
    effect_inode="$(file_inode "$effect_file")"

    # A completed run must remain a no-op on subsequent resume attempts.
    if ! ZINCIR_RESUME=1 "$BIN" >>"$RESUME_LOG" 2>&1; then
        fail "second resume process failed"
    fi
    assert_run_completed_once "$run_id" "$effect_file" "$effect_inode"

    echo "PASS: $crash_point recovered without duplicating observable state"
}

echo "== checking dedicated test database =="
sql "SELECT 1" >/dev/null || fail "cannot connect to $DB_URL"

echo "== building zincir =="
cargo build --quiet

run_case before-effect
run_case after-effect

echo "PASS: all crash boundaries recovered"
