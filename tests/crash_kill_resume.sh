#!/usr/bin/env bash
# Verifies SQLite-backed crash recovery before and after an idempotent effect.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

export RUST_LOG="info"
BIN="$ROOT_DIR/target/debug/zincir"
TMP_ROOT="$(mktemp -d -t zincir-crash.XXXXXX)"
CHILD_PID=""
START_LOG=""
RESUME_LOG=""
DATABASE_PATH=""

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
    sqlite3 -batch -noheader "$DATABASE_PATH" "$1"
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
        SELECT lower(hex(e.run_id))
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
    status="$(sql "SELECT status FROM agent_runs WHERE lower(hex(id)) = '$run_id';")"
    tool_calls="$(sql "SELECT count(*) FROM events WHERE lower(hex(run_id)) = '$run_id' AND event_type = 'tool_call';")"
    tool_results="$(sql "SELECT count(*) FROM events WHERE lower(hex(run_id)) = '$run_id' AND event_type = 'tool_result';")"
    event_types="$(sql "
        SELECT group_concat(event_type, ',')
        FROM (
            SELECT event_type FROM events
            WHERE lower(hex(run_id)) = '$run_id'
            ORDER BY seq
        );
    ")"
    effect_count="$(find "$(dirname "$effect_file")" -maxdepth 1 -type f -name 'call_1.json' | wc -l | tr -d ' ')"

    [[ "$status" == "completed" ]] || fail "expected completed, got $status"
    [[ "$tool_calls" == "1" ]] || fail "expected 1 tool_call, got $tool_calls"
    [[ "$tool_results" == "1" ]] || fail "expected 1 tool_result, got $tool_results"
    [[ "$event_types" == "state_transition,llm_call,tool_call,tool_result,llm_call,state_transition" ]] ||
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
    DATABASE_PATH="$case_dir/zincir.db"
    export ZINCIR_DATABASE_PATH="$DATABASE_PATH"
    export ZINCIR_OUTPUT_DIR="$case_dir"
    unset ZINCIR_RESUME ZINCIR_PAUSE_BEFORE_TOOL_MS ZINCIR_PAUSE_AFTER_TOOL_MS

    if [[ "$crash_point" == "before-effect" ]]; then
        export ZINCIR_PAUSE_BEFORE_TOOL_MS=60000
    else
        export ZINCIR_PAUSE_AFTER_TOOL_MS=60000
    fi

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
    status_before="$(sql "SELECT status FROM agent_runs WHERE lower(hex(id)) = '$run_id';")"
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

    if ! ZINCIR_RESUME=1 "$BIN" >>"$RESUME_LOG" 2>&1; then
        fail "second resume process failed"
    fi
    assert_run_completed_once "$run_id" "$effect_file" "$effect_inode"

    echo "PASS: $crash_point recovered without duplicating observable state"
}

echo "== building zincir =="
cargo build --quiet

run_case before-effect
run_case after-effect

echo "PASS: all SQLite crash boundaries recovered"
