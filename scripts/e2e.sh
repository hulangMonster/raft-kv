#!/usr/bin/env bash
# End-to-end smoke test for raftkv (single node, milestone M1).
# Starts a server, drives it with raftkv_cli, then crash-restarts it
# (kill -9) to prove WAL replay and durability.
#
# Usage: scripts/e2e.sh   (expects binaries already built under build/bin)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-$ROOT/build}"
BIN_DIR="$BUILD_DIR/bin"
PORT="${PORT:-19527}"

WORK="$(mktemp -d)"
DATA_DIR="$WORK/raftkv-data"
mkdir -p "$DATA_DIR"
SERVER_PID=""

server_bin="$BIN_DIR/raftkv_server"
cli_bin="$BIN_DIR/raftkv_cli"
cli() { "$cli_bin" --host 127.0.0.1 --port "$PORT" "$@"; }

cleanup() {
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill -9 "$SERVER_PID" 2>/dev/null || true
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT

[[ -x "$server_bin" ]] || {
  echo "server binary not found: $server_bin (build first: cmake --build build)" >&2
  exit 1
}
[[ -x "$cli_bin" ]] || {
  echo "client binary not found: $cli_bin (build first: cmake --build build)" >&2
  exit 1
}

start_server() {
  "$server_bin" --port "$PORT" --data-dir "$DATA_DIR" --workers 4 \
    >"$WORK/server.log" 2>&1 &
  SERVER_PID=$!
}

wait_ready() {
  for _ in $(seq 1 100); do
    if cli get __ready_probe__ >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  echo "server did not become ready; server.log:" >&2
  cat "$WORK/server.log" >&2 || true
  return 1
}

expect_eq() { # expect_eq <actual> <expected> <what>
  if [[ "$1" != "$2" ]]; then
    echo "FAIL: $3: expected '$2', got '$1'" >&2
    return 1
  fi
}

stop_server() {
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  SERVER_PID=""
}

# ---- normal lifecycle -------------------------------------------------------
start_server
wait_ready

out=$(cli put hello world);  expect_eq "$out" "OK" "put hello"
out=$(cli get hello);        expect_eq "$out" "world" "get hello"
out=$(cli put hello raftkv); expect_eq "$out" "OK" "overwrite hello"
out=$(cli get hello);        expect_eq "$out" "raftkv" "get overwritten"
out=$(cli del hello);        expect_eq "$out" "OK" "del hello"
out=$(cli get hello);        expect_eq "$out" "NOT_FOUND" "get deleted"

# ---- durability: kill -9 then replay ---------------------------------------
cli put durable yes >/dev/null
kill -9 "$SERVER_PID" 2>/dev/null || true
wait "$SERVER_PID" 2>/dev/null || true
SERVER_PID=""

start_server
wait_ready
out=$(cli get durable); expect_eq "$out" "yes" "replay after kill -9"
out=$(cli get hello);   expect_eq "$out" "NOT_FOUND" "replay keeps deletes"

stop_server
echo "e2e: PASS"
