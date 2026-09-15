#!/usr/bin/env bash
# End-to-end test for the 3-node raftkv cluster (M2.4).
# Uses per-run random ports so rapid consecutive runs never collide.
#   * start 3 nodes, wait for a leader
#   * put/get/overwrite/del (client auto-redirects to the leader)
#   * kill -9 the leader -> new leader elected, data survives
#   * kill the two followers -> a write must NOT return OK (no majority)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/build/bin"
WORK="$(mktemp -d)"

BASE=$((19000 + ($$ % 800)))
PORT1=$BASE
PORT2=$((BASE + 1))
PORT3=$((BASE + 2))
PEERS="1=127.0.0.1:$PORT1,2=127.0.0.1:$PORT2,3=127.0.0.1:$PORT3"

PIDS=()
cleanup() {
  for id in 1 2 3; do
    if [[ -n "${PIDS[$id]:-}" ]]; then kill -9 "${PIDS[$id]}" 2>/dev/null || true; fi
  done
  rm -rf "$WORK"
}
trap cleanup EXIT

node_port() {
  case "$1" in
    1) echo "$PORT1";;
    2) echo "$PORT2";;
    3) echo "$PORT3";;
  esac
}

start_node() {
  local id=$1
  "$BIN/raftkv_raft_node" --id "$id" --port "$(node_port "$id")" \
    --peers "$PEERS" --data-dir "$WORK/node$id" >"$WORK/node$id.log" 2>&1 &
  PIDS[$id]=$!
}

cli() { "$BIN/raftkv_raft_cli" --peers "$PEERS" "$@"; }

find_leader() {
  for id in 1 2 3; do
    if cli --host 127.0.0.1 --port "$(node_port "$id")" status 2>/dev/null | grep -q 'role=leader'; then
      echo "$id"
      return 0
    fi
  done
  return 1
}

wait_leader() {
  for _ in $(seq 1 300); do
    if find_leader >/dev/null; then return 0; fi
    sleep 0.1
  done
  echo "no leader within 30s" >&2
  for id in 1 2 3; do echo "--- node$id ---"; cat "$WORK/node$id.log" 2>/dev/null; done
  return 1
}

# find_leader() can transiently see no leader while a re-election is in flight;
# under 'set -e' a bare $(find_leader) assignment would then exit silently.
require_leader() {
  for _ in $(seq 1 200); do
    local l
    if l="$(find_leader)"; then echo "$l"; return 0; fi
    sleep 0.1
  done
  echo "no leader within 20s" >&2
  return 1
}

expect() {
  if [[ "$1" != "$2" ]]; then
    echo "FAIL: $3: expected '$2', got '$1'" >&2
    return 1
  fi
}

for id in 1 2 3; do start_node "$id"; done
wait_leader

out=$(cli --host 127.0.0.1 --port "$PORT1" put hello world); expect "$out" "OK" "put hello"
out=$(cli --host 127.0.0.1 --port "$PORT2" get hello);   expect "$out" "world" "get hello"
out=$(cli --host 127.0.0.1 --port "$PORT3" put hello raft); expect "$out" "OK" "overwrite hello"
out=$(cli --host 127.0.0.1 --port "$PORT1" get hello);   expect "$out" "raft" "get overwritten"
out=$(cli --host 127.0.0.1 --port "$PORT2" del hello);   expect "$out" "OK" "del hello"
out=$(cli --host 127.0.0.1 --port "$PORT3" get hello);   expect "$out" "NOT_FOUND" "get deleted"

# ---- durability: kill -9 the leader -----------------------------------------
cli --host 127.0.0.1 --port "$PORT1" put durable yes >/dev/null
LEADER="$(require_leader)"
kill -9 "${PIDS[$LEADER]}" 2>/dev/null || true
PIDS[$LEADER]=""
wait_leader
NEW_LEADER="$(require_leader)"
# The new leader commits a no-op entry, which also applies `durable`.
# Poll briefly for it to become visible.
out=""
for _ in $(seq 1 50); do
  out=$(cli --host 127.0.0.1 --port "$(node_port "$NEW_LEADER")" get durable || true)
  [[ "$out" == "yes" ]] && break
  sleep 0.1
done
expect "$out" "yes" "replay after kill -9"

# ---- no majority: kill both followers ---------------------------------------
LEADER="$(require_leader)"
for id in 1 2 3; do
  if [[ "$id" != "$LEADER" ]]; then
    [[ -n "${PIDS[$id]:-}" ]] && kill -9 "${PIDS[$id]}" 2>/dev/null || true
    PIDS[$id]=""
  fi
done
sleep 0.5
out=$(cli --host 127.0.0.1 --port "$(node_port "$LEADER")" put minority no || true)
if [[ "$out" == "OK" ]]; then
  echo "FAIL: write succeeded without a majority" >&2
  exit 1
fi

echo "raft_e2e: PASS"
