#!/usr/bin/env bash
# End-to-end test for M3 snapshots (dynamic per-run ports).
#   * 3 nodes with a small snapshot threshold -> snapshots + log compaction
#   * put/get through the leader, snapshot files must exist
#   * kill -9 a follower, write more, restart it -> recovery + catch-up
#   * kill both followers -> a write must NOT return OK
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
    [[ -n "${PIDS[$id]:-}" ]] && kill -9 "${PIDS[$id]}" 2>/dev/null || true
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
    --peers "$PEERS" --data-dir "$WORK/node$id" --snapshot-threshold 8 \
    >"$WORK/node$id.log" 2>&1 &
  PIDS[$id]=$!
}

cli() { "$BIN/raftkv_raft_cli" --peers "$PEERS" "$@"; }

# 节点瞬时不可达时必须返回空串：脚本开了 set -e + pipefail，
# `x="$(field ...)"` 一旦失败会静默退出（没有任何诊断）。
field() {
{   cli --host 127.0.0.1 --port "$(node_port "$1")" status 2>/dev/null | tr ' ' '\n' | sed -n "s/^$2=//p"; } || true
}

find_leader() {
  for id in 1 2 3; do
    if cli --host 127.0.0.1 --port "$(node_port "$id")" status 2>/dev/null | grep -q 'role=leader'; then
      echo "$id"; return 0
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

wait_field_at_least() { # <id> <field> <min>
  for _ in $(seq 1 200); do
    local v
    v=$(field "$1" "$2" 2>/dev/null || true)
    if [[ "$v" =~ ^[0-9]+$ ]] && (( v >= $3 )); then return 0; fi
    sleep 0.05
  done
  return 1
}

for id in 1 2 3; do start_node "$id"; done
wait_leader

# ---- writes + reads ---------------------------------------------------------
for i in $(seq 1 60); do
  cli --host 127.0.0.1 --port "$PORT1" put "k$i" "v$i" >/dev/null
done
out=$(cli --host 127.0.0.1 --port "$PORT2" get k60); expect "$out" "v60" "get k60"
out=$(cli --host 127.0.0.1 --port "$PORT3" get k1);  expect "$out" "v1" "get k1"

# ---- snapshots exist and cover a prefix -------------------------------------
for id in 1 2 3; do
  wait_field_at_least "$id" snapshot_index 8 || {
    echo "FAIL: node$id produced no snapshot" >&2; exit 1; }
  [[ -s "$WORK/node$id/raft/snapshot.dat" ]] || {
    echo "FAIL: node$id snapshot.dat missing" >&2; exit 1; }
done

# ---- restart recovery + catch-up via snapshot --------------------------------
LEADER="$(require_leader)"
FOLLOWER=1
[[ "$LEADER" == "1" ]] && FOLLOWER=2
kill -9 "${PIDS[$FOLLOWER]}" 2>/dev/null || true
PIDS[$FOLLOWER]=""
for i in $(seq 61 80); do
  cli --host 127.0.0.1 --port "$(node_port "$LEADER")" put "k$i" "v$i" >/dev/null
done
start_node "$FOLLOWER"
LEADER_APPLIED="$(field "$LEADER" last_applied)"
wait_field_at_least "$FOLLOWER" last_applied "$LEADER_APPLIED" || {
  echo "FAIL: node$FOLLOWER did not catch up after restart" >&2; exit 1; }

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

echo "raft_snapshot_e2e: PASS"
