#!/usr/bin/env bash
# Fault-injection stability test for the 3-node raftkv cluster (M2.5).
# Uses per-run random ports. Per iteration:
#   * SIGSTOP one follower, write (majority survives), SIGCONT, verify catch-up
#   * SIGSTOP both followers, write must NOT return OK, SIGCONT
# Usage: scripts/raft_fault.sh [--repeat N]   (default 50)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/build/bin"
WORK="$(mktemp -d)"

REPEAT=50
if [[ "${1:-}" == "--repeat" ]]; then REPEAT="${2:-50}"; fi

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
    --peers "$PEERS" --data-dir "$WORK/node$id" >"$WORK/node$id.log" 2>&1 &
  PIDS[$id]=$!
}

cli() { "$BIN/raftkv_raft_cli" --peers "$PEERS" "$@"; }

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
  return 1
}

last_applied() {
  cli --host 127.0.0.1 --port "$(node_port "$1")" status 2>/dev/null \
    | tr ' ' '\n' | sed -n 's/^last_applied=//p'
}

wait_caught_up() {
  local follower=$1 leader=$2
  for _ in $(seq 1 100); do
    if [[ "$(last_applied "$follower")" == "$(last_applied "$leader")" ]]; then
      return 0
    fi
    sleep 0.05
  done
  return 1
}

for id in 1 2 3; do start_node "$id"; done
wait_leader

for i in $(seq 1 "$REPEAT"); do
  LEADER="$(find_leader)"
  FOLLOWER_A=""
  FOLLOWER_B=""
  for id in 1 2 3; do
    if [[ "$id" != "$LEADER" ]]; then
      if [[ -z "$FOLLOWER_A" ]]; then FOLLOWER_A=$id; else FOLLOWER_B=$id; fi
    fi
  done

  # -- partition one follower, write (majority must still succeed) -----------
  kill -STOP "${PIDS[$FOLLOWER_A]}" 2>/dev/null || true
  for k in $(seq 1 5); do
    out=$(cli --host 127.0.0.1 --port "$(node_port "$LEADER")" put "fault_key_${i}_${k}" v || true)
    if [[ "$out" != "OK" ]]; then
      echo "FAIL iter $i: write with one follower stopped returned '$out'" >&2
      exit 1
    fi
  done
  kill -CONT "${PIDS[$FOLLOWER_A]}" 2>/dev/null || true
  wait_caught_up "$FOLLOWER_A" "$LEADER" || {
    echo "FAIL iter $i: follower $FOLLOWER_A did not catch up" >&2
    exit 1
  }

  # -- partition both followers, write must NOT succeed ----------------------
  kill -STOP "${PIDS[$FOLLOWER_A]}" 2>/dev/null || true
  kill -STOP "${PIDS[$FOLLOWER_B]}" 2>/dev/null || true
  out=$(cli --host 127.0.0.1 --port "$(node_port "$LEADER")" put "minority_$i" no || true)
  if [[ "$out" == "OK" ]]; then
    echo "FAIL iter $i: write succeeded without a majority" >&2
    exit 1
  fi
  kill -CONT "${PIDS[$FOLLOWER_A]}" 2>/dev/null || true
  kill -CONT "${PIDS[$FOLLOWER_B]}" 2>/dev/null || true
  wait_leader
done

echo "raft_fault: PASS ($REPEAT iterations)"
