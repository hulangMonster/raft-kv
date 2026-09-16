#!/usr/bin/env bash
# M3.5 stress + fault-injection test for snapshots (dynamic per-run ports).
#   A) 100k entries: raft.log must stay bounded (compaction works)
#   B) an empty node catches up via InstallSnapshot
#   C) --repeat N: alternate kill -9 (crash mid-snapshot) and SIGSTOP
#      (stalled during transfer); every node must catch up afterwards
# Usage: scripts/raft_snapshot_fault.sh [--repeat N]   (default 50)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/build/bin"
WORK="$(mktemp -d)"

REPEAT=50
if [[ "${1:-}" == "--repeat" ]]; then REPEAT="${2:-50}"; fi

BASE=$((19000 + ($$ % 600)))
PORT1=$BASE
PORT2=$((BASE + 1))
PORT3=$((BASE + 2))
PEERS="1=127.0.0.1:$PORT1,2=127.0.0.1:$PORT2,3=127.0.0.1:$PORT3"
THRESHOLD=2000
LOG_BOUND=1048576  # 1 MiB: ~2000 small entries fit well under this

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
    --peers "$PEERS" --data-dir "$WORK/node$id" --snapshot-threshold "$THRESHOLD" \
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

wait_field_at_least() { # <id> <field> <min>
  for _ in $(seq 1 1200); do
    local v
    v=$(field "$1" "$2" 2>/dev/null || true)
    if [[ "$v" =~ ^[0-9]+$ ]] && (( v >= $3 )); then return 0; fi
    sleep 0.05
  done
  return 1
}

wait_node_up() { # <id>: wait until the node answers status again
  for _ in $(seq 1 100); do
    if [[ -n "$(field "$1" role)" ]]; then return 0; fi
    sleep 0.1
  done
  return 1
}

for id in 1 2 3; do start_node "$id"; done
wait_leader
sleep 1  # let the first election settle before the bulk write

# ---- A) 100k entries -> raft.log bounded ------------------------------------
cli --host 127.0.0.1 --port "$PORT1" fill 100000 --pipeline 64 >/dev/null
LEADER="$(require_leader)"
for id in 1 2 3; do
  wait_field_at_least "$id" last_applied 100000 || {
    echo "FAIL: node$id did not apply 100k entries" >&2; exit 1; }
  sz=$(stat -c%s "$WORK/node$id/raft/raft.log" 2>/dev/null || echo 0)
  if (( sz > LOG_BOUND )); then
    echo "FAIL: node$id raft.log=$sz exceeds $LOG_BOUND (not bounded)" >&2; exit 1
  fi
done
echo "A) 100000 entries applied, raft.log bounded (<= ${LOG_BOUND}B)"

# ---- B) empty node catches up via InstallSnapshot ---------------------------
LEADER="$(require_leader)"
EMPTY=1
[[ "$LEADER" == "1" ]] && EMPTY=2
kill -9 "${PIDS[$EMPTY]}" 2>/dev/null || true
PIDS[$EMPTY]=""
rm -rf "$WORK/node$EMPTY"  # wipe -> brand-new empty node
start_node "$EMPTY"
LEADER_APPLIED="$(field "$LEADER" last_applied)"
wait_field_at_least "$EMPTY" last_applied "$LEADER_APPLIED" || {
  echo "FAIL: empty node$EMPTY did not catch up via InstallSnapshot" >&2; exit 1; }
echo "B) empty node caught up via InstallSnapshot (last_applied=$LEADER_APPLIED)"

# ---- C) repeated fault injection --------------------------------------------
for i in $(seq 1 "$REPEAT"); do
  LEADER="$(require_leader)"
  FOLLOWER=0
  for id in 1 2 3; do
    if [[ "$id" != "$LEADER" ]]; then FOLLOWER=$id; break; fi
  done

  if (( i % 2 == 1 )); then
    # crash mid-snapshot: kill -9, keep writing, restart, must catch up
    kill -9 "${PIDS[$FOLLOWER]}" 2>/dev/null || true
    PIDS[$FOLLOWER]=""
    sleep 0.3  # let the port free before restart
    cli --host 127.0.0.1 --port "$(node_port "$LEADER")" fill 200 --pipeline 16 >/dev/null
    start_node "$FOLLOWER"
    wait_node_up "$FOLLOWER" || { echo "FAIL iter $i: node$FOLLOWER did not come up" >&2; exit 1; }
  else
    # stalled during transfer: SIGSTOP, advance + force a snapshot, SIGCONT
    kill -STOP "${PIDS[$FOLLOWER]}" 2>/dev/null || true
    cli --host 127.0.0.1 --port "$(node_port "$LEADER")" fill 20 --pipeline 8 >/dev/null || true
    cli --host 127.0.0.1 --port "$(node_port "$LEADER")" snapshot >/dev/null || true
    kill -CONT "${PIDS[$FOLLOWER]}" 2>/dev/null || true
  fi

  # Compare against the CURRENT leader: leadership can legitimately change while
  # the fault is injected (a resumed node campaigns), and a former leader may be
  # ahead of the new one in last_applied.
  caught_up=0
  for _ in $(seq 1 1200); do
    cur_leader="$(find_leader || true)"
    if [[ -n "$cur_leader" ]]; then
      target="$(field "$cur_leader" last_applied)"
      mine="$(field "$FOLLOWER" last_applied)"
      if [[ "$mine" =~ ^[0-9]+$ && "$target" =~ ^[0-9]+$ ]] && (( mine >= target )); then
        caught_up=1; break
      fi
    fi
    sleep 0.05
  done
  if (( caught_up == 0 )); then
    echo "FAIL iter $i: node$FOLLOWER did not catch up" >&2
    for id in 1 2 3; do
      echo "  node$id: role=$(field "$id" role) applied=$(field "$id" last_applied) snap=$(field "$id" snapshot_index) leader_id=$(field "$id" leader_id) term=$(field "$id" term)" >&2
    done
    echo "  (leader was $LEADER, mode=$([[ $((i % 2)) == 1 ]] && echo kill9 || echo sigstop))" >&2
    echo "--- node$FOLLOWER log tail ---" >&2
    tail -8 "$WORK/node$FOLLOWER.log" >&2 || true
    exit 1
  fi
done

echo "C) fault injection PASS ($REPEAT iterations)"
echo "raft_snapshot_fault: PASS"
