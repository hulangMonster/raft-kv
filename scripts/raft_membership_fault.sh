#!/usr/bin/env bash
# M4.5 成员变更 + 故障注入（5 节点，动态端口）。
#   A) 5 节点启动 + 压测；备用节点 6 以 seed=5 启动（动态加入者，非投票）
#   B) --repeat N：每轮轮换一个 follower 注入 kill -9 / SIGSTOP，并在故障窗口内做一次
#      成员变更（按节点 6 当前状态决定 add / remove）；每 4 轮额外在窗口内杀掉 leader。
#      恢复后校验：所有存活节点 config_version + members 收敛一致、压测可继续、
#                  线性一致读命中已提交值。
#   C) 收尾：无故障下再做一次 add / remove 往返，证明集群健康
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/build/bin"
WORK="$(mktemp -d)"

REPEAT=50
if [[ "${1:-}" == "--repeat" ]]; then REPEAT="${2:-50}"; fi

BASE=$((19000 + ($$ % 500)))
declare -a PORT
for i in 1 2 3 4 5 6; do PORT[$i]=$((BASE + i - 1)); done
PEERS5="1=127.0.0.1:${PORT[1]},2=127.0.0.1:${PORT[2]},3=127.0.0.1:${PORT[3]},4=127.0.0.1:${PORT[4]},5=127.0.0.1:${PORT[5]}"
PEERS6="$PEERS5,6=127.0.0.1:${PORT[6]}"

declare -a PIDS
cleanup() {
  for id in 1 2 3 4 5 6; do
    [[ -n "${PIDS[$id]:-}" ]] && kill -9 "${PIDS[$id]}" 2>/dev/null || true
  done
  rm -rf "$WORK"
}
trap cleanup EXIT

start_node() { # <id> <peers>
  "$BIN/raftkv_raft_node" --id "$1" --port "${PORT[$1]}" \
    --peers "$2" --data-dir "$WORK/node$1" --snapshot-threshold 1000 \
    >"$WORK/node$1.log" 2>&1 &
  PIDS[$1]=$!
}

cli() { "$BIN/raftkv_raft_cli" --peers "$PEERS6" "$@"; }
status_of() { cli --host 127.0.0.1 --port "${PORT[$1]}" status 2>/dev/null; }
field() { { status_of "$1" | tr ' ' '\n' | sed -n "s/^$2=//p"; } || true; }
alive() { [[ -n "${PIDS[$1]:-}" ]] && kill -0 "${PIDS[$1]}" 2>/dev/null; }

find_leader() {
  for id in 1 2 3 4 5 6; do
    alive "$id" || continue
    status_of "$id" | grep -q 'role=leader' && { echo "$id"; return 0; }
  done
  return 1
}
require_leader() {
  for _ in $(seq 1 400); do
    local l; if l="$(find_leader)"; then echo "$l"; return 0; fi
    sleep 0.1
  done
  echo "no leader within 40s" >&2; return 1
}
wait_applied_at_least() { # <id> <min>
  for _ in $(seq 1 600); do
    local v; v="$(field "$1" last_applied 2>/dev/null || true)"
    if [[ "$v" =~ ^[0-9]+$ ]] && (( v >= $2 )); then return 0; fi
    sleep 0.05
  done
  return 1
}
wait_config_converged() {
  for _ in $(seq 1 600); do
    local ref="" ok=1
    for id in 1 2 3 4 5 6; do
      alive "$id" || continue
      local cv ms
      cv="$(field "$id" config_version 2>/dev/null || true)"
      ms="$(field "$id" members 2>/dev/null || true)"
      [[ -n "$cv" && -n "$ms" ]] || { ok=0; break; }
      if [[ -z "$ref" ]]; then ref="$cv|$ms"; elif [[ "$ref" != "$cv|$ms" ]]; then ok=0; break; fi
    done
    (( ok == 1 )) && return 0
    sleep 0.05
  done
  # 超时：打印每个存活节点的视图（否则只能看到"未收敛"，无法定位）
  for id in 1 2 3 4 5 6; do
    alive "$id" || continue
    echo "  node$id: cv=$(field "$id" config_version) members=$(field "$id" members) role=$(field "$id" role) retired=$(field "$id" retired)" >&2
  done
  return 1
}
fill_and_verify() { # <n> <pipeline>
  local leader; leader="$(require_leader)"
  cli --host 127.0.0.1 --port "${PORT[$leader]}" fill "$1" --pipeline "$2" >/dev/null
  cli --host 127.0.0.1 --port "${PORT[$leader]}" verify "$1" | grep -q 'missing 0'
}
node6_is_member() { # <leader id>
  local ms; ms="$(field "$1" members 2>/dev/null || true)"
  case ",$ms," in *",6:"*) return 0;; *) return 1;; esac
}
do_change() { # <leader id>
  if node6_is_member "$1"; then
    cli --host 127.0.0.1 --port "${PORT[$1]}" remove 6 >/dev/null 2>&1 || true
  else
    cli --host 127.0.0.1 --port "${PORT[$1]}" add 6 "127.0.0.1:${PORT[6]}" >/dev/null 2>&1 || true
  fi
}

echo "== A) 5 节点启动 + 压测 =="
for id in 1 2 3 4 5; do start_node "$id" "$PEERS5"; done
LEADER="$(require_leader)"
echo "leader=$LEADER"
fill_and_verify 200 8
start_node 6 "$PEERS5"
sleep 1
echo "备用节点 6 已启动（非投票）"

echo "== B) 故障注入 + 成员变更 x$REPEAT =="
for i in $(seq 1 "$REPEAT"); do
  LEADER="$(require_leader)"
  T=0
  for k in 0 1 2 3 4; do
    cand=$(( ((i - 1 + k) % 5) + 1 ))
    if [[ "$cand" != "$LEADER" ]] && alive "$cand"; then T=$cand; break; fi
  done
  (( T == 0 )) && { echo "FAIL iter $i: 找不到可注入的 follower" >&2; exit 1; }

  if (( i % 2 == 1 )); then
    kill -9 "${PIDS[$T]}" 2>/dev/null || true
    PIDS[$T]=""
    MODE=kill9
  else
    kill -STOP "${PIDS[$T]}" 2>/dev/null || true
    MODE=sigstop
  fi

  do_change "$LEADER"

  # 每 4 轮：在变更窗口内连 leader 一起杀掉（覆盖"leader 下台 + 变更在途"）
  KILLED_LEADER=""
  if (( i % 4 == 0 )); then
    KILLED_LEADER="$LEADER"
    kill -9 "${PIDS[$LEADER]}" 2>/dev/null || true
    PIDS[$LEADER]=""
  fi

  if [[ "$MODE" == kill9 ]]; then
    sleep 0.3
    start_node "$T" "$PEERS5"
  else
    kill -CONT "${PIDS[$T]}" 2>/dev/null || true
  fi
  if [[ -n "$KILLED_LEADER" ]]; then
    sleep 0.3
    start_node "$KILLED_LEADER" "$PEERS5"
  fi

  LEADER="$(require_leader)"
  wait_applied_at_least "$T" "$(field "$LEADER" commit_index)" || {
    echo "FAIL iter $i ($MODE node$T): node$T 未追平" >&2; exit 1; }
  wait_config_converged || {
    echo "FAIL iter $i ($MODE on node$T, killed_leader=$KILLED_LEADER): 配置未收敛" >&2
    for id in 1 2 3 4 5 6; do
      echo "  node$id: cv=$(field "$id" config_version) members=$(field "$id" members) role=$(field "$id" role)" >&2
    done
    exit 1; }
  fill_and_verify 100 8 || { echo "FAIL iter $i: 压测/校验失败" >&2; exit 1; }
  out="$(cli --host 127.0.0.1 --port "${PORT[$LEADER]}" get f5)"
  [[ "$out" == "v" ]] || { echo "FAIL iter $i: 线性一致读 f5 得到 '$out'" >&2; exit 1; }
  echo "iter $i OK ($MODE node$T, killed_leader=$KILLED_LEADER, cv=$(field "$LEADER" config_version))"
done

echo "== C) 收尾：无故障下 add/remove 往返 =="
LEADER="$(require_leader)"
if ! node6_is_member "$LEADER"; then
  cli --host 127.0.0.1 --port "${PORT[$LEADER]}" add 6 "127.0.0.1:${PORT[6]}" | grep -q '^OK'
fi
wait_config_converged || { echo "FAIL: 收尾 add 未收敛" >&2; exit 1; }
cli --host 127.0.0.1 --port "${PORT[$LEADER]}" remove 6 | grep -q '^OK'
wait_config_converged || { echo "FAIL: 收尾 remove 未收敛" >&2; exit 1; }
fill_and_verify 100 8
echo "raft_membership_fault: PASS ($REPEAT iterations)"
