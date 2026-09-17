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
# 收敛判据（M5.2 起对齐设计）：只要求**当前配置的成员**收敛。
# 被移除的节点是否学到"自己被移除"是**尽力而为**（设计 v1.4(a)）：移除条目由"当时的 Leader"
# 送达，若该 Leader 在送达前被替换，新 Leader 不会再送达它。此时该节点停留在旧配置 —
# 这在安全上是无害的：J4 让它拿不到票，且 M5.2 起非成员候选者的任期不再被采纳，因此它
# 无法再用竞选搅乱集群。用例 A29 守"同一 Leader 下必须确认送达"，本脚本守"当前成员收敛"。
wait_config_converged() {
  for _ in $(seq 1 600); do
    local ref="" refmembers="" ok=1
    # 先取一个存活节点的视图作为基准配置
    for id in 1 2 3 4 5 6; do
      alive "$id" || continue
      local cv ms
      cv="$(field "$id" config_version 2>/dev/null || true)"
      ms="$(field "$id" members 2>/dev/null || true)"
      [[ -n "$cv" && -n "$ms" ]] || continue
      ref="$cv|$ms"; refmembers=",$ms,"; break
    done
    [[ -n "$ref" ]] || { sleep 0.05; continue; }
    for id in 1 2 3 4 5 6; do
      alive "$id" || continue
      # 只检查"当前配置成员"（被移除的节点不参与收敛判定）
      case "$refmembers" in *",$id:"*) ;; *) continue;; esac
      local cv ms
      cv="$(field "$id" config_version 2>/dev/null || true)"
      ms="$(field "$id" members 2>/dev/null || true)"
      [[ -n "$cv" && -n "$ms" ]] || { ok=0; break; }
      [[ "$ref" == "$cv|$ms" ]] || { ok=0; break; }
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
# M5.6：`cli get` 在 key 不存在时返回非零，而脚本是 `set -e`：
# 裸的 `out="$(cli ... get k)"` 一旦非零就**静默退出**（没有 FAIL 行、没有 dump），
# 实测在 5 节点故障注入 50 轮里偶发（M5.6 定位：ERR at line 181）。
# 这里做成"有限次重试 + 明确诊断"：读不到要报出来，而不是悄悄退出。
# 参数：<leader id> <key>；成功时把值打到 stdout。
read_key_retry() { # <leader id> <key>
  local leader="$1" key="$2" out="" i
  for i in $(seq 1 40); do
    if out="$(cli --host 127.0.0.1 --port "${PORT[$leader]}" get "$key" 2>/dev/null)"; then
      echo "$out"; return 0
    fi
    sleep 0.25
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
  if ! out="$(read_key_retry "$LEADER" f5)"; then
    echo "FAIL iter $i: 线性一致读 f5 连续 10s 失败（leader=$LEADER）" >&2
    for id in 1 2 3 4 5 6; do
      echo "  node$id: role=$(field "$id" role) applied=$(field "$id" last_applied) commit=$(field "$id" commit_index)" >&2
    done
    exit 1
  fi
  [[ "$out" == "v" ]] || { echo "FAIL iter $i: 线性一致读 f5 得到 '$out'" >&2; exit 1; }
  echo "iter $i OK ($MODE node$T, killed_leader=$KILLED_LEADER, cv=$(field "$LEADER" config_version))"
done

echo "== C) 收尾：无故障下 add/remove 往返 =="
# 收尾判据以**成员状态**为准，而不是单次 CLI 退出码：50 轮故障后 6 号可能落后很多，
# 服务端 catch-up 预算是 30s，客户端若超时返回会被误判成"变更被拒"（M4 评审 O6）。
ensure_member() { # <want: yes|no>
  local want="$1" tries="$2" i
  for i in $(seq 1 "$tries"); do
    LEADER="$(require_leader)" || return 1
    if [[ "$want" == "yes" ]]; then
      node6_is_member "$LEADER" && return 0
      cli --host 127.0.0.1 --port "${PORT[$LEADER]}" add 6 "127.0.0.1:${PORT[6]}" >/dev/null 2>&1 || true
    else
      node6_is_member "$LEADER" || return 0
      cli --host 127.0.0.1 --port "${PORT[$LEADER]}" remove 6 >/dev/null 2>&1 || true
    fi
    sleep 2
  done
  return 1
}
LEADER="$(require_leader)"
ensure_member yes 3 || { echo "FAIL: 收尾 add 6 未生效" >&2; exit 1; }
wait_config_converged || { echo "FAIL: 收尾 add 未收敛" >&2; exit 1; }
ensure_member no 3 || { echo "FAIL: 收尾 remove 6 未生效" >&2; exit 1; }
wait_config_converged || { echo "FAIL: 收尾 remove 未收敛" >&2; exit 1; }
fill_and_verify 100 8
echo "raft_membership_fault: PASS ($REPEAT iterations)"
