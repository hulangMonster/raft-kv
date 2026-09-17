#!/usr/bin/env bash
# M4 成员变更 + 线性一致读 e2e（动态端口，独立于 M1/M2/M3 既有脚本）。
#   1) 3 节点启动，压测（fill + verify）
#   2) 第 4 个节点以 seed=3 节点启动（非投票），add 进集群
#   3) 4 节点继续压测；线性一致读命中已提交值
#   4) remove 掉第 4 个节点，集群回到 3 节点并继续可用
#   5) 被移除节点必须退役（status: retired=true）
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/build/bin"
WORK="$(mktemp -d)"

BASE=$((19000 + ($$ % 600)))
P1=$BASE; P2=$((BASE + 1)); P3=$((BASE + 2)); P4=$((BASE + 3))
PEERS3="1=127.0.0.1:$P1,2=127.0.0.1:$P2,3=127.0.0.1:$P3"
PEERS4="$PEERS3,4=127.0.0.1:$P4"

PIDS=()
cleanup() {
  for id in 1 2 3 4; do
    [[ -n "${PIDS[$id]:-}" ]] && kill -9 "${PIDS[$id]}" 2>/dev/null || true
  done
  rm -rf "$WORK"
}
trap cleanup EXIT

node_port() { case "$1" in 1) echo "$P1";; 2) echo "$P2";; 3) echo "$P3";; 4) echo "$P4";; esac; }

start_node() { # <id> <peers>
  "$BIN/raftkv_raft_node" --id "$1" --port "$(node_port "$1")" \
    --peers "$2" --data-dir "$WORK/node$1" --snapshot-threshold 2000 \
    >"$WORK/node$1.log" 2>&1 &
  PIDS[$1]=$!
}

cli() { "$BIN/raftkv_raft_cli" --peers "$PEERS4" "$@"; }
status_of() { cli --host 127.0.0.1 --port "$(node_port "$1")" status 2>/dev/null; }
field() { { status_of "$1" | tr ' ' '\n' | sed -n "s/^$2=//p"; } || true; }
find_leader() {
  for id in 1 2 3 4; do
    [[ -n "${PIDS[$id]:-}" ]] || continue
    if status_of "$id" | grep -q 'role=leader'; then echo "$id"; return 0; fi
  done
  return 1
}
require_leader() {
  for _ in $(seq 1 200); do
    local l; if l="$(find_leader)"; then echo "$l"; return 0; fi
    sleep 0.1
  done
  echo "no leader within 20s" >&2; return 1
}
wait_config_version_ge() { # <id> <min>
  for _ in $(seq 1 400); do
    local v; v="$(field "$1" config_version 2>/dev/null || true)"
    if [[ "$v" =~ ^[0-9]+$ ]] && (( v >= $2 )); then return 0; fi
    sleep 0.05
  done
  return 1
}
wait_config_has() { # <id> <member>
  for _ in $(seq 1 400); do
    local m; m="$(field "$1" members 2>/dev/null || true)"
    case ",$m," in *",$2:"*) return 0;; esac
    sleep 0.05
  done
  return 1
}
wait_config_lacks() { # <id> <member>
  for _ in $(seq 1 400); do
    local m; m="$(field "$1" members 2>/dev/null || true)"
    case ",$m," in *",$2:"*) sleep 0.05;; *) return 0;; esac
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
  cli --host 127.0.0.1 --port "$(node_port "$leader")" fill "$1" --pipeline "$2" >/dev/null
  cli --host 127.0.0.1 --port "$(node_port "$leader")" verify "$1" | grep -q 'missing 0'
}

echo "== 1) 3 节点启动 + 压测 =="
for id in 1 2 3; do start_node "$id" "$PEERS3"; done
LEADER="$(require_leader)"
echo "leader=$LEADER"
fill_and_verify 200 8

echo "== 2) 第 4 个节点以 seed=3 启动（非投票），再 add =="
start_node 4 "$PEERS3"
sleep 1
cli --host 127.0.0.1 --port "$(node_port "$LEADER")" add 4 "127.0.0.1:$P4" | grep -q '^OK' || {
  echo "FAIL: add 4 failed" >&2; exit 1; }
wait_config_version_ge "$LEADER" 1
for id in 1 2 3 4; do wait_config_has "$id" 4 || { echo "FAIL: node$id 配置里没有 4" >&2; exit 1; }; done
echo "add 4 OK（config_version=$(field "$LEADER" config_version)）"

echo "== 3) 4 节点压测 + 线性一致读 =="
fill_and_verify 400 16
leader="$(require_leader)"
out=""
for _ in $(seq 1 40); do
  if out="$(cli --host 127.0.0.1 --port "$(node_port "$leader")" get f10 2>/dev/null)"; then break; fi
  sleep 0.25
done
[[ "$out" == "v" ]] || { echo "FAIL: 线性一致读 f10 期望 v 得到 '$out'" >&2; exit 1; }
echo "读 f10 = $out"

echo "== 4) remove 4 =="
cli --host 127.0.0.1 --port "$(node_port "$leader")" remove 4 | grep -q '^OK' || {
  echo "FAIL: remove 4 failed" >&2; exit 1; }
for id in 1 2 3; do wait_config_lacks "$id" 4 || { echo "FAIL: node$id 配置里仍有 4" >&2; exit 1; }; done
fill_and_verify 200 8
echo "remove 4 OK"

echo "== 5) 被移除节点退役 =="
r=""
for _ in $(seq 1 200); do
  r="$(field 4 retired 2>/dev/null || true)"
  [[ "$r" == "true" ]] && break
  sleep 0.05
done
[[ "$r" == "true" ]] || { echo "FAIL: node4 未退役 (retired='$r')" >&2; exit 1; }
echo "node4 retired=true"

echo "== 6) 客户端拓扑自动刷新（--peers 只给一个非 Leader 种子）=="
leader="$(require_leader)"
seed=""
for id in 1 2 3; do
  if [[ "$id" != "$leader" ]]; then seed="$id"; break; fi
done
[[ -n "$seed" ]] || { echo "FAIL: 找不到非 Leader 节点" >&2; exit 1; }
# 没有拓扑刷新时：follower 回 NOT_LEADER，leaderHint 在 --peers 里查不到 -> NOT_LEADER
out="$("$BIN/raftkv_raft_cli" --peers "$seed=127.0.0.1:$(node_port "$seed")"         --host 127.0.0.1 --port "$(node_port "$seed")" get f10)"
[[ "$out" == "v" ]] || { echo "FAIL: 客户端未按最新拓扑路由到 Leader（得到 '$out'）" >&2; exit 1; }
echo "seed=node$seed -> 线性一致读 f10 = $out"

echo "== 7) 初始节点连不上时自动换节点 =="
DEAD=$((BASE + 50))
out="$("$BIN/raftkv_raft_cli" --peers "$seed=127.0.0.1:$(node_port "$seed")"         --host 127.0.0.1 --port "$DEAD" get f10)"
[[ "$out" == "v" ]] || { echo "FAIL: 初始节点不可达时未能切换（得到 '$out'）" >&2; exit 1; }
echo "初始端口不可达 -> 自动换节点后 f10 = $out"

echo "raft_membership_e2e: PASS"
