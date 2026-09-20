#!/usr/bin/env bash
# M5 逐格基准（单格诊断口）：跑一个 (引擎, pipeline) 单元格，输出
#   墙钟 / ms/写 / QPS / verify 结果 / 每节点 status 指标 / leader CPU 与机器忙度。
#
# 与 scripts/bench_m5_ab.sh 的分工：
#   * bench_m5_ab.sh  = 官方验收：M4 vs M5 同轮交替、N 次中位数、输出比值与判定表；
#   * 本脚本          = 诊断用单格：关心"这一格内部发生了什么"（fsync 次数与耗时、批量、
#                       锁等待、节点 CPU、机器忙度），以及可选的 leader 侧 fsync 计数
#                       （strace）。docs/m5-bench.md §3.11 的每一行数字都由它复现。
#
# 用法：
#   scripts/bench_m5_cell.sh --eng m5 --pipeline 8 --n 2000
#   scripts/bench_m5_cell.sh --eng m5 --nodes 10 --pipeline 8 --n 500      # 节点规模（§3.12）
#   scripts/bench_m5_cell.sh --eng m5 --nodes 10 --pipeline 1 --n 200 -- --transport=reactor
#   scripts/bench_m5_cell.sh --eng m4 --pipeline 8 --n 2000 --strace-leader
#   scripts/bench_m5_cell.sh --eng m5 --pipeline 1 --n 500 --threshold 5000
#
# --nodes N：起 N 个节点回环集群（默认 3）。**所有节点都写进 seed 的 `--peers`**，即全部是
#   投票成员 —— 这正是 §3.12"节点规模"那一节的口径（不做成员变更，只比不同 N 的稳态）。
#   注意：单机跑 N 个节点会共享 CPU 与磁盘，N 大时绝对值只作趋势参考。
#
# 退出码：0 = 该格 verify missing 0；1 = 无 leader / 缺写；2 = 参数或环境错（可进 CI 门禁）。
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BASE_REF=dc6c56a                 # M4 基线（与 bench_m5_ab.sh 一致）
ENG=m5
NODES=3
PIPELINE=8
N=2000
THRESHOLD=5000
DATAROOT=""
M5BIN="$ROOT/build/bin"
M4BIN="/tmp/m5base-${BASE_REF}/build/bin"
STRACE=0
EXTRA=()

usage() { sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --eng)        ENG="$2"; shift 2 ;;
    --nodes)      NODES="$2"; shift 2 ;;
    --pipeline)   PIPELINE="$2"; shift 2 ;;
    --n)          N="$2"; shift 2 ;;
    --threshold)  THRESHOLD="$2"; shift 2 ;;
    --data-root)  DATAROOT="$2"; shift 2 ;;
    --m4-bin)     M4BIN="$2"; shift 2 ;;
    --m5-bin)     M5BIN="$2"; shift 2 ;;
    --strace-leader) STRACE=1; shift ;;
    -h|--help)    usage; exit 0 ;;
    --)           shift; EXTRA=("$@"); break ;;
    *)            echo "unknown arg: $1（-h 看用法）" >&2; exit 2 ;;
  esac
done

case "$ENG" in
  m5) BIN="$M5BIN" ;;
  m4) BIN="$M4BIN" ;;
  *)  echo "--eng 只接受 m5|m4（收到 $ENG）" >&2; exit 2 ;;
esac
if [[ ! -x "$BIN/raftkv_raft_node" ]]; then
  echo "找不到 $BIN/raftkv_raft_node" >&2
  [[ "$ENG" == m4 ]] && echo "  m4 基线 worktree 由 scripts/bench_m5_ab.sh 自动创建（/tmp/m5base-$BASE_REF），"                             "或用 --m4-bin 指向别处的构建" >&2
  exit 2
fi
if [[ "$NODES" -lt 1 || "$NODES" -gt 50 ]] 2>/dev/null; then
  echo "--nodes 需要 1..50 的整数（收到 $NODES）" >&2; exit 2
fi
[[ -z "$DATAROOT" ]] && DATAROOT="/tmp/raftkv-cell-$$"
command -v strace >/dev/null 2>&1 || { [[ $STRACE == 1 ]] && { echo "--strace-leader 需要 strace" >&2; exit 2; }; }

BASE=$(( 45000 + RANDOM % 500 ))   # N ≤ 50 时端口不越界（45000..45499 + N）
PEERS=""
for id in $(seq 1 "$NODES"); do
  PEERS="$PEERS$id=127.0.0.1:$((BASE+id-1))"
  [[ $id -lt $NODES ]] && PEERS="$PEERS,"
done
DIR="$DATAROOT/$ENG-n$NODES-p$PIPELINE"
rm -rf "$DIR"; mkdir -p "$DIR"
PIDS=()

cleanup() {
  for p in "${PIDS[@]:-}"; do [[ -n "$p" ]] && kill -9 "$p" 2>/dev/null; done
  return 0
}
trap cleanup EXIT

start_node() {  # <id>
  local id="$1"
  if [[ $id == 1 && $STRACE == 1 ]]; then
    strace -f -tt -e trace=fsync,fdatasync -o "$DIR/strace-leader.txt" \
      "$BIN/raftkv_raft_node" --id "$id" --port "$((BASE+id-1))" --peers "$PEERS" \
      --data-dir "$DIR/n$id" --snapshot-threshold "$THRESHOLD" ${EXTRA[@]+"${EXTRA[@]}"} \
      >"$DIR/n$id.log" 2>&1 &
  else
    "$BIN/raftkv_raft_node" --id "$id" --port "$((BASE+id-1))" --peers "$PEERS" \
      --data-dir "$DIR/n$id" --snapshot-threshold "$THRESHOLD" ${EXTRA[@]+"${EXTRA[@]}"} \
      >"$DIR/n$id.log" 2>&1 &
  fi
  PIDS+=("$!")
}

for id in $(seq 1 "$NODES"); do start_node "$id"; done

CLI="$BIN/raftkv_raft_cli --peers $PEERS --host 127.0.0.1"
LPORT=0
for _ in $(seq 1 400); do
  for id in $(seq 1 "$NODES"); do
    if $CLI --port $((BASE+id-1)) status 2>/dev/null | grep -q 'role=leader'; then
      LPORT=$((BASE+id-1)); break
    fi
  done
  [[ $LPORT -ne 0 ]] && break
  sleep 0.1
done
if [[ $LPORT -eq 0 ]]; then
  echo "CELL eng=$ENG p=$PIPELINE n=$N NO_LEADER（日志：$DIR/n*.log）" >&2
  exit 1
fi
LEADER_ID=$(( LPORT - BASE + 1 ))
sleep 1

$CLI --port "$LPORT" fill 200 --pipeline 1 >/dev/null 2>&1 || true   # 预热（丢弃）

cpu_jiffies() { awk '{print $14+$15}' "/proc/$1/stat" 2>/dev/null || echo 0; }
LEADER_PID="${PIDS[$((LEADER_ID-1))]}"
cpu_before=0
for p in "${PIDS[@]}"; do cpu_before=$(( cpu_before + $(cpu_jiffies "$p") )); done
lcpu_before=$(cpu_jiffies "$LEADER_PID")
stat_before=$(awk '/^cpu /{t=0; for(i=2;i<=NF;i++) t+=$i; printf "%d %d", t, $5+$6}' /proc/stat)

T0=$(date +%s.%N)
$CLI --port "$LPORT" fill "$N" --pipeline "$PIPELINE" >/dev/null
T1=$(date +%s.%N)

cpu_after=0
for p in "${PIDS[@]}"; do cpu_after=$(( cpu_after + $(cpu_jiffies "$p") )); done
lcpu_after=$(cpu_jiffies "$LEADER_PID")
stat_after=$(awk '/^cpu /{t=0; for(i=2;i<=NF;i++) t+=$i; printf "%d %d", t, $5+$6}' /proc/stat)

VERIFY=$($CLI --port "$LPORT" verify "$N" | tail -1)
MS=$(awk -v a="$T0" -v b="$T1" 'BEGIN{printf "%.1f", (b-a)*1000}')
QPS=$(awk -v n="$N" -v m="$MS" 'BEGIN{ if (m>0) printf "%.0f", n*1000/m; else print 0 }')
CPU_S=$(awk -v a="$cpu_before" -v b="$cpu_after" 'BEGIN{printf "%.2f", (b-a)/100}')
USPW=$(awk -v c="$CPU_S" -v n="$N" 'BEGIN{ if (n>0) printf "%.0f", c*1000000/n; else print 0 }')
BUSY=$(awk -v x="$stat_before" -v y="$stat_after" 'BEGIN{
  split(x,B," "); split(y,A," ");
  dt=A[1]-B[1]; di=A[2]-B[2];
  if (dt>0) printf "%.0f", 100*(dt-di)/dt; else print -1 }')
FIELDS='(role|term|commit_index|last_applied|snapshot_index|fsync_calls|fsync_ms|batch_avg|batch_max|repl_lag_max|elections_total|snapshots_total|lat_p50_us|lat_p99_us|lock_wait_us_total|lock_wait_max|inflight_rpc)=[0-9a-z]+'

echo "CELL eng=$ENG nodes=$NODES pipeline=$PIPELINE n=$N ms=$MS ms_per_write=$(awk -v m="$MS" -v n="$N" 'BEGIN{printf "%.3f", m/n}') qps=$QPS verify=[$VERIFY]"
for id in $(seq 1 "$NODES"); do
  tag=FOLLOWER; [[ $id -eq $LEADER_ID ]] && tag=LEADER
  echo "$tag id=$id $($CLI --port $((BASE+id-1)) status 2>/dev/null | tr '\n' ' ' | grep -oE "$FIELDS" | tr '\n' ' ')"
done
LCPU_S=$(awk -v a="$lcpu_before" -v b="$lcpu_after" 'BEGIN{printf "%.2f", (b-a)/100}')
echo "RESOURCE leader_cpu_s=$LCPU_S all_nodes_cpu_s=$CPU_S us_per_write_allnodes=$USPW machine_busy_pct=$BUSY"

if [[ $STRACE == 1 ]]; then
  ALL=$(wc -l < "$DIR/strace-leader.txt" 2>/dev/null || echo 0)
  echo "STRACE leader_fsync_lines=$ALL（含 meta/dir 的 fsync；详见 $DIR/strace-leader.txt）"
fi

if [[ "$VERIFY" == *"missing 0"* ]]; then
  echo "CELL_OK（硬门禁：verify missing 0）"
  exit 0
fi
echo "CELL_FAILED：$VERIFY（日志：$DIR/n*.log）" >&2
exit 1
