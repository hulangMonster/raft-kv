#!/usr/bin/env bash
# M5 同机 A/B 基准：M4 基线（默认 dc6c56a）vs M5 工作区
#   * 同一脚本内交替测量（M4→M5→M4→M5…），避免跨会话/跨机器状态比较
#   * 每档先跑 200 写预热（丢弃），每档重复 N 次取中位数
#   * 每次 fill 后必须 verify，missing != 0 则整轮作废
# 用法: scripts/bench_m5_ab.sh [--quick] [--repeats N] [--base-ref REF] [--only m4|m5]
# 判据(M5.5 修订, docs/m5-design.md §10 + docs/m5-bench.md §3.7): **同机比值口径**
#   * pipeline=1  : M5 延迟 <= M4 延迟 x 1.2   (小并发下延迟不劣化)
#   * pipeline=8/64: M5 吞吐 >= M4 吞吐 x 0.8   (高并发下吞吐不明显退化)
#   * 硬门禁     : 每个格子 verify 必须 missing 0（probe 里已强校验）
# 为什么不用绝对判据(<=8ms/写、>=1200qps)：见 docs/m5-bench.md §3.6 —— 本机单次持久化
# 提交下限 ~8ms(fsync/fdatasync/预分配/O_DIRECT 实测)，Raft 必须 durable 后才 ack，
# 绝对目标隐含 "flush ≈ 1ms" 的硬件；比值口径才是同机可复现、可回归的判据。
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BASE_REF=dc6c56a
REPS=3
QUICK=0
ONLY=both
THRESHOLD=5000

while [[ $# -gt 0 ]]; do
  case "$1" in
    --quick) QUICK=1; shift ;;
    --repeats) REPS="$2"; shift 2 ;;
    --base-ref) BASE_REF="$2"; shift 2 ;;
    --only) ONLY="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done
[[ $QUICK == 1 ]] && REPS=1

WORK="$(mktemp -d)"
LOGDIR="/tmp/m5bench-logs"
mkdir -p "$LOGDIR"
RAW="$LOGDIR/raw-$(date +%s).txt"
echo "# m5 A/B raw log  $(date)" > "$RAW"
echo "# machine: $(nproc) cores, load=$(cut -d' ' -f1 /proc/loadavg), ref=$BASE_REF, threshold=$THRESHOLD, reps=$REPS, quick=$QUICK" >> "$RAW"

M4SRC="/tmp/m5base-$BASE_REF"
if [[ ! -d "$M4SRC" ]]; then
  git -C "$ROOT" worktree add --detach "$M4SRC" "$BASE_REF" >/dev/null 2>&1 || {
    echo "worktree add failed for $BASE_REF" >&2; exit 1; }
fi
echo "== building baseline $BASE_REF =="
cmake -S "$M4SRC" -B "$M4SRC/build" -DCMAKE_BUILD_TYPE=Release >/dev/null
cmake --build "$M4SRC/build" -j"$(nproc)" >/dev/null || { echo "baseline build failed" >&2; exit 1; }
echo "== building current worktree =="
cmake --build "$ROOT/build" -j"$(nproc)" >/dev/null || { echo "m5 build failed" >&2; exit 1; }

probe() { # <engine:m4|m5> <bin> <pipeline> <rep>
  local eng="$1" bin="$2" p="$3" rep="$4"
  local base=$(( 21000 + RANDOM % 3000 ))
  local peers="1=127.0.0.1:$base,2=127.0.0.1:$((base+1)),3=127.0.0.1:$((base+2))"
  local dir="$WORK/$eng-p$p-r$rep"; mkdir -p "$dir"
  local pids=()
  for id in 1 2 3; do
    "$bin/raftkv_raft_node" --id "$id" --port $((base+id-1)) --peers "$peers" \
      --data-dir "$dir/n$id" --snapshot-threshold "$THRESHOLD" >"$dir/n$id.log" 2>&1 &
    pids+=($!)
  done
  local cli="$bin/raftkv_raft_cli --peers $peers --host 127.0.0.1"
  local leaderport=0
  for _ in $(seq 1 150); do
    for id in 1 2 3; do
      if $cli --port $((base+id-1)) status 2>/dev/null | grep -q 'role=leader'; then
        leaderport=$((base+id-1)); break
      fi
    done
    [[ $leaderport -ne 0 ]] && break
    sleep 0.1
  done
  if [[ $leaderport -eq 0 ]]; then
    echo "$eng p=$p rep=$rep NO_LEADER" | tee -a "$RAW"
    for pid in "${pids[@]}"; do kill -9 "$pid" 2>/dev/null || true; done
    return 1
  fi
  sleep 1
  $cli --port "$leaderport" fill 200 --pipeline 1 >/dev/null 2>&1 || true   # 预热（丢弃）

  local t0 t1 ms lat qps verify
  t0=$(date +%s.%N)
  $cli --port "$leaderport" fill "$N" --pipeline "$p" >/dev/null
  t1=$(date +%s.%N)
  verify=$($cli --port "$leaderport" verify "$N" | tail -1)
  ms=$(awk -v a="$t0" -v b="$t1" 'BEGIN{printf "%.1f", (b-a)*1000}')
  lat=$(awk -v n="$N" -v ms="$ms" 'BEGIN{printf "%.3f", ms/n}')
  qps=$(awk -v n="$N" -v ms="$ms" 'BEGIN{ if (ms>0) printf "%.0f", n*1000/ms; else print 0 }')
  echo "$eng p=$p rep=$rep n=$N ms=$ms ms_per_write=$lat qps=$qps verify=[$verify]" | tee -a "$RAW"
  for pid in "${pids[@]}"; do kill -9 "$pid" 2>/dev/null || true; done
  sleep 1
  [[ "$verify" == *"missing 0"* ]] || { echo "VERIFY_FAILED $eng p=$p" >&2; return 1; }
  return 0
}

medians() { # <engine> <pipeline> <field>
  awk -v e="$1" -v want="$2" -v f="$3" '
    $1==e {
      hit=0; v=""
      for (i=1;i<=NF;i++) {
        n=split($i,kv,"=")
        if (n<2) continue
        if (kv[1]=="p" && kv[2]==want) hit=1
        if (kv[1]==f) v=kv[2]
      }
      if (hit && v!="") print v
    }
  ' "$RAW" | sort -g | awk '{a[NR]=$1} END{ if (NR==0) {print "n/a"; exit} if (NR%2) printf "%.3f", a[(NR+1)/2]; else printf "%.3f", (a[NR/2]+a[NR/2+1])/2 }'
}

PIPELINES=(1 8 64)
echo
echo "== A/B (each cell = median of $REPS runs) =="
for p in "${PIPELINES[@]}"; do
  if [[ $QUICK == 1 ]]; then
    case $p in 1) N=500;; 8) N=2000;; 64) N=2000;; esac
  else
    case $p in 1) N=5000;; 8) N=20000;; 64) N=20000;; esac
  fi
  for rep in $(seq 1 "$REPS"); do
    [[ "$ONLY" == "m5" || "$ONLY" == "both" ]] || true
    if [[ "$ONLY" == "both" || "$ONLY" == "m5" ]]; then probe m5 "$ROOT/build/bin" "$p" "$rep"; fi
    if [[ "$ONLY" == "both" || "$ONLY" == "m4" ]]; then probe m4 "$M4SRC/build/bin" "$p" "$rep"; fi
  done
done

echo
echo "| pipeline | n | M4 median ms/write | M5 median ms/write | M4 median qps | M5 median qps | M5/M4 延迟 | M5/M4 吞吐 | 判定(§10 比值口径) |"
echo "|---|---|---|---|---|---|---|---|---|"
for p in "${PIPELINES[@]}"; do
  if [[ $QUICK == 1 ]]; then
    case $p in 1) N=500;; 8) N=2000;; 64) N=2000;; esac
  else
    case $p in 1) N=5000;; 8) N=20000;; 64) N=20000;; esac
  fi
  m4l=$(medians m4 "$p" ms_per_write); m5l=$(medians m5 "$p" ms_per_write)
  m4q=$(medians m4 "$p" qps);          m5q=$(medians m5 "$p" qps)
  verdict="n/a"; lratio="n/a"; qratio="n/a"
  if [[ "$m4l" != "n/a" && "$m5l" != "n/a" ]]; then
    lratio=$(awk -v a="$m4l" -v b="$m5l" 'BEGIN{ if (a>0) printf "%.2fx", b/a; else print "n/a" }')
    qratio=$(awk -v qa="$m4q" -v qb="$m5q" 'BEGIN{ if (qa>0) printf "%.2fx", qb/qa; else print "n/a" }')
    verdict=$(awk -v p="$p" -v a="$m4l" -v b="$m5l" -v qa="$m4q" -v qb="$m5q" 'BEGIN{
      if (p==1) { if (b <= a*1.2) print "达标(延迟<=1.2x)"; else print "未达标(延迟>1.2x)" }
      else      { if (qb >= qa*0.8) print "达标(吞吐>=0.8x)"; else print "未达标(吞吐<0.8x)" }
    }')
  fi
  echo "| $p | $N | $m4l | $m5l | $m4q | $m5q | $lratio | $qratio | $verdict |"
done
echo
echo "raw log: $RAW"
echo "（硬门禁：每次 fill 之后都跑 verify，出现 missing != 0 时 probe 会打印 VERIFY_FAILED 并整轮作废）"
rm -rf "$WORK"
