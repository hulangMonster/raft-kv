#!/usr/bin/env bash
# Benchmark: fsync-per-append (pipeline=1) vs group commit + client pipelining.
set -euo pipefail
cd "$(dirname "$0")/.."
W=$(mktemp -d)
B=$((19000 + ($$ % 600)))
PEERS="1=127.0.0.1:$B,2=127.0.0.1:$((B+1)),3=127.0.0.1:$((B+2))"
declare -a pids
for id in 1 2 3; do
  ./build/bin/raftkv_raft_node --id "$id" --port $((B+id-1)) --peers "$PEERS" \
    --data-dir "$W/n$id" --snapshot-threshold 5000 >"$W/n$id.log" 2>&1 &
  pids[$id]=$!
done
sleep 2
CLI="./build/bin/raftkv_raft_cli --peers $PEERS --host 127.0.0.1 --port $B"

run_bench() {
  local p=$1 n=$2 t0 t1 el
  t0=$(date +%s.%N)
  $CLI fill "$n" --pipeline "$p" >/dev/null
  t1=$(date +%s.%N)
  el=$(awk -v a="$t0" -v b="$t1" 'BEGIN{print b-a}')
  awk -v p="$p" -v n="$n" -v t="$el" \
    'BEGIN{printf "pipeline=%-3s n=%-6d elapsed=%6.1fs qps=%7.0f\n", p, n, t, n/t}'
  # QPS is only meaningful if every write really landed: concurrent workers must
  # not share a clientId (the dedup table would silently drop out-of-order ids).
  $CLI verify "$n"
}

run_bench 1 5000
run_bench 8 20000
run_bench 64 20000

for id in 1 2 3; do kill -9 "${pids[$id]}" 2>/dev/null || true; done
rm -rf "$W"
