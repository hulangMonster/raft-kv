// M5.1：指标实现（设计 §9）。
#include "raft/metrics.h"

#include <algorithm>
#include <cstdio>

#include "raft/lock_probe.h"

namespace raftkv::raft {

namespace {
// 分桶上界（us）：1,2,5,10,20,50,100,200,500,1000,+inf
constexpr uint64_t kBounds[Metrics::kBuckets] = {
    1,     2,     5,     10,    20,    50,     100,    200,
    500,   1000,  2000,  5000,  10000, 20000,  50000,  UINT64_MAX};
// 溢出桶（>=50ms）按 50000 上报（下界估计）；其余桶上报自身上界。
constexpr uint64_t kOverflowReportUs = 50000;
}  // namespace

void Metrics::onWriteCompleted(uint64_t latencyUs) {
  writes_.fetch_add(1, std::memory_order_relaxed);
  size_t b = 0;
  while (b + 1 < kBuckets && latencyUs > kBounds[b]) ++b;
  latencyBuckets_[b].fetch_add(1, std::memory_order_relaxed);

  // qps 窗口：>1s 没有新写就重新起窗（近似即可，指标不参与判定）
  const uint64_t now = lockprobe::nowUs();
  const uint64_t start = windowStartUs_.load(std::memory_order_relaxed);
  if (now - start >= 5000000ULL) {   // 5s 无写则重新起窗
    windowStartUs_.store(now, std::memory_order_relaxed);
    windowWrites_.store(0, std::memory_order_relaxed);
  }
  windowWrites_.fetch_add(1, std::memory_order_relaxed);
}

void Metrics::onFsync(uint64_t durationUs) {
  fsyncCalls_.fetch_add(1, std::memory_order_relaxed);
  fsyncUs_.fetch_add(durationUs, std::memory_order_relaxed);
}

void Metrics::onBatch(size_t entries) {
  batches_.fetch_add(1, std::memory_order_relaxed);
  batchEntries_.fetch_add(entries, std::memory_order_relaxed);
  uint64_t prev = batchMax_.load(std::memory_order_relaxed);
  while (entries > prev && !batchMax_.compare_exchange_weak(
                               prev, entries, std::memory_order_relaxed)) {
  }
}

void Metrics::onLockWait(uint64_t waitUs) {
  lockWaitUsTotal_.fetch_add(waitUs, std::memory_order_relaxed);
  uint64_t prev = lockWaitUsMax_.load(std::memory_order_relaxed);
  while (waitUs > prev && !lockWaitUsMax_.compare_exchange_weak(
                              prev, waitUs, std::memory_order_relaxed)) {
  }
}

void Metrics::onElection() {
  elections_.fetch_add(1, std::memory_order_relaxed);
}
void Metrics::onSnapshot(uint64_t bytes) {
  snapshots_.fetch_add(1, std::memory_order_relaxed);
  snapshotBytes_.fetch_add(bytes, std::memory_order_relaxed);
}
void Metrics::onConfigChange() {
  configChanges_.fetch_add(1, std::memory_order_relaxed);
}
void Metrics::setReplicationLag(Index lag) {
  replLag_.store(lag, std::memory_order_relaxed);
}
void Metrics::setInflightRpc(size_t n) {
  inflightRpc_.store(n, std::memory_order_relaxed);
}

uint64_t Metrics::fsyncCalls() const {
  return fsyncCalls_.load(std::memory_order_relaxed);
}
uint64_t Metrics::fsyncUs() const {
  return fsyncUs_.load(std::memory_order_relaxed);
}
uint64_t Metrics::writes() const {
  return writes_.load(std::memory_order_relaxed);
}
uint64_t Metrics::batches() const {
  return batches_.load(std::memory_order_relaxed);
}
uint64_t Metrics::batchEntries() const {
  return batchEntries_.load(std::memory_order_relaxed);
}
uint64_t Metrics::batchMax() const {
  return batchMax_.load(std::memory_order_relaxed);
}
uint64_t Metrics::elections() const {
  return elections_.load(std::memory_order_relaxed);
}
uint64_t Metrics::snapshots() const {
  return snapshots_.load(std::memory_order_relaxed);
}
uint64_t Metrics::snapshotBytes() const {
  return snapshotBytes_.load(std::memory_order_relaxed);
}
uint64_t Metrics::configChanges() const {
  return configChanges_.load(std::memory_order_relaxed);
}

uint64_t Metrics::lockWaitUsTotal() const {
  return lockWaitUsTotal_.load(std::memory_order_relaxed) +
         lockprobe::lockWaitUsTotal();
}
uint64_t Metrics::lockWaitUsMax() const {
  return std::max(lockWaitUsMax_.load(std::memory_order_relaxed),
                  lockprobe::lockWaitUsMax());
}

uint64_t Metrics::percentileUs(double p) const {
  uint64_t total = 0;
  for (size_t i = 0; i < kBuckets; ++i) {
    total += latencyBuckets_[i].load(std::memory_order_relaxed);
  }
  if (total == 0) return 0;
  const uint64_t target =
      static_cast<uint64_t>(p * static_cast<double>(total) + 0.5);
  uint64_t cum = 0;
  for (size_t i = 0; i < kBuckets; ++i) {
    cum += latencyBuckets_[i].load(std::memory_order_relaxed);
    if (cum >= target) {
      return kBounds[i] == UINT64_MAX ? kOverflowReportUs : kBounds[i];
    }
  }
  return 0;
}

uint64_t Metrics::latencyP50Us() const { return percentileUs(0.50); }
uint64_t Metrics::latencyP99Us() const { return percentileUs(0.99); }

uint64_t Metrics::qps() const {
  const uint64_t start = windowStartUs_.load(std::memory_order_relaxed);
  if (start == 0) return 0;
  const uint64_t now = lockprobe::nowUs();
  const uint64_t elapsed = now > start ? now - start : 0;
  if (elapsed > 5000000ULL) return 0;  // 窗口过期（>5s 无写）
  if (elapsed == 0) return 0;
  const uint64_t w = windowWrites_.load(std::memory_order_relaxed);
  return w * 1000000ULL / elapsed;
}

uint64_t Metrics::inflightNow() const {
  if (inflightProvider_) return inflightProvider_();
  return inflightRpc_.load(std::memory_order_relaxed);
}

std::string Metrics::statusFragment() const {
  const uint64_t batches = batches_.load(std::memory_order_relaxed);
  const uint64_t entries = batchEntries_.load(std::memory_order_relaxed);
  char buf[640];
  std::snprintf(
      buf, sizeof(buf),
      "qps=%llu lat_p50_us=%llu lat_p99_us=%llu fsync_calls=%llu fsync_ms=%llu "
      "batch_avg=%llu batch_max=%llu repl_lag_max=%llu elections_total=%llu "
      "snapshots_total=%llu snapshot_bytes=%llu config_changes=%llu "
      "lock_wait_us_total=%llu lock_wait_max=%llu inflight_rpc=%llu",
      (unsigned long long)qps(), (unsigned long long)latencyP50Us(),
      (unsigned long long)latencyP99Us(),
      (unsigned long long)fsyncCalls_.load(std::memory_order_relaxed),
      (unsigned long long)(fsyncUs_.load(std::memory_order_relaxed) / 1000ULL),
      (unsigned long long)(batches == 0 ? 0 : entries / batches),
      (unsigned long long)batchMax_.load(std::memory_order_relaxed),
      (unsigned long long)replLag_.load(std::memory_order_relaxed),
      (unsigned long long)elections_.load(std::memory_order_relaxed),
      (unsigned long long)snapshots_.load(std::memory_order_relaxed),
      (unsigned long long)snapshotBytes_.load(std::memory_order_relaxed),
      (unsigned long long)configChanges_.load(std::memory_order_relaxed),
      (unsigned long long)lockWaitUsTotal(),
      (unsigned long long)lockWaitUsMax(),
      (unsigned long long)inflightRpc_.load(std::memory_order_relaxed));
  return std::string(buf);
}

std::string Metrics::prometheusText() const {
  std::string out;
  out += "# TYPE raftkv_qps gauge\n";
  out += "raftkv_qps " + std::to_string(qps()) + "\n";
  out += "# TYPE raftkv_write_latency_us summary\n";
  out += "raftkv_write_latency_us{quantile=\"0.5\"} " +
         std::to_string(latencyP50Us()) + "\n";
  out += "raftkv_write_latency_us{quantile=\"0.99\"} " +
         std::to_string(latencyP99Us()) + "\n";
  out += "# TYPE raftkv_fsync_total counter\n";
  out += "raftkv_fsync_total " + std::to_string(fsyncCalls()) + "\n";
  out += "raftkv_fsync_ms_total " + std::to_string(fsyncUs() / 1000ULL) + "\n";
  out += "raftkv_writes_total " + std::to_string(writes()) + "\n";
  out += "raftkv_commit_batch_entries_avg " +
         std::to_string(batches() == 0 ? 0 : batchEntries() / batches()) + "\n";
  out += "raftkv_commit_batch_entries_max " + std::to_string(batchMax()) + "\n";
  out += "raftkv_replication_lag_max " +
         std::to_string(replLag_.load(std::memory_order_relaxed)) + "\n";
  out += "raftkv_elections_total " + std::to_string(elections()) + "\n";
  out += "raftkv_snapshots_total " + std::to_string(snapshots()) + "\n";
  out += "raftkv_snapshot_bytes_total " + std::to_string(snapshotBytes()) + "\n";
  out += "raftkv_config_changes_total " + std::to_string(configChanges()) + "\n";
  out += "raftkv_lock_wait_us_total " + std::to_string(lockWaitUsTotal()) + "\n";
  out += "raftkv_lock_wait_us_max " + std::to_string(lockWaitUsMax()) + "\n";
  out += "raftkv_inflight_rpc " +
         std::to_string(inflightRpc_.load(std::memory_order_relaxed)) + "\n";
  return out;
}

}  // namespace raftkv::raft
