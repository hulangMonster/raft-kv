#pragma once

// M5.1：进程内指标（设计 §9 / 决策⑥）。
//
// 约束（I13）：指标**只读、无副作用、不参与任何正确性判定**。
// 成本目标：单次采样 < 1%（因此全是 atomic 的 O(1) 累加，采样/渲染只在 status 请求时做）。
//
// #2 TDD 阶段：本文件只提供接口 + 计数桩（实现留到 M5.1），使用例先 RED。

#include <atomic>
#include <cstdint>
#include <cstddef>
#include <string>

#include "types.h"

namespace raftkv::raft {

class Metrics {
 public:
  // ---- 写路径 ----
  void onWriteCompleted(uint64_t latencyUs);   // 客户端写完成（提交后）
  void onFsync(uint64_t durationUs);           // 一次 fsync（含耗时）
  void onBatch(size_t entries);                // 一次组提交批（条目数）
  void onLockWait(uint64_t waitUs);            // 一次 mu_ 获取等待

  // ---- 事件计数 ----
  void onElection();                           // 进入 Candidate
  void onSnapshot(uint64_t bytes);             // 完成一次快照
  void onConfigChange();                       // 一次成员变更提交

  // ---- 瞬时量（采样时写）----
  void setReplicationLag(Index lag);           // leader 视角最大 lag
  void setInflightRpc(size_t n);               // Reactor 在途请求数（M5.3）

  // ---- 渲染 ----
  // status 一行内的片段，形如 "qps=0 lat_p50_us=0 lat_p99_us=0 fsync_calls=0 ..."（只含新增字段）。
  std::string statusFragment() const;
  // 可选 /metrics 文本（Prometheus 风格）；为空表示不支持。
  std::string prometheusText() const;

  // ---- 原始计数（用例断言用）----
  uint64_t fsyncCalls() const;
  uint64_t fsyncUs() const;
  uint64_t writes() const;
  uint64_t batches() const;
  uint64_t batchEntries() const;
  uint64_t elections() const;
  uint64_t snapshots() const;
  uint64_t snapshotBytes() const;
  uint64_t configChanges() const;
  uint64_t lockWaitUsTotal() const;
  uint64_t lockWaitUsMax() const;
  uint64_t latencyP50Us() const;
  uint64_t latencyP99Us() const;
  uint64_t qps() const;

 private:
  // 延迟分桶上界（微秒）：1,2,5,10,20,50,100,200,500,1000, +inf
  static constexpr size_t kBuckets = 11;
  std::atomic<uint64_t> fsyncCalls_{0};
  std::atomic<uint64_t> fsyncUs_{0};
  std::atomic<uint64_t> writes_{0};
  std::atomic<uint64_t> batches_{0};
  std::atomic<uint64_t> batchEntries_{0};
  std::atomic<uint64_t> elections_{0};
  std::atomic<uint64_t> snapshots_{0};
  std::atomic<uint64_t> snapshotBytes_{0};
  std::atomic<uint64_t> configChanges_{0};
  std::atomic<uint64_t> lockWaitUsTotal_{0};
  std::atomic<uint64_t> lockWaitUsMax_{0};
  std::atomic<uint64_t> latencyBuckets_[kBuckets] = {};
};

}  // namespace raftkv::raft
