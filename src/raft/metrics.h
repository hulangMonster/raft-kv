#pragma once

// M5.1：进程内指标（设计 §9 / 决策⑥）。
//
// 约束（I13）：指标**只读、无副作用、不参与任何正确性判定**。
// 成本：全是 atomic 的 O(1) 累加；分位/速率只在 status（或 /metrics）请求时计算。
// 锁等待时长由 lock_probe 的全局计数提供（计时默认关闭，见 lockprobe::timing_enabled），
// 因此"关掉锁等待计时"时零额外开销 —— M5.5 的"指标开销 < 1%"用开关对照验证。

#include <atomic>
#include <functional>
#include <cstddef>
#include <cstdint>
#include <string>

#include "types.h"

namespace raftkv::raft {

class Metrics {
 public:
  // 分桶上界(us)：1,2,5,10,20,50,100,200,500,1000,2000,5000,10000,20000,50000,>=50ms
  // 上报值是所在桶的上界；最后一个桶（>=50ms）按 50000 上报（下界估计）。
  static constexpr size_t kBuckets = 16;

  // ---- 写路径 ----
  void onWriteCompleted(uint64_t latencyUs);  // 客户端写提交完成（端到端延迟）
  void onFsync(uint64_t durationUs);          // 一次 fsync（含耗时）
  void onBatch(size_t entries);               // 一次组提交批（条目数）
  void onLockWait(uint64_t waitUs);           // 显式上报的锁等待（测试/离线用）

  // ---- 事件计数 ----
  void onElection();
  void onSnapshot(uint64_t bytes);
  void onConfigChange();

  // ---- 瞬时量（采样时写）----
  void setReplicationLag(Index lag);
  void setInflightRpc(size_t n);
  // M5.1：注入在途 RPC 数提供者（Reactor::inflight）；渲染时读取，避免锁内成本
  void setInflightProvider(std::function<size_t()> fn) {
    inflightProvider_ = std::move(fn);
  }
  uint64_t inflightNow() const;

  // ---- 渲染 ----
  std::string statusFragment() const;   // status 一行内的新增字段（k=v）
  std::string prometheusText() const;   // 可选 /metrics（msgType 15）

  // ---- 原始计数（用例断言用）----
  uint64_t fsyncCalls() const;
  uint64_t fsyncUs() const;
  uint64_t writes() const;
  uint64_t batches() const;
  uint64_t batchEntries() const;
  uint64_t batchMax() const;
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
  uint64_t percentileUs(double p) const;

  std::atomic<uint64_t> fsyncCalls_{0};
  std::atomic<uint64_t> fsyncUs_{0};
  std::atomic<uint64_t> writes_{0};
  std::atomic<uint64_t> batches_{0};
  std::atomic<uint64_t> batchEntries_{0};
  std::atomic<uint64_t> batchMax_{0};
  std::atomic<uint64_t> elections_{0};
  std::atomic<uint64_t> snapshots_{0};
  std::atomic<uint64_t> snapshotBytes_{0};
  std::atomic<uint64_t> configChanges_{0};
  std::atomic<uint64_t> lockWaitUsTotal_{0};
  std::atomic<uint64_t> lockWaitUsMax_{0};
  std::atomic<uint64_t> inflightRpc_{0};
  std::function<size_t()> inflightProvider_;
  std::atomic<uint64_t> replLag_{0};
  // qps 窗口
  std::atomic<uint64_t> windowStartUs_{0};
  std::atomic<uint64_t> windowWrites_{0};
  std::atomic<uint64_t> latencyBuckets_[kBuckets] = {};
};

}  // namespace raftkv::raft
