#pragma once

// M5（I9 的断言基础设施）：把"当前线程是否持有 Raft 的**共识锁**"暴露给测试。
//
// 动机：M5 验收口径要求"锁内无 fsync / 无网络 IO"必须是**可断言**的，而不是"看代码觉得没有"。
// I9 的"锁内"专指 `RaftNode::mu_`（保护全部共识状态的那把锁）；`membershipMu_`（成员变更串行化）
// 与 `metaPersistMu_`（meta 落盘串行化）是**叶子锁**，在它们内部做 IO 是设计允许的，不算违反 I9。
// 因此这里按"锁类别"分别计数。
//
// 用法：测试侧的 SpyLogStore / SpyTransport 在同步点读 consensusHeld() 即可判定。
// 成本：每次加解锁两次 thread_local 读写（ns 级），不改变任何语义。
//
// 注意：FileLogStore::mu_ 等存储内部锁不在 I9 范围内。

#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>

namespace raftkv::raft {

enum class LockClass : uint8_t {
  kConsensus = 0,   // RaftNode::mu_
  kMembership = 1,  // RaftNode::membershipMu_
  kMeta = 2,        // RaftNode::metaPersistMu_
  kCount = 3,
};

namespace lockprobe {

// 每线程、每类别的持有深度。
inline thread_local uint32_t depth[static_cast<size_t>(LockClass::kCount)] = {};
inline thread_local uint64_t enters[static_cast<size_t>(LockClass::kCount)] = {};

inline bool held(LockClass c) {
  return depth[static_cast<size_t>(c)] > 0;
}
// I9 的判定口径：是否持有共识锁 mu_。
inline bool consensusHeld() { return held(LockClass::kConsensus); }
inline bool anyHeld() {
  for (size_t i = 0; i < static_cast<size_t>(LockClass::kCount); ++i) {
    if (depth[i] > 0) return true;
  }
  return false;
}
inline uint32_t heldDepth(LockClass c) { return depth[static_cast<size_t>(c)]; }
inline uint64_t enterCount(LockClass c) { return enters[static_cast<size_t>(c)]; }

// 锁等待统计（全局；仅在 timing_enabled 时更新）。
inline std::atomic<bool> timing_enabled{false};
inline std::atomic<uint64_t> lockWaitUsTotalAtomic{0};
inline std::atomic<uint64_t> lockWaitUsMaxAtomic{0};

inline uint64_t nowUs() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

inline uint64_t lockWaitUsTotal() {
  return lockWaitUsTotalAtomic.load(std::memory_order_relaxed);
}
inline uint64_t lockWaitUsMax() {
  return lockWaitUsMaxAtomic.load(std::memory_order_relaxed);
}
inline void setTimingEnabled(bool on) {
  timing_enabled.store(on, std::memory_order_relaxed);
}
inline void resetLockWait() {
  lockWaitUsTotalAtomic.store(0, std::memory_order_relaxed);
  lockWaitUsMaxAtomic.store(0, std::memory_order_relaxed);
}
inline void recordLockWait(uint64_t us) {
  lockWaitUsTotalAtomic.fetch_add(us, std::memory_order_relaxed);
  uint64_t prev = lockWaitUsMaxAtomic.load(std::memory_order_relaxed);
  while (us > prev && !lockWaitUsMaxAtomic.compare_exchange_weak(
                          prev, us, std::memory_order_relaxed)) {
  }
}

}  // namespace lockprobe

// 与 std::mutex 接口兼容（BasicLockable + try_lock）。条件变量等待期间是解锁状态，
// 因此 wait 期间深度归零——与真实持有语义一致。
template <LockClass C = LockClass::kConsensus>
class ProbedMutexT {
 public:
  void lock() {
    if (lockprobe::timing_enabled.load(std::memory_order_relaxed)) {
      const uint64_t t0 = lockprobe::nowUs();
      mu_.lock();
      lockprobe::recordLockWait(lockprobe::nowUs() - t0);
    } else {
      mu_.lock();
    }
    ++lockprobe::depth[static_cast<size_t>(C)];
    ++lockprobe::enters[static_cast<size_t>(C)];
  }
  void unlock() {
    --lockprobe::depth[static_cast<size_t>(C)];
    mu_.unlock();
  }
  bool try_lock() {
    if (!mu_.try_lock()) return false;
    ++lockprobe::depth[static_cast<size_t>(C)];
    ++lockprobe::enters[static_cast<size_t>(C)];
    return true;
  }

 private:
  std::mutex mu_;
};

using ProbedMutex = ProbedMutexT<LockClass::kConsensus>;       // RaftNode::mu_
using MembershipMutex = ProbedMutexT<LockClass::kMembership>;  // 成员变更串行化
using MetaMutex = ProbedMutexT<LockClass::kMeta>;              // meta 落盘串行化

}  // namespace raftkv::raft
