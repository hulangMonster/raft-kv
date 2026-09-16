#pragma once

// M5（I9 的断言基础设施）：把"当前线程是否持有 Raft 共识锁"暴露给测试。
//
// 动机：M5 验收口径要求"锁内无 fsync / 无网络 IO"必须是**可断言**的，而不是"看代码觉得没有"。
// 做法：RaftNode 的 mu_/membershipMu_/metaPersistMu_ 换成 ProbedMutex（lock/unlock 维护一次
// thread_local 深度计数），测试侧的 SpyLogStore / SpyTransport 在同步点读 held() 即可判定。
//
// 锁等待计时（lock_wait_us_total/max）默认关闭：开启后每次加锁多两次 clock 读取，
// 只在显式打开（--lock-wait-metrics）时才有成本 —— 见 Metrics::statusFragment()。
//
// 注意：只覆盖 RaftNode 自己的锁；FileLogStore::mu_ 等存储内部锁不在 I9 范围内。

#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>

namespace raftkv::raft {
namespace lockprobe {

// 每线程的 Raft 锁持有深度（>0 = 该线程正处于某个 RaftNode 临界区内）。
inline thread_local uint32_t depth = 0;
// 单调计数：诊断用（断言"确实发生过持锁"，避免用例空转通过）。
inline thread_local uint64_t enters = 0;

// 锁等待统计（全局；仅在 timing_enabled 时更新）。
inline std::atomic<bool> timing_enabled{false};
inline std::atomic<uint64_t> lockWaitUsTotalAtomic{0};
inline std::atomic<uint64_t> lockWaitUsMaxAtomic{0};

inline bool held() { return depth > 0; }
inline uint32_t heldDepth() { return depth; }
inline uint64_t enterCount() { return enters; }

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
// 因此 wait 期间 depth 归零——与真实持有语义一致。
class ProbedMutex {
 public:
  void lock() {
    if (lockprobe::timing_enabled.load(std::memory_order_relaxed)) {
      const uint64_t t0 = lockprobe::nowUs();
      mu_.lock();
      lockprobe::recordLockWait(lockprobe::nowUs() - t0);
    } else {
      mu_.lock();
    }
    ++lockprobe::depth;
    ++lockprobe::enters;
  }
  void unlock() {
    --lockprobe::depth;
    mu_.unlock();
  }
  bool try_lock() {
    if (!mu_.try_lock()) return false;
    ++lockprobe::depth;
    ++lockprobe::enters;
    return true;
  }

 private:
  std::mutex mu_;
};

}  // namespace raftkv::raft
