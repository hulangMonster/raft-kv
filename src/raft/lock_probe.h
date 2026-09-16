#pragma once

// M5（I9 的断言基础设施）：把"当前线程是否持有 Raft 共识锁"暴露给测试。
//
// 动机：M5 验收口径要求"锁内无 fsync / 无网络 IO"必须是**可断言**的，而不是"看代码觉得没有"。
// 做法：RaftNode 的 mu_/membershipMu_/metaPersistMu_ 换成 ProbedMutex（lock/unlock/WAIT 各维护
// 一次 thread_local 深度计数），测试侧的 SpyLogStore / SpyTransport 在同步点读 held() 即可判定。
// 成本：每次加解锁两次 thread_local 读写（ns 级），不改变任何语义。
//
// 注意：只覆盖 RaftNode 自己的锁；FileLogStore::mu_ 等存储内部锁不在 I9 范围内。

#include <cstdint>
#include <mutex>

namespace raftkv::raft {
namespace lockprobe {

// 每线程的 Raft 锁持有深度（>0 = 该线程正处于某个 RaftNode 临界区内）。
inline thread_local uint32_t depth = 0;
// 单调计数：诊断用（断言"确实发生过持锁"，避免用例空转通过）。
inline thread_local uint64_t enters = 0;

inline bool held() { return depth > 0; }
inline uint32_t heldDepth() { return depth; }
inline uint64_t enterCount() { return enters; }

}  // namespace lockprobe

// 与 std::mutex 接口兼容（BasicLockable + try_lock）。条件变量等待期间是解锁状态，
// 因此 wait 期间 depth 归零——与真实持有语义一致。
class ProbedMutex {
 public:
  void lock() {
    mu_.lock();
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
