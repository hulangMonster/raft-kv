#pragma once

#include <memory>
#include <string>

#include "types.h"

namespace raftkv::raft {

// M5.4（设计 §8.3）：快照载荷的**流式**产出。每次 next() 最多 maxChunk 字节，
// 返回 false 表示结束。允许对同一个 view 重复 stream() 得到可重放的新流——
// save() 需要两趟（第一趟算 payloadLen/crc32，第二趟写盘），单趟无法预知长度。
class SnapshotStream {
 public:
  virtual ~SnapshotStream() = default;
  virtual bool next(Bytes& out, size_t maxChunk) = 0;
};

// Immutable, consistent view of the state machine taken under RaftNode::mu_.
// serialize() must be called OUTSIDE the lock (lock discipline L8).
class SnapshotView {
 public:
  virtual ~SnapshotView() = default;
  virtual Bytes serialize() const = 0;
  // 默认实现：把 serialize() 的一整块当成单块输出（旧实现不改也能编译）。
  // KvSnapshotView 覆写为真正的分块流（峰值 O(块) 而非 O(状态)）。
  virtual std::unique_ptr<SnapshotStream> stream() const;
};

// 默认流：一次性吐出整块（内存峰值 O(状态)，只作为兼容回退）。
class OneShotSnapshotStream : public SnapshotStream {
 public:
  explicit OneShotSnapshotStream(Bytes bytes) : bytes_(std::move(bytes)) {}
  bool next(Bytes& out, size_t /*maxChunk*/) override {
    if (done_) return false;
    done_ = true;
    out = std::move(bytes_);
    return true;
  }

 private:
  Bytes bytes_;
  bool done_ = false;
};

inline std::unique_ptr<SnapshotStream> SnapshotView::stream() const {
  return std::make_unique<OneShotSnapshotStream>(serialize());
}

// KV state machine. Applied entries are committed log entries, in order.
// apply() MUST be idempotent per (clientId, requestId) — see m2-design.md §6.5.
class StateMachine {
 public:
  virtual ~StateMachine() = default;
  virtual void apply(const LogEntry& entry) = 0;
  virtual bool get(const std::string& key, std::string& out) const = 0;
  virtual Index lastApplied() const = 0;

  // M3 snapshot support (defaults = unsupported, so M2 implementations compile
  // unchanged). snapshotView() is called under the lock and must only copy
  // memory (no serialization / I/O); restore() is a pure in-memory swap.
  virtual std::shared_ptr<const SnapshotView> snapshotView() const {
    return nullptr;
  }
  virtual bool restore(const Bytes& payload) {
    (void)payload;
    return false;
  }
};

}  // namespace raftkv::raft
