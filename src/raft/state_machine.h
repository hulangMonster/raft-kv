#pragma once

#include <memory>
#include <string>

#include "types.h"

namespace raftkv::raft {

// Immutable, consistent view of the state machine taken under RaftNode::mu_.
// serialize() must be called OUTSIDE the lock (lock discipline L8).
class SnapshotView {
 public:
  virtual ~SnapshotView() = default;
  virtual Bytes serialize() const = 0;
};

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
