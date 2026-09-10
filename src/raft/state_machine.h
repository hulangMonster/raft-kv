#pragma once

#include <string>

#include "types.h"

namespace raftkv::raft {

// KV state machine. Applied entries are committed log entries, in order.
// apply() MUST be idempotent per (clientId, requestId) — see m2-design.md §6.5.
class StateMachine {
 public:
  virtual ~StateMachine() = default;
  virtual void apply(const LogEntry& entry) = 0;
  virtual bool get(const std::string& key, std::string& out) const = 0;
  virtual Index lastApplied() const = 0;
};

}  // namespace raftkv::raft
