#pragma once

#include <string>
#include <unordered_map>

#include "raft/state_machine.h"
#include "raft/types.h"

namespace raftkv {

// Pure in-memory KV state machine (decision D1': M1's Store is NOT modified;
// M2 uses this StateMachine whose durability comes from the Raft log).
// apply() is idempotent per (clientId, requestId).
class KvStateMachine : public raft::StateMachine {
 public:
  void apply(const raft::LogEntry& entry) override;
  bool get(const std::string& key, std::string& out) const override;
  raft::Index lastApplied() const override;

 private:
  std::unordered_map<std::string, std::string> data_;
  std::unordered_map<uint64_t, uint64_t> lastRequest_;  // clientId -> requestId
  raft::Index lastApplied_ = raft::kNoIndex;
};

}  // namespace raftkv
