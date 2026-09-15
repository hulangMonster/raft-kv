#pragma once

#include <memory>
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

  // M3: consistent view (copies data_ + lastRequest_ + lastApplied_) and
  // wholesale restore. Both include the idempotency table.
  std::shared_ptr<const raft::SnapshotView> snapshotView() const override;
  bool restore(const Bytes& payload) override;

 private:
  std::unordered_map<std::string, std::string> data_;
  std::unordered_map<uint64_t, uint64_t> lastRequest_;  // clientId -> requestId
  raft::Index lastApplied_ = raft::kNoIndex;
};

}  // namespace raftkv
