// KvStateMachine: pure in-memory KV + idempotency (decision D1').
// Durability comes from the Raft log, never from this class itself.
#include "kv/kv_state_machine.h"

namespace raftkv {

void KvStateMachine::apply(const raft::LogEntry& e) {
  // No-op marker (leader election): advance applied index, no data change.
  // GET never enters the log for real client reads, so op==kGet == no-op.
  if (e.op == OpCode::kGet) {
    lastApplied_ = e.index;
    return;
  }

  // Idempotency per (clientId, requestId): replay / duplicate is dropped.
  const auto it = lastRequest_.find(e.clientId);
  if (it != lastRequest_.end() && e.requestId <= it->second) {
    return;
  }

  if (e.op == OpCode::kPut) {
    data_[e.key] = e.value;
  } else if (e.op == OpCode::kDel) {
    data_.erase(e.key);
  }

  lastRequest_[e.clientId] = e.requestId;
  lastApplied_ = e.index;
}

bool KvStateMachine::get(const std::string& key, std::string& out) const {
  const auto it = data_.find(key);
  if (it == data_.end()) return false;
  out = it->second;
  return true;
}

raft::Index KvStateMachine::lastApplied() const { return lastApplied_; }

}  // namespace raftkv
