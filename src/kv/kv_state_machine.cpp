// KvStateMachine: pure in-memory KV + idempotency (decision D1').
// Durability comes from the Raft log, never from this class itself.
//
// M3 payload format (internal to this class; the snapshot file adds its own
// header around it):
//   [lastApplied:8]
//   [kvCount:4]    then kvCount    x [klen:4][vlen:4][key][value]
//   [dedupCount:4] then dedupCount x [clientId:8][requestId:8]
#include "kv/kv_state_machine.h"

#include <utility>
#include <vector>

namespace raftkv {

namespace {

void putU64(Bytes& out, uint64_t v) {
  for (int i = 7; i >= 0; --i) {
    out.push_back(static_cast<Byte>((v >> (i * 8)) & 0xff));
  }
}
uint64_t getU64(const Byte* p) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) v = (v << 8) | p[i];
  return v;
}

// Immutable copy handed to the out-of-lock serializer (lock discipline L8).
class KvSnapshotView : public raft::SnapshotView {
 public:
  KvSnapshotView(std::vector<std::pair<std::string, std::string>> kv,
                 std::vector<std::pair<uint64_t, uint64_t>> dedup,
                 raft::Index lastApplied)
      : kv_(std::move(kv)),
        dedup_(std::move(dedup)),
        lastApplied_(lastApplied) {}

  Bytes serialize() const override {
    Bytes out;
    putU64(out, lastApplied_);
    putU32(out, static_cast<uint32_t>(kv_.size()));
    for (const auto& kv : kv_) {
      putU32(out, static_cast<uint32_t>(kv.first.size()));
      putU32(out, static_cast<uint32_t>(kv.second.size()));
      out.insert(out.end(), kv.first.begin(), kv.first.end());
      out.insert(out.end(), kv.second.begin(), kv.second.end());
    }
    putU32(out, static_cast<uint32_t>(dedup_.size()));
    for (const auto& d : dedup_) {
      putU64(out, d.first);
      putU64(out, d.second);
    }
    return out;
  }

 private:
  std::vector<std::pair<std::string, std::string>> kv_;
  std::vector<std::pair<uint64_t, uint64_t>> dedup_;
  raft::Index lastApplied_;
};

}  // namespace

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

std::shared_ptr<const raft::SnapshotView> KvStateMachine::snapshotView() const {
  std::vector<std::pair<std::string, std::string>> kv(data_.begin(),
                                                      data_.end());
  std::vector<std::pair<uint64_t, uint64_t>> dedup(lastRequest_.begin(),
                                                   lastRequest_.end());
  return std::make_shared<KvSnapshotView>(std::move(kv), std::move(dedup),
                                          lastApplied_);
}

bool KvStateMachine::restore(const Bytes& payload) {
  if (payload.size() < 8) return false;
  const raft::Index lastApplied = getU64(payload.data());
  size_t off = 8;

  if (off + 4 > payload.size()) return false;
  const uint32_t kvCount = getU32(payload.data() + off);
  off += 4;
  // Never trust a count from the wire: each entry needs at least 8 bytes, so a
  // bogus count must not turn into a huge reserve().
  if (kvCount > (payload.size() - off) / 8) return false;

  std::unordered_map<std::string, std::string> data;
  data.reserve(kvCount);
  for (uint32_t i = 0; i < kvCount; ++i) {
    if (off + 8 > payload.size()) return false;
    const uint32_t klen = getU32(payload.data() + off);
    const uint32_t vlen = getU32(payload.data() + off + 4);
    off += 8;
    if (off + klen + vlen > payload.size()) return false;
    std::string k(reinterpret_cast<const char*>(payload.data() + off), klen);
    off += klen;
    std::string v(reinterpret_cast<const char*>(payload.data() + off), vlen);
    off += vlen;
    data[std::move(k)] = std::move(v);
  }

  if (off + 4 > payload.size()) return false;
  const uint32_t dedupCount = getU32(payload.data() + off);
  off += 4;
  if (dedupCount > (payload.size() - off) / 16) return false;

  std::unordered_map<uint64_t, uint64_t> dedup;
  dedup.reserve(dedupCount);
  for (uint32_t i = 0; i < dedupCount; ++i) {
    if (off + 16 > payload.size()) return false;
    const uint64_t clientId = getU64(payload.data() + off);
    const uint64_t requestId = getU64(payload.data() + off + 8);
    off += 16;
    dedup[clientId] = requestId;
  }
  if (off != payload.size()) return false;  // trailing garbage

  data_ = std::move(data);
  lastRequest_ = std::move(dedup);
  lastApplied_ = lastApplied;
  return true;
}

}  // namespace raftkv
