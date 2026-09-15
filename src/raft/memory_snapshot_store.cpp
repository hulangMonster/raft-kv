// In-memory SnapshotStore adapter used by deterministic unit tests (no I/O).
#include "raft/snapshot_store.h"

#include <mutex>
#include <utility>

namespace raftkv::raft {

bool MemorySnapshotStore::load(SnapshotData& out) {
  std::lock_guard<std::mutex> lock(mu_);
  if (!has_) return false;
  out = data_;
  return true;
}

bool MemorySnapshotStore::save(const SnapshotData& data) {
  std::lock_guard<std::mutex> lock(mu_);
  // B5: never move the durable boundary backwards (a save racing a newer
  // InstallSnapshot must lose).
  if (has_ && data.lastIncludedIndex <= data_.lastIncludedIndex) return false;
  data_ = data;
  has_ = true;
  recv_.clear();
  recvIndex_ = kNoIndex;
  recvTerm_ = kNoTerm;
  recvEnd_ = 0;
  return true;
}

bool MemorySnapshotStore::receiveChunk(Index lastIncludedIndex,
                                       Term lastIncludedTerm, uint64_t offset,
                                       const Bytes& data, bool done) {
  std::lock_guard<std::mutex> lock(mu_);

  // A new (index, term) - or a restart at offset 0 - begins a fresh transfer.
  if (lastIncludedIndex != recvIndex_ || lastIncludedTerm != recvTerm_ ||
      offset == 0) {
    recvIndex_ = lastIncludedIndex;
    recvTerm_ = lastIncludedTerm;
    recvEnd_ = 0;
    recv_.clear();
    if (offset != 0) return false;  // ask the leader to restart at 0
  }
  // B7: a retransmitted chunk is an idempotent success, never a second append.
  if (offset + data.size() <= recvEnd_) return true;
  if (offset != recvEnd_) {  // gap / overlap: restart the transfer
    recvIndex_ = kNoIndex;
    recvTerm_ = kNoTerm;
    recvEnd_ = 0;
    recv_.clear();
    return false;
  }

  recv_.insert(recv_.end(), data.begin(), data.end());
  recvEnd_ += data.size();
  if (!done) return true;

  // M3.3: the wire payload is the raw snapshot file; validate+decode it.
  SnapshotData parsed;
  const bool ok = decodeSnapshotFile(recv_.data(), recv_.size(), parsed) &&
                  parsed.lastIncludedIndex == lastIncludedIndex &&
                  parsed.lastIncludedTerm == lastIncludedTerm;
  recvIndex_ = kNoIndex;
  recvTerm_ = kNoTerm;
  recvEnd_ = 0;
  recv_.clear();
  if (!ok) return false;
  if (has_ && parsed.lastIncludedIndex <= data_.lastIncludedIndex) {
    return true;  // already have something newer
  }
  data_ = std::move(parsed);
  has_ = true;
  return true;
}

}  // namespace raftkv::raft
