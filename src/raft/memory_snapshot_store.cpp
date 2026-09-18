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
  encoded_ = encodeSnapshotFile(data_);  // M5.4：供分块发送/切片读取
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
  encoded_ = encodeSnapshotFile(data_);
  return true;
}

// ---- M5.4（§8.3/§8.4）：内存适配器的流式/切片接口（语义与 File 版一致，便于对拍） ----
bool MemorySnapshotStore::saveStreaming(const SnapshotView& view,
                                        Index lastIncludedIndex,
                                        Term lastIncludedTerm,
                                        const Bytes& config) {
  SnapshotData data;
  data.lastIncludedIndex = lastIncludedIndex;
  data.lastIncludedTerm = lastIncludedTerm;
  data.config = config;
  std::unique_ptr<SnapshotStream> st = view.stream();
  Bytes buf;
  while (st->next(buf, 1u << 20)) {
    data.payload.insert(data.payload.end(), buf.begin(), buf.end());
  }
  return save(data);
}

uint64_t MemorySnapshotStore::installedBytes() const {
  std::lock_guard<std::mutex> lock(mu_);
  return encoded_.size();
}

bool MemorySnapshotStore::readInstalled(uint64_t offset, uint64_t len,
                                        Bytes& out) const {
  std::lock_guard<std::mutex> lock(mu_);
  if (len == 0 || offset + len > encoded_.size()) return false;
  out.assign(encoded_.begin() + static_cast<ptrdiff_t>(offset),
             encoded_.begin() + static_cast<ptrdiff_t>(offset + len));
  return true;
}

uint64_t MemorySnapshotStore::recvProgress() const {
  std::lock_guard<std::mutex> lock(mu_);
  return recvEnd_;
}

}  // namespace raftkv::raft
