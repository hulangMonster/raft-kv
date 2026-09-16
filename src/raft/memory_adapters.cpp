// In-memory adapters used by deterministic unit tests (no I/O, no flakiness).
#include "raft/transport.h"

#include <utility>

#include "raft/log_store.h"
#include "raft/raft_node.h"

namespace raftkv::raft {

// ---- MemoryLogStore ---------------------------------------------------------

bool MemoryLogStore::load(Term& term, int& votedFor, Index& lastIndex) {
  term = term_;
  votedFor = votedFor_;
  lastIndex = this->lastIndex();
  return true;
}

bool MemoryLogStore::persistMeta(Term term, int votedFor) {
  term_ = term;
  votedFor_ = votedFor;
  return true;
}

bool MemoryLogStore::append(const std::vector<LogEntry>& entries) {
  return appendNoSync(entries);
}

bool MemoryLogStore::appendNoSync(const std::vector<LogEntry>& entries) {
  for (const LogEntry& e : entries) {
    if (e.index <= lastIndex()) {
      // Same index + same term: already present (idempotent append).
      if (termAt(e.index) == e.term) continue;
      // Conflict: truncate this suffix, then append.
      if (!truncateSuffix(e.index)) return false;
    }
    if (e.index != lastIndex() + 1) return false;  // must be contiguous (D3)
    entries_.push_back(e);
  }
  return true;
}

bool MemoryLogStore::sync() { return true; }  // nothing to flush in memory

bool MemoryLogStore::truncateSuffix(Index fromIndex) {
  if (fromIndex == kNoIndex) return true;
  if (fromIndex <= lastIncluded_) return false;  // cannot cut below boundary
  if (fromIndex > lastIndex() + 1) return false;
  entries_.resize(static_cast<size_t>(fromIndex - firstIndex()));
  return true;
}

std::vector<LogEntry> MemoryLogStore::slice(Index from, size_t maxEntries,
                                            size_t maxBytes) const {
  std::vector<LogEntry> out;
  size_t bytes = 0;
  Index start = from;
  if (start < firstIndex()) start = firstIndex();  // clamp (D3)
  for (const LogEntry& e : entries_) {
    if (e.index < start) continue;
    if (out.size() >= maxEntries) break;
    const size_t sz = e.key.size() + e.value.size();
    if (bytes + sz > maxBytes && !out.empty()) break;
    out.push_back(e);
    bytes += sz;
  }
  return out;
}

Index MemoryLogStore::lastIndex() const {
  return entries_.empty() ? lastIncluded_
                          : static_cast<Index>(entries_.back().index);
}

Term MemoryLogStore::lastTerm() const {
  if (!entries_.empty()) return entries_.back().term;
  return (lastIncluded_ == kNoIndex) ? kNoTerm : lastIncludedTerm_;
}

Term MemoryLogStore::termAt(Index index) const {
  if (index == kNoIndex) return kNoTerm;
  if (index == lastIncluded_ && lastIncluded_ != kNoIndex) {
    return lastIncludedTerm_;  // boundary entry (D3)
  }
  if (index < firstIndex() || index > lastIndex()) return kNoTerm;
  return entries_[static_cast<size_t>(index - firstIndex())].term;
}

// ---- M3 compaction (D3) -----------------------------------------------------

void MemoryLogStore::setBoundary(Index lastIncludedIndex,
                                 Term lastIncludedTerm) {
  lastIncluded_ = lastIncludedIndex;
  lastIncludedTerm_ = lastIncludedTerm;
  size_t drop = 0;
  while (drop < entries_.size() && entries_[drop].index <= lastIncludedIndex) {
    ++drop;
  }
  if (drop > 0) {
    entries_.erase(entries_.begin(),
                   entries_.begin() + static_cast<ptrdiff_t>(drop));
  }
}

bool MemoryLogStore::compact(Index upTo, Term termAtUpTo) {
  if (upTo == kNoIndex) return true;
  if (upTo <= lastIncluded_) return true;  // already compacted: no-op
  // upTo may exceed lastIndex() (InstallSnapshot): setBoundary drops whatever
  // is at or below the boundary and records the new end of log.
  setBoundary(upTo, termAtUpTo);
  return true;
}

Index MemoryLogStore::firstIndex() const { return lastIncluded_ + 1; }
Index MemoryLogStore::lastIncludedIndex() const { return lastIncluded_; }
Term MemoryLogStore::lastIncludedTerm() const { return lastIncludedTerm_; }

// ---- MemoryTransport --------------------------------------------------------

void MemoryTransport::addNode(int id, RaftNode* node) {
  if (static_cast<size_t>(id) >= nodes_.size()) {
    nodes_.resize(static_cast<size_t>(id) + 1, nullptr);
    isolated_.resize(static_cast<size_t>(id) + 1, false);
  }
  nodes_[static_cast<size_t>(id)] = node;
}

void MemoryTransport::isolate(int id) {
  if (static_cast<size_t>(id) < isolated_.size()) {
    isolated_[static_cast<size_t>(id)] = true;
  }
}

void MemoryTransport::heal(int id) {
  if (static_cast<size_t>(id) < isolated_.size()) {
    isolated_[static_cast<size_t>(id)] = false;
  }
}

void MemoryTransport::sendRequestVote(int peerId, const RequestVoteArgs& args,
                                      VoteCb cb) {
  RaftNode* peer = (static_cast<size_t>(peerId) < nodes_.size())
                       ? nodes_[static_cast<size_t>(peerId)]
                       : nullptr;
  const bool dropped = (static_cast<size_t>(peerId) < isolated_.size()) &&
                       isolated_[static_cast<size_t>(peerId)];
  if (peer == nullptr || dropped) {
    // Dropped message: no callback (sender treats it as a timeout).
    return;
  }
  cb(peer->onRequestVote(args));
}

void MemoryTransport::sendAppendEntries(int peerId,
                                        const AppendEntriesArgs& args,
                                        AppendCb cb) {
  RaftNode* peer = (static_cast<size_t>(peerId) < nodes_.size())
                       ? nodes_[static_cast<size_t>(peerId)]
                       : nullptr;
  const bool dropped = (static_cast<size_t>(peerId) < isolated_.size()) &&
                       isolated_[static_cast<size_t>(peerId)];
  if (peer == nullptr || dropped) return;
  cb(peer->onAppendEntries(args));
}

void MemoryTransport::sendReadProbe(int peerId, const ReadProbeArgs& args,
                                    ReadProbeCb cb) {
  RaftNode* peer = (static_cast<size_t>(peerId) < nodes_.size())
                       ? nodes_[static_cast<size_t>(peerId)]
                       : nullptr;
  const bool dropped = (static_cast<size_t>(peerId) < isolated_.size()) &&
                       isolated_[static_cast<size_t>(peerId)];
  if (peer == nullptr || dropped) return;
  cb(peer->onReadProbe(args));
}

void MemoryTransport::addPeer(int id, const std::string& addr) {
  (void)addr;  // 内存传输按 id 直接投递，地址只对配置语义有意义
  if (static_cast<size_t>(id) >= nodes_.size()) {
    nodes_.resize(static_cast<size_t>(id) + 1, nullptr);
    isolated_.resize(static_cast<size_t>(id) + 1, false);
  }
}

void MemoryTransport::removePeer(int id) {
  if (static_cast<size_t>(id) < nodes_.size()) {
    nodes_[static_cast<size_t>(id)] = nullptr;
  }
}

void MemoryTransport::sendInstallSnapshot(int peerId,
                                          const InstallSnapshotArgs& args,
                                          InstallCb cb) {
  RaftNode* peer = (static_cast<size_t>(peerId) < nodes_.size())
                       ? nodes_[static_cast<size_t>(peerId)]
                       : nullptr;
  const bool dropped = (static_cast<size_t>(peerId) < isolated_.size()) &&
                       isolated_[static_cast<size_t>(peerId)];
  if (peer == nullptr || dropped) return;
  cb(peer->onInstallSnapshot(args));
}

}  // namespace raftkv::raft
