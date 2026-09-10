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
  for (const LogEntry& e : entries) {
    if (e.index <= lastIndex()) {
      // Same index + same term: already present (idempotent append).
      if (termAt(e.index) == e.term) continue;
      // Conflict: truncate this suffix, then append.
      if (!truncateSuffix(e.index)) return false;
    }
    entries_.push_back(e);
  }
  return true;
}

bool MemoryLogStore::truncateSuffix(Index fromIndex) {
  if (fromIndex == kNoIndex) return true;
  if (fromIndex > lastIndex() + 1) return false;
  entries_.resize(static_cast<size_t>(fromIndex - 1));
  return true;
}

std::vector<LogEntry> MemoryLogStore::slice(Index from, size_t maxEntries,
                                            size_t maxBytes) const {
  std::vector<LogEntry> out;
  size_t bytes = 0;
  for (const LogEntry& e : entries_) {
    if (e.index < from) continue;
    if (out.size() >= maxEntries) break;
    const size_t sz = e.key.size() + e.value.size();
    if (bytes + sz > maxBytes && !out.empty()) break;
    out.push_back(e);
    bytes += sz;
  }
  return out;
}

Index MemoryLogStore::lastIndex() const {
  return static_cast<Index>(entries_.size());
}

Term MemoryLogStore::lastTerm() const {
  return entries_.empty() ? kNoTerm : entries_.back().term;
}

Term MemoryLogStore::termAt(Index index) const {
  if (index == kNoIndex || index > lastIndex()) return kNoTerm;
  return entries_[static_cast<size_t>(index - 1)].term;
}

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

}  // namespace raftkv::raft
