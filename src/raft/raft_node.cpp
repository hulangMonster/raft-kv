// RaftNode core: election (M2.1/M2.2) + replication/commit (M2.3) +
// thread-safety via outbound-job discipline (M2.4).
//
// Locking: see raft_node.h. Public methods that send RPCs (tick/propose) build
// jobs under mu_, release it, then send. Inline callbacks (MemoryTransport /
// blocking TcpTransport) therefore re-enter safely because the sender no
// longer holds its own lock.
#include "raft/raft_node.h"

#include <algorithm>
#include <chrono>
#include <limits>
#include <stdexcept>
#include <utility>

#include "raft/clock.h"
#include "raft/log_store.h"
#include "raft/snapshot_store.h"
#include "raft/state_machine.h"
#include "raft/transport.h"

namespace raftkv::raft {

namespace {
uint64_t electionTimeoutMs(const RaftConfig& cfg, int selfId, Term term) {
  const uint64_t span = cfg.electionTimeoutMaxMs - cfg.electionTimeoutMinMs + 1;
  // Deterministic per (node, term) so unit tests stay flake-free, while the
  // spread keeps peers from timing out in lockstep (split votes).
  const uint64_t offset =
      (static_cast<uint64_t>(selfId) * 61u + term * 37u) % span;
  return cfg.electionTimeoutMinMs + offset;
}
}  // namespace

RaftNode::RaftNode(RaftConfig cfg, LogStore& log, StateMachine& sm,
                   Transport& transport, Clock& clock,
                   SnapshotStore* snapshots)
    : cfg_(std::move(cfg)),
      log_(log),
      sm_(sm),
      transport_(transport),
      clock_(clock),
      snapshots_(snapshots) {
  // A zero chunk size would make InstallSnapshot emit empty chunks forever and
  // a zero threshold would snapshot on every tick: reject the config up front.
  if (snapshots_ != nullptr &&
      (cfg_.snapshotChunkBytes == 0 || cfg_.snapshotThresholdEntries == 0)) {
    throw std::invalid_argument(
        "raft: snapshotChunkBytes and snapshotThresholdEntries must be > 0");
  }

  // M3.4 startup recovery (m3-design.md v1.1 §5.3; order matters):
  //   1) load snapshot  2) restore state machine  3) set the log boundary
  //   4) load the log tail  5) lastApplied = commitIndex = boundary
  if (snapshots_ != nullptr) {
    SnapshotData snap;
    if (snapshots_->load(snap)) {
      // A snapshot that decodes but cannot be restored is NOT the "torn
      // snapshot" case: compaction has already dropped the prefix it covers,
      // so falling through to a full replay would replay a log that no longer
      // starts at index 1 (and wipe it, see LogStore::load). Refuse to start.
      if (!sm_.restore(snap.payload)) {
        throw std::runtime_error(
            "raft: durable snapshot could not be restored (payload/version "
            "mismatch)");
      }
      lastIncluded_ = snap.lastIncludedIndex;
      lastIncludedTerm_ = snap.lastIncludedTerm;
      lastApplied_ = snap.lastIncludedIndex;
      commitIndex_ = snap.lastIncludedIndex;
      // D3: the boundary must be known before load() validates firstIndex().
      log_.setBoundary(snap.lastIncludedIndex, snap.lastIncludedTerm);
      snapshotBytes_ = encodeSnapshotFile(snap);
    }
    // Absent/corrupt (magic/version/CRC) snapshot falls through to a full log
    // replay (D2 guarantee: compaction only runs after a durable snapshot).
  }

  // Restore durable term/votedFor on restart (log entries live in log_ itself).
  {
    Term t = kNoTerm;
    int v = -1;
    Index last = kNoIndex;
    // A failed load leaves term/votedFor/log unverified, so the node must not
    // participate in elections with a possibly torn log.
    if (!log_.load(t, v, last)) {
      throw std::runtime_error("raft: log load failed (I/O error or torn log)");
    }
    currentTerm_ = t;
    votedFor_ = v;
  }
  lastHeartbeatMs_ = clock_.nowMs();
  electionTimeoutMs_ = electionTimeoutMs(cfg_, cfg_.selfId, currentTerm_);
  syncedIndex_ = log_.lastIndex();  // everything recovered from disk is durable
}

// ---- internal helpers (caller MUST hold mu_) --------------------------------

void RaftNode::becomeFollower(Term newTerm) {
  role_ = Role::kFollower;
  if (newTerm > currentTerm_) {
    currentTerm_ = newTerm;
    votedFor_ = -1;
    log_.persistMeta(currentTerm_, votedFor_);
  }
  leaderId_ = -1;
  votesGranted_ = 0;
  lastHeartbeatMs_ = clock_.nowMs();
  electionTimeoutMs_ = electionTimeoutMs(cfg_, cfg_.selfId, currentTerm_);

  nextIndex_.clear();
  matchIndex_.clear();
  lastSentEndIndex_.clear();
  snapshotSendOffset_.clear();
  snapshotChunkEnd_.clear();
  cv_.notify_all();
}

void RaftNode::becomeLeader() {
  role_ = Role::kLeader;
  leaderId_ = cfg_.selfId;
  lastHeartbeatSentMs_ = 0;  // send an immediate heartbeat on the next tick

  const Index next = log_.lastIndex() + 1;
  for (const int peer : cfg_.peerIds) {
    nextIndex_[peer] = next;
    matchIndex_[peer] = kNoIndex;
    lastSentEndIndex_[peer] = kNoIndex;
    snapshotSendOffset_[peer] = 0;
    snapshotChunkEnd_[peer] = 0;
  }
  matchIndex_[cfg_.selfId] = log_.lastIndex();
  cv_.notify_all();
}

void RaftNode::startElection(
    uint64_t now, std::vector<std::pair<int, RequestVoteArgs>>& voteJobs) {
  role_ = Role::kCandidate;
  ++currentTerm_;
  votedFor_ = cfg_.selfId;
  leaderId_ = -1;
  votesGranted_ = 0;
  log_.persistMeta(currentTerm_, votedFor_);  // I5: durable before requesting votes
  lastHeartbeatMs_ = now;
  electionTimeoutMs_ = electionTimeoutMs(cfg_, cfg_.selfId, currentTerm_);

  if (cfg_.peerIds.empty()) {  // majority of 1 == self (single-node path)
    becomeLeader();
    return;
  }

  for (const int peer : cfg_.peerIds) {
    RequestVoteArgs args;
    args.term = currentTerm_;
    args.candidateId = cfg_.selfId;
    args.lastLogIndex = log_.lastIndex();
    args.lastLogTerm = log_.lastTerm();
    voteJobs.emplace_back(peer, args);
  }
}

AppendEntriesArgs RaftNode::buildAppendEntries(int peer) {
  Index next = nextIndex_[peer];
  if (next == kNoIndex) next = log_.lastIndex() + 1;
  if (next < 1) next = 1;

  const Index prevLogIndex = next - 1;
  const Term prevLogTerm = log_.termAt(prevLogIndex);
  auto entries =
      log_.slice(next, cfg_.maxEntriesPerAppend, cfg_.maxBytesPerAppend);
  // Group commit (M5): never replicate entries that are not locally durable —
  // commit accounting may only count durable replicas.
  while (!entries.empty() && entries.back().index > syncedIndex_) {
    entries.pop_back();
  }
  const Index endIndex = entries.empty() ? prevLogIndex : entries.back().index;
  lastSentEndIndex_[peer] = endIndex;

  AppendEntriesArgs args;
  args.term = currentTerm_;
  args.leaderId = cfg_.selfId;
  args.prevLogIndex = prevLogIndex;
  args.prevLogTerm = prevLogTerm;
  args.entries = std::move(entries);
  args.leaderCommit = commitIndex_;
  return args;
}

void RaftNode::advanceCommitAndApply() {
  if (role_ == Role::kLeader) {
    const size_t majority = (cfg_.peerIds.size() + 1) / 2 + 1;
    for (Index n = log_.lastIndex(); n > commitIndex_; --n) {
      if (log_.termAt(n) != currentTerm_) continue;  // §5.4.2: only current term
      // Self counts only once the entry is locally durable (group commit).
      size_t replicated = (n <= syncedIndex_) ? 1 : 0;
      for (const int peer : cfg_.peerIds) {
        if (matchIndex_[peer] >= n) ++replicated;
      }
      if (replicated >= majority) {
        commitIndex_ = n;
        break;
      }
    }
  }

  // Apply committed entries in order.
  while (lastApplied_ < commitIndex_) {
    const Index next = lastApplied_ + 1;
    auto entries = log_.slice(next, 1, std::numeric_limits<size_t>::max());
    if (entries.empty() || entries[0].index != next) break;
    sm_.apply(entries[0]);
    lastApplied_ = next;
  }
  cv_.notify_all();
}

// ---- public API -------------------------------------------------------------

void RaftNode::tick() {
  maybeSnapshot();  // M3: two-phase snapshot; takes mu_ internally

  std::vector<std::pair<int, RequestVoteArgs>> voteJobs;
  std::vector<std::pair<int, AppendEntriesArgs>> appendJobs;
  std::vector<std::pair<int, InstallSnapshotArgs>> snapJobs;
  {
    std::lock_guard<std::mutex> lock(mu_);
    const uint64_t now = clock_.nowMs();

    if (role_ == Role::kLeader) {
      // Raft §8: a leader must commit an entry from its own term before it
      // may advance the commit point over prior-term entries (and serve reads).
      // Append a no-op GET entry (never produced by a client) for that.
      if (cfg_.appendNoop && log_.lastTerm() != currentTerm_) {
        LogEntry noop;
        noop.index = log_.lastIndex() + 1;
        noop.term = currentTerm_;
        noop.op = OpCode::kGet;
        noop.clientId = 0;
        noop.requestId = noop.index;  // unique; not a client request
        if (log_.append({noop})) {
          matchIndex_[cfg_.selfId] = noop.index;
          if (noop.index > syncedIndex_) syncedIndex_ = noop.index;
          advanceCommitAndApply();  // single-node cluster commits immediately
        }
      }
      if (now - lastHeartbeatSentMs_ >= cfg_.heartbeatMs) {
        lastHeartbeatSentMs_ = now;
        for (const int peer : cfg_.peerIds) {
          // M3.3: a peer behind the snapshot boundary gets one snapshot chunk
          // per tick (so the ticker is never blocked for long).
          if (lastIncluded_ != kNoIndex && !snapshotBytes_.empty() &&
              nextIndex_[peer] <= lastIncluded_) {
            const uint64_t total = snapshotBytes_.size();
            uint64_t start = snapshotSendOffset_[peer];
            if (start >= total) start = 0;
            const uint64_t remaining = total - start;
            const uint64_t take =
                std::min<uint64_t>(cfg_.snapshotChunkBytes, remaining);
            InstallSnapshotArgs a;
            a.term = currentTerm_;
            a.leaderId = cfg_.selfId;
            a.lastIncludedIndex = lastIncluded_;
            a.lastIncludedTerm = lastIncludedTerm_;
            a.offset = start;
            a.done = (start + take >= total);
            a.data.assign(snapshotBytes_.begin() + start,
                          snapshotBytes_.begin() + start + take);
            snapshotChunkEnd_[peer] = start + take;
            snapJobs.emplace_back(peer, std::move(a));
          } else {
            appendJobs.emplace_back(peer, buildAppendEntries(peer));
          }
        }
      }
    } else if (now - lastHeartbeatMs_ >= electionTimeoutMs_) {
      startElection(now, voteJobs);
    }
  }

  for (auto& job : voteJobs) {
    transport_.sendRequestVote(
        job.first, job.second,
        [this, peer = job.first](const RequestVoteReply& reply) {
          onRequestVoteReply(peer, reply);
        });
  }
  for (auto& job : appendJobs) {
    transport_.sendAppendEntries(
        job.first, job.second,
        [this, peer = job.first](const AppendEntriesReply& reply) {
          onAppendEntriesReply(peer, reply);
        });
  }
  for (auto& job : snapJobs) {
    transport_.sendInstallSnapshot(
        job.first, job.second,
        [this, peer = job.first](const InstallSnapshotReply& reply) {
          onInstallSnapshotReply(peer, reply);
        });
  }
}

RequestVoteReply RaftNode::onRequestVote(const RequestVoteArgs& args) {
  std::lock_guard<std::mutex> lock(mu_);
  if (args.term < currentTerm_) {
    return {currentTerm_, false};
  }
  if (args.term > currentTerm_) {
    becomeFollower(args.term);
  }

  const Term lastLogTerm = log_.lastTerm();
  const Index lastLogIndex = log_.lastIndex();
  const bool upToDate =
      (args.lastLogTerm > lastLogTerm) ||
      (args.lastLogTerm == lastLogTerm && args.lastLogIndex >= lastLogIndex);

  if ((votedFor_ == -1 || votedFor_ == args.candidateId) && upToDate) {
    votedFor_ = args.candidateId;
    log_.persistMeta(currentTerm_, votedFor_);  // I5: durable before granting
    lastHeartbeatMs_ = clock_.nowMs();          // granting a vote resets timer
    return {currentTerm_, true};
  }
  return {currentTerm_, false};
}

AppendEntriesReply RaftNode::onAppendEntries(const AppendEntriesArgs& args) {
  std::lock_guard<std::mutex> lock(mu_);
  if (args.term < currentTerm_) {
    return {currentTerm_, false, kNoIndex, kNoTerm};
  }
  if (args.term > currentTerm_) {
    becomeFollower(args.term);
  } else if (role_ == Role::kCandidate) {
    role_ = Role::kFollower;  // a valid leader for this term exists
  }

  leaderId_ = args.leaderId;
  lastHeartbeatMs_ = clock_.nowMs();  // heartbeat resets the election timer

  // m3-design.md §5.5: an entry strictly BELOW our compacted boundary is gone,
  // so prevLogTerm cannot be verified here. Point the leader at our first index
  // instead of pretending the logs match. (prevLogIndex == lastIncluded_ is
  // fine: termAt() answers with the boundary term.)
  if (args.prevLogIndex > kNoIndex &&
      args.prevLogIndex < log_.lastIncludedIndex()) {
    return {currentTerm_, false, log_.firstIndex(), log_.lastIncludedTerm()};
  }
  if (args.prevLogIndex > log_.lastIndex()) {
    return {currentTerm_, false, log_.lastIndex() + 1, kNoTerm};
  }
  if (args.prevLogIndex > kNoIndex &&
      log_.termAt(args.prevLogIndex) != args.prevLogTerm) {
    const Term conflictTerm = log_.termAt(args.prevLogIndex);
    Index conflictIndex = args.prevLogIndex;
    while (conflictIndex > 1 &&
           log_.termAt(conflictIndex - 1) == conflictTerm) {
      --conflictIndex;
    }
    return {currentTerm_, false, conflictIndex, conflictTerm};
  }

  std::vector<LogEntry> toAppend;
  for (const LogEntry& e : args.entries) {
    if (e.index <= log_.lastIndex()) {
      if (log_.termAt(e.index) == e.term) continue;  // already present
      if (!log_.truncateSuffix(e.index)) {
        return {currentTerm_, false, kNoIndex, kNoTerm};
      }
      if (syncedIndex_ > log_.lastIndex()) syncedIndex_ = log_.lastIndex();
    }
    toAppend.push_back(e);
  }
  if (!toAppend.empty()) {
    // One fsync for the whole batch (follower-side group commit too).
    if (!log_.append(toAppend)) {
      return {currentTerm_, false, kNoIndex, kNoTerm};
    }
    if (toAppend.back().index > syncedIndex_) {
      syncedIndex_ = toAppend.back().index;
    }
  }

  // The last entry this leader has actually shown us is the most we may ever
  // consider committed: anything beyond it is our own (unverified) suffix.
  const Index lastMatched =
      args.entries.empty() ? args.prevLogIndex : args.entries.back().index;
  if (lastMatched > syncedIndex_) {
    // Entries that were already present were not necessarily durable (they may
    // have been written by appendNoSync before we stepped down), so never ack
    // them as durable until they hit the disk.
    if (!log_.sync()) return {currentTerm_, false, kNoIndex, kNoTerm};
    syncedIndex_ = log_.lastIndex();
  }

  if (args.leaderCommit > commitIndex_) {
    // commitIndex = min(leaderCommit, index of last new entry), never backwards.
    const Index newCommit = std::min(args.leaderCommit, lastMatched);
    if (newCommit > commitIndex_) {
      commitIndex_ = newCommit;
      advanceCommitAndApply();
    }
  }
  return {currentTerm_, true, kNoIndex, kNoTerm};
}

void RaftNode::onRequestVoteReply(int /*peerId*/, const RequestVoteReply& reply) {
  std::lock_guard<std::mutex> lock(mu_);
  if (reply.term > currentTerm_) {
    becomeFollower(reply.term);
    return;
  }
  if (role_ != Role::kCandidate || reply.term != currentTerm_ ||
      !reply.voteGranted) {
    return;
  }

  ++votesGranted_;
  const size_t totalNodes = cfg_.peerIds.size() + 1;
  const size_t majority = totalNodes / 2 + 1;
  if (static_cast<size_t>(votesGranted_) + 1 >= majority) {  // +1 for self vote
    becomeLeader();
  }
}

void RaftNode::onAppendEntriesReply(int peerId, const AppendEntriesReply& reply) {
  std::lock_guard<std::mutex> lock(mu_);
  if (role_ != Role::kLeader) return;
  if (reply.term > currentTerm_) {
    becomeFollower(reply.term);
    return;
  }
  if (reply.term < currentTerm_) return;  // stale ack

  if (reply.success) {
    const Index end = lastSentEndIndex_[peerId];
    if (end > matchIndex_[peerId]) matchIndex_[peerId] = end;
    if (end != kNoIndex && end + 1 > nextIndex_[peerId]) {
      nextIndex_[peerId] = end + 1;
    }
    advanceCommitAndApply();
  } else {
    // Fast backup (M2.5): follow the follower's conflict hint, with two
    // snapshot-aware adjustments:
    //   * never aim below our own boundary — a peer that fell behind it can only
    //     be caught up by InstallSnapshot, which tick() starts as soon as
    //     nextIndex_ <= lastIncluded_;
    //   * the hint may also be AHEAD of what we thought (the peer reports where
    //     its own compacted log starts), so jump there instead of crawling one
    //     index at a time.
    Index target = reply.conflictIndex;
    if (target == kNoIndex || target < 1) target = 1;
    if (lastIncluded_ != kNoIndex && target < lastIncluded_) {
      target = lastIncluded_;
    }
    if (target != nextIndex_[peerId]) {
      // Accept the hint in either direction. A hint above our own log end means
      // the peer's compacted prefix is ahead of us: we simply cannot serve it
      // until our log grows past it, and crawling down one index per heartbeat
      // would only cycle between InstallSnapshot and AppendEntries.
      nextIndex_[peerId] = target;
    } else if (nextIndex_[peerId] > (lastIncluded_ != kNoIndex ? lastIncluded_ : 1)) {
      --nextIndex_[peerId];  // safety fallback (should not normally happen)
    }
  }
}

ClientReply RaftNode::propose(const ClientRequest& req, uint64_t timeoutMs) {
  LogEntry e;
  const bool isGet = (req.op == OpCode::kGet);

  {
    std::lock_guard<std::mutex> lock(mu_);
    if (role_ != Role::kLeader) {
      return {ClientStatus::kNotLeader, "", leaderId_};
    }
    if (isGet) {
      std::string out;
      if (sm_.get(req.key, out)) return {ClientStatus::kOk, out, -1};
      return {ClientStatus::kNotFound, "", -1};
    }

    e.index = log_.lastIndex() + 1;
    e.term = currentTerm_;
    e.op = req.op;
    e.key = req.key;
    e.value = req.value;
    e.clientId = req.clientId;
    e.requestId = req.requestId;
    // Group commit (M5): write immediately, fsync once per flush batch.
    if (!log_.appendNoSync({e})) {
      return {ClientStatus::kErr, "log append failed", -1};
    }
  }

  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(timeoutMs);
  for (;;) {
    bool iAmFlusher = false;
    Index flushTarget = kNoIndex;
    {
      std::lock_guard<std::mutex> lock(mu_);
      if (role_ != Role::kLeader || currentTerm_ != e.term) {
        return {ClientStatus::kNotLeader, "", leaderId_};
      }
      if (e.index <= commitIndex_) return {ClientStatus::kOk, "", -1};
      if (!syncInFlight_ && syncedIndex_ < e.index) {
        syncInFlight_ = true;
        flushTarget = log_.lastIndex();  // captured under the lock
        iAmFlusher = true;
      }
    }

    if (iAmFlusher) {
      const bool syncOk = log_.sync();  // ONE fsync for every entry so far
      std::vector<std::pair<int, AppendEntriesArgs>> jobs;
      {
        std::lock_guard<std::mutex> lock(mu_);
        if (syncOk) {
          // Only claim durability for entries that are still in the log: a
          // concurrent truncateSuffix()/compact() may have shrunk it meanwhile
          // (both leave the retained prefix durable, so clamping is safe).
          Index durable = flushTarget;
          if (durable > log_.lastIndex()) durable = log_.lastIndex();
          if (durable > syncedIndex_) syncedIndex_ = durable;
        }
        // A failed fsync must never be reported as durable (I5); leave
        // syncInFlight_ clear so another proposer retries.
        syncInFlight_ = false;
        if (syncOk && role_ == Role::kLeader && currentTerm_ == e.term) {
          // Single-node clusters commit here (no peer jobs below).
          advanceCommitAndApply();
          for (const int peer : cfg_.peerIds) {
            // Peers behind the snapshot boundary are served by tick()'s
            // InstallSnapshot branch: never send AppendEntries into the
            // compacted region (§5.5).
            if (lastIncluded_ != kNoIndex && nextIndex_[peer] <= lastIncluded_) {
              continue;
            }
            jobs.emplace_back(peer, buildAppendEntries(peer));
          }
        }
        cv_.notify_all();
      }
      if (!syncOk) {
        return {ClientStatus::kErr, "log sync failed", -1};
      }
      for (auto& job : jobs) {
        transport_.sendAppendEntries(
            job.first, job.second,
            [this, peer = job.first](const AppendEntriesReply& reply) {
              onAppendEntriesReply(peer, reply);
            });
      }
      continue;
    }

    std::unique_lock<std::mutex> lock(mu_);
    cv_.wait_until(lock, deadline, [&] {
      return e.index <= commitIndex_ || role_ != Role::kLeader ||
             currentTerm_ != e.term;
    });
    if (role_ != Role::kLeader || currentTerm_ != e.term) {
      return {ClientStatus::kNotLeader, "", leaderId_};
    }
    if (e.index <= commitIndex_) return {ClientStatus::kOk, "", -1};
    if (std::chrono::steady_clock::now() >= deadline) {
      return {ClientStatus::kErr, "timeout waiting for commit", -1};
    }
    // Otherwise loop: if our entry is still unsynced we may become the flusher.
  }
}

Role RaftNode::role() const {
  std::lock_guard<std::mutex> lock(mu_);
  return role_;
}
Term RaftNode::currentTerm() const {
  std::lock_guard<std::mutex> lock(mu_);
  return currentTerm_;
}
int RaftNode::leaderId() const {
  std::lock_guard<std::mutex> lock(mu_);
  return leaderId_;
}
Index RaftNode::commitIndex() const {
  std::lock_guard<std::mutex> lock(mu_);
  return commitIndex_;
}
Index RaftNode::lastApplied() const {
  std::lock_guard<std::mutex> lock(mu_);
  return lastApplied_;
}

// ---- M3 snapshot support (stubs in Phase #2; real logic lands in M3.1+) ----

void RaftNode::maybeSnapshot() {
  std::shared_ptr<const SnapshotView> view;
  Index snapIndex = kNoIndex;
  Term snapTerm = kNoTerm;
  uint64_t epoch = 0;

  {
    std::lock_guard<std::mutex> lock(mu_);
    if (snapshots_ == nullptr) return;

    const bool due =
        snapshotRequested_ ||
        (lastApplied_ > lastIncluded_ &&
         lastApplied_ - lastIncluded_ >= cfg_.snapshotThresholdEntries);
    if (!due) return;

    snapshotRequested_ = false;
    snapIndex = lastApplied_;
    snapTerm = log_.termAt(snapIndex);
    if (snapIndex == kNoIndex || snapTerm == kNoTerm) return;  // nothing yet
    epoch = installEpoch_;
    view = sm_.snapshotView();  // memory copy only, no serialization (L8)
  }
  if (!view) return;

  // Serialize + persist OUTSIDE the lock (lock discipline L8).
  SnapshotData data;
  data.lastIncludedIndex = snapIndex;
  data.lastIncludedTerm = snapTerm;
  data.payload = view->serialize();
  if (!snapshots_->save(data)) return;
  Bytes encoded = encodeSnapshotFile(data);  // outside the lock (L8)

  {
    std::lock_guard<std::mutex> lock(mu_);
    // B5: an InstallSnapshot that landed while we were persisting is newer and
    // must win — never move the boundary (or the log) backwards.
    if (epoch != installEpoch_) return;
    if (!(snapIndex <= commitIndex_ && snapIndex > lastIncluded_)) return;
    // B12: only advance the boundary once compaction really succeeded; a false
    // return means the on-disk log no longer matches our in-memory state.
    if (!log_.compact(snapIndex, snapTerm)) {
      throw std::runtime_error("raft: log compaction failed");
    }
    lastIncluded_ = snapIndex;
    lastIncludedTerm_ = snapTerm;
    snapshotBytes_ = std::move(encoded);
    for (auto& kv : snapshotSendOffset_) kv.second = 0;  // restart peer sends
    if (snapIndex > syncedIndex_) syncedIndex_ = snapIndex;
  }
}

void RaftNode::triggerSnapshot() {
  std::lock_guard<std::mutex> lock(mu_);
  snapshotRequested_ = true;
}

Index RaftNode::lastIncludedIndex() const {
  std::lock_guard<std::mutex> lock(mu_);
  return lastIncluded_;
}

Term RaftNode::lastIncludedTerm() const {
  std::lock_guard<std::mutex> lock(mu_);
  return lastIncludedTerm_;
}

InstallSnapshotReply RaftNode::onInstallSnapshot(const InstallSnapshotArgs& args) {
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (args.term < currentTerm_) return {currentTerm_, false, 0};
    if (args.term > currentTerm_) {
      becomeFollower(args.term);
    } else if (role_ == Role::kCandidate) {
      role_ = Role::kFollower;
    }
    leaderId_ = args.leaderId;
    lastHeartbeatMs_ = clock_.nowMs();  // reset election timer
    // Stale snapshot: our state is already at/after this boundary -> ignore.
    // nextOffset carries our boundary+1 so the leader can resume AppendEntries
    // from there instead of retrying the snapshot forever.
    if (args.lastIncludedIndex <= lastIncluded_) {
      return {currentTerm_, false, lastIncluded_ + 1};
    }
    if (snapshots_ == nullptr) return {currentTerm_, false, 0};
  }

  // L8: chunk I/O happens OUTSIDE mu_. receiveChunk() is idempotent for
  // retransmitted chunks and serialises concurrent transfers internally.
  if (!snapshots_->receiveChunk(args.lastIncludedIndex, args.lastIncludedTerm,
                                args.offset, args.data, args.done)) {
    return {currentTerm(), false, 0};  // leader restarts the transfer at 0
  }
  if (!args.done) return {currentTerm(), true, 0};

  SnapshotData installed;
  if (!snapshots_->load(installed)) return {currentTerm(), false, 0};
  // Remember the installed bytes: if this node becomes leader it must be able
  // to serve this snapshot to lagging peers (B6).
  Bytes encoded = encodeSnapshotFile(installed);  // outside the lock (L8)

  {
    std::lock_guard<std::mutex> lock(mu_);
    if (installed.lastIncludedIndex <= lastIncluded_) {
      return {currentTerm_, true, 0};  // raced with a newer install
    }
    // Raft §7: only keep the local suffix if our entry at the snapshot boundary
    // carries the same term; otherwise the whole suffix conflicts.
    if (log_.termAt(installed.lastIncludedIndex) != installed.lastIncludedTerm) {
      if (!log_.truncateSuffix(log_.firstIndex())) {
        return {currentTerm_, false, 0};
      }
    }
    if (!sm_.restore(installed.payload)) return {currentTerm_, false, 0};
    // B12: compaction must succeed before the boundary is advanced.
    if (!log_.compact(installed.lastIncludedIndex, installed.lastIncludedTerm)) {
      throw std::runtime_error("raft: log compaction failed (InstallSnapshot)");
    }
    ++installEpoch_;  // B5: invalidates any in-flight maybeSnapshot() save
    lastIncluded_ = installed.lastIncludedIndex;
    lastIncludedTerm_ = installed.lastIncludedTerm;
    snapshotBytes_ = std::move(encoded);
    for (auto& kv : snapshotSendOffset_) kv.second = 0;  // restart peer sends
    // B9 (I4): commit/applied never move backwards. restore() rewound the state
    // machine to the boundary, so replay whatever is still committed above it.
    lastApplied_ = installed.lastIncludedIndex;
    if (installed.lastIncludedIndex > commitIndex_) {
      commitIndex_ = installed.lastIncludedIndex;
    }
    if (installed.lastIncludedIndex > syncedIndex_) {
      syncedIndex_ = installed.lastIncludedIndex;
    }
    advanceCommitAndApply();
  }
  return {currentTerm(), true, 0};
}

void RaftNode::onInstallSnapshotReply(int peerId,
                                      const InstallSnapshotReply& reply) {
  std::lock_guard<std::mutex> lock(mu_);
  if (role_ != Role::kLeader) return;
  if (reply.term > currentTerm_) {
    becomeFollower(reply.term);
    return;
  }
  if (reply.term < currentTerm_) return;
  if (!reply.success) {
    // The transfer failed (or the peer already had a newer snapshot): drop the
    // in-flight offset so the next attempt restarts at 0 instead of wedging on
    // a chunk the receiver will never accept (B7).
    snapshotSendOffset_[peerId] = 0;
    // The peer already has state at/after our snapshot boundary: resume from
    // its reported boundary+1 rather than retrying the snapshot forever.
    if (reply.nextOffset > 0 && reply.nextOffset > lastIncluded_) {
      nextIndex_[peerId] = reply.nextOffset;
    }
    return;
  }

  snapshotSendOffset_[peerId] = snapshotChunkEnd_[peerId];
  if (!snapshotBytes_.empty() &&
      snapshotSendOffset_[peerId] >= snapshotBytes_.size()) {
    // Transfer complete: the peer now has the snapshot.
    nextIndex_[peerId] = lastIncluded_ + 1;
    matchIndex_[peerId] = lastIncluded_;
    snapshotSendOffset_[peerId] = 0;
  }
}

}  // namespace raftkv::raft
