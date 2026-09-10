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
#include <utility>

#include "raft/clock.h"
#include "raft/log_store.h"
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
                   Transport& transport, Clock& clock)
    : cfg_(std::move(cfg)),
      log_(log),
      sm_(sm),
      transport_(transport),
      clock_(clock) {
  // Restore durable term/votedFor on restart (log entries live in log_ itself).
  {
    Term t = kNoTerm;
    int v = -1;
    Index last = kNoIndex;
    if (log_.load(t, v, last)) {
      currentTerm_ = t;
      votedFor_ = v;
    }
  }
  lastHeartbeatMs_ = clock_.nowMs();
  electionTimeoutMs_ = electionTimeoutMs(cfg_, cfg_.selfId, currentTerm_);
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
      size_t replicated = 1;                         // self
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
  std::vector<std::pair<int, RequestVoteArgs>> voteJobs;
  std::vector<std::pair<int, AppendEntriesArgs>> appendJobs;
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
          advanceCommitAndApply();  // single-node cluster commits immediately
        }
      }
      if (now - lastHeartbeatSentMs_ >= cfg_.heartbeatMs) {
        lastHeartbeatSentMs_ = now;
        for (const int peer : cfg_.peerIds) {
          appendJobs.emplace_back(peer, buildAppendEntries(peer));
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

  for (const LogEntry& e : args.entries) {
    if (e.index <= log_.lastIndex()) {
      if (log_.termAt(e.index) == e.term) continue;
      if (!log_.truncateSuffix(e.index)) {
        return {currentTerm_, false, kNoIndex, kNoTerm};
      }
    }
    if (!log_.append({e})) {
      return {currentTerm_, false, kNoIndex, kNoTerm};
    }
  }

  if (args.leaderCommit > commitIndex_) {
    commitIndex_ = std::min(args.leaderCommit, log_.lastIndex());
    advanceCommitAndApply();
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
    // Fast backup (M2.5): jump to the follower's reported conflict index.
    Index target = reply.conflictIndex;
    if (target == kNoIndex || target < 1) target = 1;
    if (target < nextIndex_[peerId]) {
      nextIndex_[peerId] = target;
    } else if (nextIndex_[peerId] > 1) {
      --nextIndex_[peerId];  // safety fallback (should not normally happen)
    }
  }
}

ClientReply RaftNode::propose(const ClientRequest& req, uint64_t timeoutMs) {
  std::vector<std::pair<int, AppendEntriesArgs>> appendJobs;
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
    if (!log_.append({e})) {
      return {ClientStatus::kErr, "log append failed", -1};
    }
    matchIndex_[cfg_.selfId] = e.index;
    for (const int peer : cfg_.peerIds) {
      appendJobs.emplace_back(peer, buildAppendEntries(peer));
    }
  }

  for (auto& job : appendJobs) {
    transport_.sendAppendEntries(
        job.first, job.second,
        [this, peer = job.first](const AppendEntriesReply& reply) {
          onAppendEntriesReply(peer, reply);
        });
  }

  std::unique_lock<std::mutex> lock(mu_);
  (void)cv_.wait_for(lock, std::chrono::milliseconds(timeoutMs), [&] {
    return e.index <= commitIndex_ || role_ != Role::kLeader ||
           currentTerm_ != e.term;
  });

  if (role_ != Role::kLeader || currentTerm_ != e.term) {
    return {ClientStatus::kNotLeader, "", leaderId_};
  }
  if (e.index <= commitIndex_) return {ClientStatus::kOk, "", -1};
  return {ClientStatus::kErr, "timeout waiting for commit", -1};
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

}  // namespace raftkv::raft
