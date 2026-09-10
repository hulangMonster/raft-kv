#pragma once

#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "types.h"

namespace raftkv::raft {

class Clock;
class LogStore;
class StateMachine;
class Transport;

// Raft consensus core. Contains NO socket or filesystem code: persistence and
// networking are injected (LogStore / Transport / Clock).
//
// Lock discipline (m2-prerequisites.md L1-L5):
//   * mu_ guards all consensus state.
//   * Public methods never call transport_ while holding mu_; they build
//     outbound jobs under the lock, release it, then send (L2/L4).
//   * propose() waits on cv_ for commit / step-down / timeout (L5).
//   * Private helpers below assume the caller already holds mu_.
class RaftNode {
 public:
  RaftNode(RaftConfig cfg, LogStore& log, StateMachine& sm,
           Transport& transport, Clock& clock);

  void tick();  // called by the ticker thread (or manually by unit tests)

  // Inbound RPC handlers (called by the transport on the receiving side).
  RequestVoteReply onRequestVote(const RequestVoteArgs& args);
  AppendEntriesReply onAppendEntries(const AppendEntriesArgs& args);

  // Outbound RPC callbacks (called by the transport on the sending side).
  void onRequestVoteReply(int peerId, const RequestVoteReply& reply);
  void onAppendEntriesReply(int peerId, const AppendEntriesReply& reply);

  // Client proposal. Blocks until committed / stepped down / timeout.
  ClientReply propose(const ClientRequest& req, uint64_t timeoutMs);

  Role role() const;
  Term currentTerm() const;
  int leaderId() const;         // -1 when unknown
  Index commitIndex() const;
  Index lastApplied() const;

 private:
  // Caller holds mu_.
  void startElection(uint64_t now,
                     std::vector<std::pair<int, RequestVoteArgs>>& voteJobs);
  void becomeFollower(Term newTerm);
  void becomeLeader();
  AppendEntriesArgs buildAppendEntries(int peer);
  void advanceCommitAndApply();

  RaftConfig cfg_;
  LogStore& log_;
  StateMachine& sm_;
  Transport& transport_;
  Clock& clock_;

  Role role_ = Role::kFollower;
  Term currentTerm_ = kNoTerm;
  int votedFor_ = -1;
  int leaderId_ = -1;
  int votesGranted_ = 0;                 // granted votes in the current election
  uint64_t lastHeartbeatMs_ = 0;         // election timer (reset by RPC)
  uint64_t electionTimeoutMs_ = 0;
  uint64_t lastHeartbeatSentMs_ = 0;     // leader-only heartbeat pacing

  Index commitIndex_ = kNoIndex;
  Index lastApplied_ = kNoIndex;

  // Leader-only replication bookkeeping.
  std::unordered_map<int, Index> nextIndex_;
  std::unordered_map<int, Index> matchIndex_;
  std::unordered_map<int, Index> lastSentEndIndex_;  // ack context per peer

  mutable std::mutex mu_;
  std::condition_variable cv_;
};

}  // namespace raftkv::raft
