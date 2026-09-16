#pragma once

#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "cluster_config.h"
#include "types.h"

namespace raftkv::raft {

class Clock;
class LogStore;
class SnapshotStore;
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
  // M4: seed = 启动种子配置（version=0，来自 --peers）；真实配置由 快照配置 -> 日志配置条目 重建。
  RaftNode(RaftConfig cfg, LogStore& log, StateMachine& sm,
           Transport& transport, Clock& clock,
           SnapshotStore* snapshots = nullptr,
           const ClusterConfig& seed = ClusterConfig{});

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

  // ---- M3: snapshots (m3-design.md v1.1 §4.4) ----
  void triggerSnapshot();  // manual trigger; consumed by the next tick()
  Index lastIncludedIndex() const;
  Term lastIncludedTerm() const;
  InstallSnapshotReply onInstallSnapshot(const InstallSnapshotArgs& args);
  void onInstallSnapshotReply(int peerId, const InstallSnapshotReply& reply);

  // ---- M4: membership + linearizable read (m4-design.md v1.1 §4.3) ----
  ClusterConfig clusterConfig() const;   // 值拷贝（锁内读）
  uint64_t configVersion() const;
  bool retired() const;                  // self 不是当前配置的投票成员
  // add: 先 CatchUp 追平再追加配置条目；remove: 直接追加。非 Leader -> kNotLeader。
  ClientReply changeMembership(MembershipOp op, int targetId,
                               const std::string& targetAddr, uint64_t timeoutMs);
  ClientReply linearizableGet(const std::string& key, uint64_t timeoutMs);
  ReadProbeReply onReadProbe(const ReadProbeArgs& args);
  void onReadProbeReply(int peerId, const ReadProbeReply& reply);

 private:
  // Caller holds mu_.
  void startElection(uint64_t now,
                     std::vector<std::pair<int, RequestVoteArgs>>& voteJobs);
  void becomeFollower(Term newTerm);
  void becomeLeader();
  AppendEntriesArgs buildAppendEntries(int peer);
  void advanceCommitAndApply();
  void maybeSnapshot();
  void rebuildConfigFromSeedAndLog();

  // ---- M4.2: membership plumbing ----
  struct PeerJob {                    // 给某个 peer 的复制作业（追加 or 快照块）
    bool isSnapshot = false;
    AppendEntriesArgs append;
    InstallSnapshotArgs snapshot;
  };
  bool retiredLocked() const;                      // 调用方持锁
  bool hasMajorityLocked(const ClusterConfig& c, Index index) const;
  std::vector<int> replicationTargetsLocked() const;   // 配置成员 ∪ CatchUp 目标
  void erasePeerStateLocked(int peerId);
  void applyAppendedConfigLocked(const std::vector<LogEntry>& appended);
  void adoptConfigLocked(const ClusterConfig& sc, Index inFlightIndex,
                         bool computeDraining);
  void recomputeConfigLocked();   // 日志截断后的配置回滚（设计 §5.2）
  PeerJob buildPeerJobLocked(int peer);
  bool catchUpPeer(int peerId, uint64_t timeoutMs);
  void drainPeerQueues();                          // L10：锁外执行地址簿更新
  bool readQuorumLocked(uint64_t seq) const;       // ReadIndex 多数派（M4.4）

  RaftConfig cfg_;
  LogStore& log_;
  StateMachine& sm_;
  Transport& transport_;
  Clock& clock_;
  SnapshotStore* snapshots_ = nullptr;  // nullptr == snapshots disabled (M2)
  ClusterConfig seedConfig_;            // M4: 启动种子配置（--peers），version = 0
  ClusterConfig currConfig_;            // M4: 当前配置（seed -> 日志配置条目；快照配置见 M4.3）
  ClusterConfig prevConfig_;            // M4: 在途配置条目的 C_old（J2 双重多数派用）
  ClusterConfig baseConfig_;            // M4.5: 已持久基线（快照 / 已提交配置）；回滚基准
  Index inFlightConfigIndex_ = kNoIndex;  // M4: 在途配置条目 index（kNoIndex = 无）
  std::unordered_map<int, std::string> pendingPeers_;  // M4: CatchUp 目标（非投票、不计多数派）
  std::vector<std::pair<int, std::string>> peerAddQueue_;  // M4: 待注册地址（锁外执行）
  std::vector<int> peerRemoveQueue_;                       // M4: 待摘除节点（锁外执行）
  std::vector<int> drainingPeers_;      // M4: C_old 有、C_new 无的节点；配置条目提交前仍要送达
  uint64_t readSeq_ = 0;                            // M4.4: ReadIndex 探针序号（单调）
  std::unordered_map<int, uint64_t> readAcks_;      // M4.4: peer -> 已确认的最大探针序号

  // M3 snapshot boundary state
  Index lastIncluded_ = kNoIndex;
  Term lastIncludedTerm_ = kNoTerm;
  bool snapshotRequested_ = false;
  Bytes snapshotBytes_;  // encoded snapshot used to serve InstallSnapshot
  // Bumped on every accepted InstallSnapshot. A snapshot generated by
  // maybeSnapshot() is discarded if this changed while it was being persisted
  // (the installed snapshot is newer and must win).
  uint64_t installEpoch_ = 0;
  std::unordered_map<int, uint64_t> snapshotSendOffset_;  // per-peer progress
  std::unordered_map<int, uint64_t> snapshotChunkEnd_;    // per-peer last chunk

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
  // M5 group commit: highest log index known to be locally durable, and a flag
  // ensuring only one proposer performs the batch fsync at a time.
  Index syncedIndex_ = kNoIndex;
  bool syncInFlight_ = false;

  // Leader-only replication bookkeeping.
  std::unordered_map<int, Index> nextIndex_;
  std::unordered_map<int, Index> matchIndex_;
  std::unordered_map<int, Index> lastSentEndIndex_;  // ack context per peer

  mutable std::mutex mu_;
  std::condition_variable cv_;
};

}  // namespace raftkv::raft
