#pragma once

#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "cluster_config.h"
#include "lock_probe.h"
#include "types.h"

namespace raftkv::raft {

class Clock;
class Metrics;
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
           const ClusterConfig& seed = ClusterConfig{},
           Metrics* metrics = nullptr);  // M5.1: 可选指标（nullptr = 零开销）

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
    bool skip = false;                // M5.3：该 peer 已有在途发送 -> 本 tick 跳过
    bool isSnapshot = false;
    AppendEntriesArgs append;
    InstallSnapshotArgs snapshot;
  };
  bool retiredLocked() const;                      // 调用方持锁
  bool hasMajorityLocked(const ClusterConfig& c, Index index) const;
  std::vector<int> replicationTargetsLocked() const;   // 配置成员 ∪ CatchUp 目标
  void erasePeerStateLocked(int peerId);            // 含 readAcks_ 清理（评审 O4）
  // 已彻底离开配置的 peer（不在 currConfig_/CatchUp/送达集合）：调用方负责
  // erasePeerStateLocked + 锁外 transport_.removePeer（L10）。
  std::vector<int> removablePeersLocked() const;
  void applyAppendedConfigLocked(const std::vector<LogEntry>& appended);
  void adoptConfigLocked(const ClusterConfig& sc, Index inFlightIndex,
                         bool computeDraining);
  void recomputeConfigLocked();   // 日志截断后的配置回滚（设计 §5.2）
  PeerJob buildPeerJobLocked(int peer);
  // M5.3：该 peer 当前是否允许再发一批（无在途，或在途已超时）
  bool peerSendAllowedLocked(int peer) const;
  // M4 评审 B6：把「追加日志条目」与「等待提交」拆开，使成员变更能在同一个 mu_
  // 临界区内完成「J1 复查 + 构造配置 + 追加条目」（version 直接用真实条目 index）。
  bool appendEntryLocked(const LogEntry& e);                        // 调用方持锁
  ClientReply awaitCommit(Index index, Term term, uint64_t timeoutMs);
  bool catchUpPeer(int peerId, uint64_t timeoutMs);
  void drainPeerQueues();                          // L10：锁外执行地址簿更新
  void purgeDrainsLocked();                        // 送达目标：确认完成/预算到期 -> 回收
  // M5.2（I5/I9）：把当前 term/votedFor 在**锁外**落盘。内部持 metaPersistMu_ 串行化
  // "读取当前值 + 落盘"，保证磁盘 meta 版本单调；metaDirty_ 用于 becomeFollower 的延迟落盘。
  bool flushMetaOutsideLock();
  bool readQuorumLocked(uint64_t seq) const;       // ReadIndex 多数派（M4.4）
  // M4 评审 B5：§8 读屏障——commitIndex_ 处的条目是否属于当前任期
  bool hasCurrentTermCommitLocked() const;

  RaftConfig cfg_;
  LogStore& log_;
  StateMachine& sm_;
  Transport& transport_;
  Clock& clock_;
  SnapshotStore* snapshots_ = nullptr;  // nullptr == snapshots disabled (M2)
  ClusterConfig seedConfig_;            // M4: 启动种子配置（--peers），version = 0
  Metrics* metrics_ = nullptr;          // M5.1: 指标（只读、不参与判定）
  ClusterConfig currConfig_;            // M4: 当前配置（seed -> 日志配置条目；快照配置见 M4.3）
  ClusterConfig prevConfig_;            // M4: 在途配置条目的 C_old（J2 双重多数派用）
  ClusterConfig baseConfig_;            // M4.5: 已持久基线（快照 / 已提交配置）；回滚基准
  Index inFlightConfigIndex_ = kNoIndex;  // M4: 在途配置条目 index（kNoIndex = 无）
  std::unordered_map<int, std::string> pendingPeers_;  // M4: CatchUp 目标（非投票、不计多数派）
  std::vector<std::pair<int, std::string>> peerAddQueue_;  // M4: 待注册地址（锁外执行）
  std::vector<int> peerRemoveQueue_;                       // M4: 待摘除节点（锁外执行）
  // M4 设计 v1.4(a) / 评审 O2：C_old 有、C_new 无的节点在**确认收到**移除它的配置
  // 条目之前仍是复制目标（否则它会一直自认成员、靠不断竞选抬高任期搅乱集群）；
  // 超过预算仍未确认则放弃（不能让一个彻底下线的节点长期占着复制目标与连接）。
  struct DrainState {
    Index until = kNoIndex;   // 需要它确认收到的配置条目 index
    uint64_t deadlineMs = 0;  // 放弃时刻（clock_ 时间轴）
  };
  std::unordered_map<int, DrainState> drainingPeers_;
  // M4 评审 B6：成员变更串行化（覆盖 CatchUp 阶段，见 changeMembership）
  MembershipMutex membershipMu_;
  // M5.2：串行化 meta 落盘（叶子锁，锁序 metaPersistMu_ -> mu_）；metaDirty_ 受 mu_ 保护。
  MetaMutex metaPersistMu_;
  bool metaDirty_ = false;
  // M5.2：非选举场景的 meta 落盘限频（I5 只对授权/自投票要求严格）
  static constexpr uint64_t kMetaFlushMinGapMs = 50;
  uint64_t lastMetaFlushMs_ = 0;
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
  // M5.2：InstallSnapshot 的 compact 搬到锁外执行（I9），这里暂存待压缩边界。
  bool pendingInstallCompact_ = false;
  Index pendingInstallIndex_ = kNoIndex;
  Term pendingInstallTerm_ = kNoTerm;
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
  // M5.3（异步 transport 的必要约束）：同一 peer 同时只能有**一批**在途。
  // 原因：应答没有序号，`onAppendEntriesReply` 用 `lastSentEndIndex_[peer]` 归因；
  // 若多批在途，后一批的范围会被算到前一批的 ack 上 -> over-count -> 已 ack 的写丢失
  // （A/B 实测：pipeline=64 出现 missing 59）。超时（2*rpcTimeoutMs）后允许重发。
  std::unordered_map<int, uint64_t> appendSentMs_;
  std::unordered_map<int, uint64_t> snapshotSentMs_;
  // M4：peer 在本任期是否成功应答过 AppendEntries（设计 §5.4 步骤 5 追平判据）
  std::unordered_map<int, Term> ackedTerm_;

  mutable ProbedMutex mu_;
  // M5：mu_ 换成 ProbedMutex 后必须用 condition_variable_any（cv 只接受
  // unique_lock<std::mutex>）。等待期间是解锁状态，与 lockprobe 语义一致。
  std::condition_variable_any cv_;
};

}  // namespace raftkv::raft
