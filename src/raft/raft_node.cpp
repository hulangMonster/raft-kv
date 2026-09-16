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
#include <thread>
#include <utility>

#include "raft/clock.h"
#include "raft/lock_probe.h"
#include "raft/log_store.h"
#include "raft/metrics.h"
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
                   SnapshotStore* snapshots, const ClusterConfig& seed,
                   Metrics* metrics)
    : cfg_(std::move(cfg)),
      log_(log),
      sm_(sm),
      transport_(transport),
      clock_(clock),
      snapshots_(snapshots),
      seedConfig_(seed),
      metrics_(metrics) {
  // M4 scaffolding: seedConfig_ 尚未参与配置重建（M4.1 落地，见 m4-prerequisites.md §5.1）
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
      // M4.3：快照携带配置 -> 作为恢复基线（优先于 seed；日志中的配置条目随后再推进）
      if (!snap.config.empty()) {
        ClusterConfig sc;
        if (!decodeClusterConfig(snap.config.data(), snap.config.size(), sc)) {
          throw std::runtime_error("raft: undecodable cluster config in snapshot");
        }
        currConfig_ = sc;
      }
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

  // M4.1: 启动配置重建（seed -> 日志中的配置条目；快照配置在 M4.3 接入）
  rebuildConfigFromSeedAndLog();
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
    // M5.2（I9）：这里不再在持锁状态落盘。term/votedFor 由 tick 的
    // flushMetaOutsideLock() 在锁外落盘（~10ms 内 durable）；真正要求
    // "回复前必须 durable" 的是授权投票与自投票（见 onRequestVote/startElection）。
    metaDirty_ = true;
  }
  leaderId_ = -1;
  votesGranted_ = 0;
  lastHeartbeatMs_ = clock_.nowMs();
  electionTimeoutMs_ = electionTimeoutMs(cfg_, cfg_.selfId, currentTerm_);

  nextIndex_.clear();
  matchIndex_.clear();
  lastSentEndIndex_.clear();
  ackedTerm_.clear();  // 新任期/新角色：旧的应答不作数
  snapshotSendOffset_.clear();
  snapshotChunkEnd_.clear();
  cv_.notify_all();
}

void RaftNode::becomeLeader() {
  role_ = Role::kLeader;
  leaderId_ = cfg_.selfId;
  lastHeartbeatSentMs_ = 0;  // send an immediate heartbeat on the next tick

  const Index next = log_.lastIndex() + 1;
  for (const int peer : replicationTargetsLocked()) {
    nextIndex_[peer] = next;
    matchIndex_[peer] = kNoIndex;
    lastSentEndIndex_[peer] = kNoIndex;
    snapshotSendOffset_[peer] = 0;
    snapshotChunkEnd_[peer] = 0;
  }
  // 新配置里可能有本节点此前不知道的地址：出锁后注册（L10）
  for (const Member& m : currConfig_.members) {
    if (m.id != cfg_.selfId && !m.addr.empty()) {
      peerAddQueue_.emplace_back(m.id, m.addr);
    }
  }
  matchIndex_[cfg_.selfId] = log_.lastIndex();
  ackedTerm_.clear();  // 本任期尚无任何应答（追平判据要求本任期应答）
  cv_.notify_all();
}

void RaftNode::startElection(
    uint64_t now, std::vector<std::pair<int, RequestVoteArgs>>& voteJobs) {
  role_ = Role::kCandidate;
  ++currentTerm_;
  votedFor_ = cfg_.selfId;
  leaderId_ = -1;
  votesGranted_ = 0;
  // M5.2（I5/I9）：term/votedFor 的落盘移到 tick 的锁外阶段——**必须落盘成功后才发
  // 投票请求**（否则崩溃后可能重复投票，违反 I3）。这里只标记。
  metaDirty_ = true;
  lastHeartbeatMs_ = now;
  electionTimeoutMs_ = electionTimeoutMs(cfg_, cfg_.selfId, currentTerm_);

  const std::vector<int> voters = currConfig_.votingIds();
  if (voters.size() <= 1) {  // 只有自己一个投票成员 -> 立即当选
    becomeLeader();
    return;
  }

  for (const int peer : voters) {
    if (peer == cfg_.selfId) continue;
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
    for (Index n = log_.lastIndex(); n > commitIndex_; --n) {
      if (log_.termAt(n) != currentTerm_) continue;  // §5.4.2: only current term
      if (!hasMajorityLocked(currConfig_, n)) continue;
      // J2（评审 B2）：只要 commitIndex_ 会触及或越过在途配置条目，就必须同时拿到
      // C_old 对该条目的多数派。只判断 n == inFlightConfigIndex_ 会让"后一条普通
      // 条目先提交"绕过 J2（偶数旧配置下 C_new 多数派可以不含 C_old 多数派），
      // 从而把已提交的配置/条目暴露给 §5.4.2 反例。
      if (inFlightConfigIndex_ != kNoIndex && n >= inFlightConfigIndex_ &&
          !hasMajorityLocked(prevConfig_, inFlightConfigIndex_)) {
        continue;
      }
      commitIndex_ = n;
      break;
    }
  }

  // Apply committed entries in order. 配置条目只推进 applied（配置在追加时已生效）
  while (lastApplied_ < commitIndex_) {
    const Index next = lastApplied_ + 1;
    auto entries = log_.slice(next, 1, std::numeric_limits<size_t>::max());
    if (entries.empty() || entries[0].index != next) break;
    // M4（评审 O9）：配置条目同样交给状态机——KvStateMachine 把它当作"推进
    // applied 的 no-op"，这样 SM 的 applied 与 Raft 的 lastApplied_ 始终一致。
    sm_.apply(entries[0]);
    lastApplied_ = next;
  }

  // 配置条目一旦提交：释放在途标记；自身被移除则让位（§5.7）
  if (inFlightConfigIndex_ != kNoIndex && commitIndex_ >= inFlightConfigIndex_) {
    inFlightConfigIndex_ = kNoIndex;
    prevConfig_ = currConfig_;
    baseConfig_ = currConfig_;  // 已提交 -> 成为新的回滚基线
    // 送达目标收尾（设计 v1.4(a)）：被移除的节点只有**确认收到**移除它的配置条目
    // 才算送达完成（matchIndex >= 条目 index）；一直没确认的（例如已经下线）由
    // 预算超时在 purgeDrainsLocked() 里放弃。非 Leader 不承担送达职责。
    for (auto it = drainingPeers_.begin(); it != drainingPeers_.end();) {
      const auto mit = matchIndex_.find(it->first);
      const bool acked =
          mit != matchIndex_.end() && mit->second >= it->second.until;
      if (acked || role_ != Role::kLeader ||
          clock_.nowMs() >= it->second.deadlineMs) {
        it = drainingPeers_.erase(it);
      } else {
        ++it;
      }
    }
    // 评审 O2：已经彻底离开配置的 peer，其每 peer 状态（nextIndex_/matchIndex_/
    // 快照发送进度/readAcks_）与地址簿条目必须回收，否则要等到下一次配置变更或
    // InstallSnapshot 才可能被清理（连接/内存泄漏）。
    const std::vector<int> gone = removablePeersLocked();
    for (const int id : gone) {
      erasePeerStateLocked(id);
      peerRemoveQueue_.push_back(id);  // 锁外执行 transport_.removePeer（L10）
    }
    if (role_ == Role::kLeader && !currConfig_.isVoting(cfg_.selfId)) {
      becomeFollower(currentTerm_);
    }
  }
  cv_.notify_all();
}

// ---- public API -------------------------------------------------------------

void RaftNode::tick() {
  maybeSnapshot();  // M3: two-phase snapshot; takes mu_ internally

  std::vector<std::pair<int, RequestVoteArgs>> voteJobs;
  std::vector<std::pair<int, AppendEntriesArgs>> appendJobs;
  std::vector<std::pair<int, InstallSnapshotArgs>> snapJobs;
  Index noopPending = kNoIndex;
  Term electionTerm = kNoTerm;
  bool metaFlushNeeded = false;
  {
    std::lock_guard<ProbedMutex> lock(mu_);
    const uint64_t now = clock_.nowMs();

    if (role_ == Role::kLeader) {
      purgeDrainsLocked();  // 送达目标确认完成/预算到期 -> 回收（O2 + 设计 v1.4(a)）
      // Raft §8: a leader must commit an entry from its own term before it
      // may advance the commit point over prior-term entries (and serve reads).
      if (cfg_.appendNoop && log_.lastTerm() != currentTerm_) {
        LogEntry noop;
        noop.index = log_.lastIndex() + 1;
        noop.term = currentTerm_;
        noop.op = OpCode::kGet;
        noop.clientId = 0;
        noop.requestId = noop.index;  // unique; not a client request
        // M5.2（I9）：锁内只追加（write），fsync 在锁外做（否则 §8 屏障要等下一次
        // 客户端写才可能满足，线性一致读会整体失败）。
        if (log_.appendNoSync({noop})) {
          matchIndex_[cfg_.selfId] = noop.index;
          noopPending = noop.index;
        }
      }
    } else if (!retiredLocked() && now - lastHeartbeatMs_ >= electionTimeoutMs_) {
      startElection(now, voteJobs);
      electionTerm = currentTerm_;
    }
    metaFlushNeeded = metaDirty_ || (electionTerm != kNoTerm);
  }

  // 锁外 1：no-op 的 fsync（让 syncedIndex_ 门控在同一 tick 内放行）
  if (noopPending != kNoIndex) {
    const uint64_t fsyncT0Us = lockprobe::nowUs();
    const bool syncOk = log_.sync();
    if (metrics_ != nullptr) {
      metrics_->onFsync(lockprobe::nowUs() - fsyncT0Us);
    }
    std::lock_guard<ProbedMutex> lock(mu_);
    if (syncOk) {
      Index durable = noopPending;
      if (durable > log_.lastIndex()) durable = log_.lastIndex();
      if (durable > syncedIndex_) syncedIndex_ = durable;
      advanceCommitAndApply();  // single-node cluster commits immediately
    }
  }

  // 锁外 2：term/votedFor 落盘。
  //   * 选举（自投票）：I5 要求**发投票请求前必须 durable** -> 每次都落盘
  //   * 其它（becomeFollower 的任期更新）：只要求"尽快 durable" -> **限频 ≥50ms**
  //     （v1.4 观测：故障注入下任期频繁更替会让 ticker 反复做 meta fsync（1.4-2.8ms/次），
  //      并与授权路径争 metaPersistMu_，把心跳节奏拖慢 -> 触发更多选举 -> 正反馈）
  if (metaFlushNeeded) {
    bool doFlush = (electionTerm != kNoTerm);
    if (doFlush) {
      std::lock_guard<ProbedMutex> lock(mu_);
      lastMetaFlushMs_ = clock_.nowMs();
    } else {
      const uint64_t nowMs = clock_.nowMs();
      std::lock_guard<ProbedMutex> lock(mu_);
      if (nowMs - lastMetaFlushMs_ >= kMetaFlushMinGapMs) {
        lastMetaFlushMs_ = nowMs;
        doFlush = true;
      }
    }
    if (doFlush) {
      const bool ok = flushMetaOutsideLock();
      if (!ok) {
        voteJobs.clear();
      } else if (electionTerm != kNoTerm) {
        std::lock_guard<ProbedMutex> lock(mu_);
        if (currentTerm_ != electionTerm) voteJobs.clear();  // 窗口内被取代
      }
    } else if (electionTerm != kNoTerm) {
      voteJobs.clear();  // 不会发生（选举必落盘），保守兜底
    }
  }

  // 锁段 2：心跳/复制作业（每个 peer 一条；落后于快照边界 -> 一个快照块）
  {
    std::lock_guard<ProbedMutex> lock(mu_);
    const uint64_t now = clock_.nowMs();
    if (role_ == Role::kLeader && now - lastHeartbeatSentMs_ >= cfg_.heartbeatMs) {
      lastHeartbeatSentMs_ = now;
      for (const int peer : replicationTargetsLocked()) {
        PeerJob job = buildPeerJobLocked(peer);
        if (job.isSnapshot) {
          snapJobs.emplace_back(peer, std::move(job.snapshot));
        } else {
          appendJobs.emplace_back(peer, std::move(job.append));
        }
      }
    }
  }

  drainPeerQueues();  // L10：地址簿更新只在锁外执行

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

// M5.2（I5/I9）：锁外落盘 term/votedFor。持 metaPersistMu_ 串行化"读当前值 + 落盘"，
// 保证磁盘版本单调；不持 mu_ 做 IO。返回 false 表示落盘失败（调用方不得据此对外回复）。
bool RaftNode::flushMetaOutsideLock() {
  std::lock_guard<MetaMutex> metaLock(metaPersistMu_);
  Term t = kNoTerm;
  int v = -1;
  {
    std::lock_guard<ProbedMutex> lock(mu_);
    t = currentTerm_;
    v = votedFor_;
  }
  if (t == kNoTerm) return true;  // 尚未参与任何任期：无需落盘
  const uint64_t metaT0Us = lockprobe::nowUs();
  const bool ok = log_.persistMeta(t, v);
  if (metrics_ != nullptr) {
    metrics_->onFsync(lockprobe::nowUs() - metaT0Us);
  }
  {
    std::lock_guard<ProbedMutex> lock(mu_);
    if (ok && currentTerm_ == t) metaDirty_ = false;
  }
  return ok;
}

RequestVoteReply RaftNode::onRequestVote(const RequestVoteArgs& args) {
  // M5.2（I5）：整个"决定授权 -> 落盘 -> 回锁校验 -> 回复"在 metaPersistMu_ 下串行，
  // 既保证"回复授权前 votedFor 已 durable"，也保证磁盘 meta 版本单调（叶子锁，锁序
  // metaPersistMu_ -> mu_）。
  std::lock_guard<MetaMutex> metaLock(metaPersistMu_);
  Term grantedTerm = kNoTerm;
  {
    std::lock_guard<ProbedMutex> lock(mu_);
    // J4 + 纵深防御（M5.2 故障注入实测补充）：候选者不在当前配置的投票成员里时，
    // **连它的任期都不采纳**。否则一个"没收到自己被移除"的陈旧节点（或任何陌生节点）
    // 可以靠不停竞选把健康 Leader 逼下台 —— 它拿不到票，但每次竞选都发起一轮真实选举，
    // 足以把集群搅成选举风暴（实测：被移除节点落后一个配置版本 + 持续竞选）。
    // 不采纳不会让真正落后的节点卡住任期：它会通过 AppendEntries 或 RequestVoteReply
    // 里更高的 term 追平（两条路径都会 becomeFollower）。
    if (retiredLocked() || !currConfig_.isVoting(args.candidateId)) {
      return {currentTerm_, false};
    }
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
    if (!((votedFor_ == -1 || votedFor_ == args.candidateId) && upToDate)) {
      return {currentTerm_, false};
    }
    votedFor_ = args.candidateId;
    metaDirty_ = true;
    grantedTerm = currentTerm_;
    lastHeartbeatMs_ = clock_.nowMs();  // granting a vote resets timer
  }

  // 锁外落盘：I5 —— 没有 durable 就绝不授权
  const uint64_t metaT0Us = lockprobe::nowUs();
  const bool durable = log_.persistMeta(grantedTerm, args.candidateId);
  if (metrics_ != nullptr) {
    metrics_->onFsync(lockprobe::nowUs() - metaT0Us);
  }

  std::lock_guard<ProbedMutex> lock(mu_);
  if (!durable) return {currentTerm_, false};
  if (currentTerm_ != grantedTerm) return {currentTerm_, false};  // 窗口内已被更高 term 取代
  metaDirty_ = false;
  return {currentTerm_, true};
}

AppendEntriesReply RaftNode::onAppendEntries(const AppendEntriesArgs& args) {
  std::unique_lock<ProbedMutex> lock(mu_);
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
  // so prevLogTerm cannot be verified here.
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
      // M4.5（设计 §5.2 回滚）：被截断的配置条目不得继续生效
      recomputeConfigLocked();
    }
    toAppend.push_back(e);
  }
  if (!toAppend.empty()) {
    // M5.2（I9）：锁内只 write（page-cache），fsync 在锁外
    if (!log_.appendNoSync(toAppend)) {
      return {currentTerm_, false, kNoIndex, kNoTerm};
    }
    // 决策③「配置条目追加即生效」：必须在这里生效，不能推迟到 fsync 之后。
    // 否则一旦本次 fsync 失败或窗口内任期被取代（我们就不 ack），该条目已经躺在日志里，
    // 而重传时 `toAppend` 会为空（条目已存在）→ 配置**永远不会生效** → 收敛失败。
    // （M5.2 实测：raft_membership_fault.sh iter 26 "配置未收敛"，诊断 dump 定位到此。）
    applyAppendedConfigLocked(toAppend);
  }

  // The last entry this leader has actually shown us is the most we may ever
  // consider committed: anything beyond it is our own (unverified) suffix.
  const Index lastMatched =
      args.entries.empty() ? args.prevLogIndex : args.entries.back().index;
  const Term term0 = currentTerm_;
  const bool needSync = lastMatched > syncedIndex_;

  lock.unlock();
  bool syncOk = true;
  if (needSync) {
    const uint64_t fsyncT0Us = lockprobe::nowUs();
    syncOk = log_.sync();  // M5.2（I9）：fsync 在锁外执行
    if (metrics_ != nullptr) {
      metrics_->onFsync(lockprobe::nowUs() - fsyncT0Us);
    }
  }
  lock.lock();

  // 回锁复核：窗口内可能已被更高 term 取代 / 日志被截断或压缩
  if (currentTerm_ != term0) {
    return {currentTerm_, false, kNoIndex, kNoTerm};
  }
  if (!syncOk) {
    return {currentTerm_, false, kNoIndex, kNoTerm};  // I10：fsync 失败绝不 ack
  }
  if (needSync) {
    Index durable = lastMatched;
    if (durable > log_.lastIndex()) durable = log_.lastIndex();  // M3 B3 夹紧
    if (durable > syncedIndex_) syncedIndex_ = durable;
  }
  // I5/I11：到这里本批已 durable，才可以推进 commit / 回 success（配置条目在追加时
  // 已按决策③生效，见上）。
  if (args.leaderCommit > commitIndex_) {
    const Index newCommit = std::min(args.leaderCommit, lastMatched);
    if (newCommit > commitIndex_) {
      commitIndex_ = newCommit;
      advanceCommitAndApply();
    }
  }
  return {currentTerm_, true, kNoIndex, kNoTerm};
}

void RaftNode::onRequestVoteReply(int peerId, const RequestVoteReply& reply) {
  std::lock_guard<ProbedMutex> lock(mu_);
  if (reply.term > currentTerm_) {
    becomeFollower(reply.term);
    return;
  }
  if (role_ != Role::kCandidate || reply.term != currentTerm_ ||
      !reply.voteGranted) {
    return;
  }

  if (!currConfig_.isVoting(peerId)) return;  // 非投票成员 / 已移除节点的票不计
  const size_t majority = currConfig_.majority();
  if (majority == 0) return;

  ++votesGranted_;
  const size_t selfVote = currConfig_.isVoting(cfg_.selfId) ? 1 : 0;
  if (static_cast<size_t>(votesGranted_) + selfVote >= majority) {
    becomeLeader();
  }
}

void RaftNode::onAppendEntriesReply(int peerId, const AppendEntriesReply& reply) {
  std::lock_guard<ProbedMutex> lock(mu_);
  if (role_ != Role::kLeader) return;
  if (reply.term > currentTerm_) {
    becomeFollower(reply.term);
    return;
  }
  if (reply.term < currentTerm_) return;  // stale ack

  if (reply.success) {
    const Index end = lastSentEndIndex_[peerId];
    if (end > matchIndex_[peerId]) matchIndex_[peerId] = end;
    ackedTerm_[peerId] = currentTerm_;  // 追平判据：本任期已应答
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
  // M4.4（D-2）：GET 不再直接读本地状态机，改走 ReadIndex 线性一致读
  if (req.op == OpCode::kGet) {
    return linearizableGet(req.key, timeoutMs);
  }
  LogEntry e;
  {
    std::lock_guard<ProbedMutex> lock(mu_);
    if (role_ != Role::kLeader) {
      return {ClientStatus::kNotLeader, "", leaderId_};
    }

    e.index = log_.lastIndex() + 1;
    e.term = currentTerm_;
    e.op = req.op;
    e.key = req.key;
    e.value = req.value;
    e.clientId = req.clientId;
    e.requestId = req.requestId;
    // Group commit (M5): write immediately, fsync once per flush batch.
    if (!appendEntryLocked(e)) {
      return {ClientStatus::kErr, "log append failed", -1};
    }
  }

  return awaitCommit(e.index, e.term, timeoutMs);
}

// 追加一条已填好 index/term/op/payload 的条目（调用方必须持有 mu_）。
// Group commit：只 appendNoSync，fsync 由 awaitCommit 的 flusher 合并执行。
// 配置条目"追加即生效"（决策③），J2 的提交判定在 advanceCommitAndApply。
bool RaftNode::appendEntryLocked(const LogEntry& e) {
  if (!log_.appendNoSync({e})) return false;
  applyAppendedConfigLocked({e});
  return true;
}

// 等待条目提交。propose 与 changeMembership 共用（M4 评审 B6）。
ClientReply RaftNode::awaitCommit(Index index, Term term, uint64_t timeoutMs) {
  const uint64_t metricT0Us = lockprobe::nowUs();
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(timeoutMs);
  for (;;) {
    bool iAmFlusher = false;
    Index flushTarget = kNoIndex;
    {
      std::lock_guard<ProbedMutex> lock(mu_);
      // 已提交即成功：Leader 自我移除（§5.7）会在配置条目提交的同一锁段内降级，
      // 此时请求其实已经生效，不能报 NotLeader。
      if (index <= commitIndex_) {
        if (metrics_ != nullptr) {
          metrics_->onWriteCompleted(lockprobe::nowUs() - metricT0Us);
        }
        return {ClientStatus::kOk, "", -1};
      }
      if (role_ != Role::kLeader || currentTerm_ != term) {
        return {ClientStatus::kNotLeader, "", leaderId_};
      }
      if (!syncInFlight_ && syncedIndex_ < index) {
        syncInFlight_ = true;
        flushTarget = log_.lastIndex();  // captured under the lock
        iAmFlusher = true;
      }
    }

    if (iAmFlusher) {
      const Index syncedBefore = syncedIndex_;
      const uint64_t fsyncT0Us = lockprobe::nowUs();
      const bool syncOk = log_.sync();  // ONE fsync for every entry so far
      if (metrics_ != nullptr) {
        metrics_->onFsync(lockprobe::nowUs() - fsyncT0Us);
      }
      std::vector<std::pair<int, AppendEntriesArgs>> jobs;
      std::vector<std::pair<int, InstallSnapshotArgs>> snapJobs;
      {
        std::lock_guard<ProbedMutex> lock(mu_);
        if (syncOk) {
          // Only claim durability for entries that are still in the log: a
          // concurrent truncateSuffix()/compact() may have shrunk it meanwhile
          // (both leave the retained prefix durable, so clamping is safe).
          Index durable = flushTarget;
          if (durable > log_.lastIndex()) durable = log_.lastIndex();
          if (durable > syncedIndex_) {
            if (metrics_ != nullptr && durable > syncedBefore) {
              metrics_->onBatch(static_cast<size_t>(durable - syncedBefore));
            }
            syncedIndex_ = durable;
          }
        }
        // A failed fsync must never be reported as durable (I5); leave
        // syncInFlight_ clear so another proposer retries.
        syncInFlight_ = false;
        if (syncOk && role_ == Role::kLeader && currentTerm_ == term) {
          // Single-node clusters commit here (no peer jobs below).
          advanceCommitAndApply();
          for (const int peer : replicationTargetsLocked()) {
            PeerJob job = buildPeerJobLocked(peer);
            if (job.isSnapshot) {
              snapJobs.emplace_back(peer, std::move(job.snapshot));
            } else {
              jobs.emplace_back(peer, std::move(job.append));
            }
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
      for (auto& job : snapJobs) {
        transport_.sendInstallSnapshot(
            job.first, job.second,
            [this, peer = job.first](const InstallSnapshotReply& reply) {
              onInstallSnapshotReply(peer, reply);
            });
      }
      continue;
    }

    std::unique_lock<ProbedMutex> lock(mu_);
    cv_.wait_until(lock, deadline, [&] {
      return index <= commitIndex_ || role_ != Role::kLeader ||
             currentTerm_ != term;
    });
    if (index <= commitIndex_) {
      if (metrics_ != nullptr) {
        metrics_->onWriteCompleted(lockprobe::nowUs() - metricT0Us);
      }
      return {ClientStatus::kOk, "", -1};
    }
    if (role_ != Role::kLeader || currentTerm_ != term) {
      return {ClientStatus::kNotLeader, "", leaderId_};
    }
    if (std::chrono::steady_clock::now() >= deadline) {
      return {ClientStatus::kErr, "timeout waiting for commit", -1};
    }
    // Otherwise loop: if our entry is still unsynced we may become the flusher.
  }
}

Role RaftNode::role() const {
  std::lock_guard<ProbedMutex> lock(mu_);
  return role_;
}
Term RaftNode::currentTerm() const {
  std::lock_guard<ProbedMutex> lock(mu_);
  return currentTerm_;
}
int RaftNode::leaderId() const {
  std::lock_guard<ProbedMutex> lock(mu_);
  return leaderId_;
}
Index RaftNode::commitIndex() const {
  std::lock_guard<ProbedMutex> lock(mu_);
  return commitIndex_;
}
Index RaftNode::lastApplied() const {
  std::lock_guard<ProbedMutex> lock(mu_);
  return lastApplied_;
}

// ---- M3 snapshot support (stubs in Phase #2; real logic lands in M3.1+) ----

void RaftNode::maybeSnapshot() {
  std::shared_ptr<const SnapshotView> view;
  Index snapIndex = kNoIndex;
  Term snapTerm = kNoTerm;
  ClusterConfig snapConfig;  // M4.3：随快照持久化，否则安装后拓扑丢失
  uint64_t epoch = 0;

  {
    std::lock_guard<ProbedMutex> lock(mu_);
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
    // M4（评审 B3）：配置条目"追加即生效"，所以 currConfig_.version 可能大于快照
    // 边界 snapIndex。把更新的在途配置写进快照会有两种后果：重启时
    // rebuildConfigFromSeedAndLog 会从 firstIndex() 重放该条目并因版本回退直接抛
    // 异常（节点起不来），或者该条目随后被截断而快照已经把没覆盖到的新拓扑持久化
    // （拓扑泄漏）。因此按边界取配置：边界处的最后一个已生效配置。
    if (inFlightConfigIndex_ != kNoIndex && inFlightConfigIndex_ > snapIndex) {
      snapConfig = prevConfig_;  // 在途配置的 C_old 才是边界内的配置
    } else {
      snapConfig = currConfig_;  // 内存拷贝（无序列化/IO，符合 L8）
    }
    if (snapConfig.version > snapIndex) return;  // 边界覆盖不到该配置 -> 跳过本次快照
    view = sm_.snapshotView();  // memory copy only, no serialization (L8)
  }
  if (!view) return;

  // Serialize + persist OUTSIDE the lock (lock discipline L8).
  SnapshotData data;
  data.lastIncludedIndex = snapIndex;
  data.lastIncludedTerm = snapTerm;
  data.payload = view->serialize();
  data.config = encodeClusterConfig(snapConfig);  // 锁外序列化（L8）
  if (!snapshots_->save(data)) return;
  Bytes encoded = encodeSnapshotFile(data);  // outside the lock (L8)

  {
    std::lock_guard<ProbedMutex> lock(mu_);
    // B5: an InstallSnapshot that landed while we were persisting is newer and
    // must win — never move the boundary (or the log) backwards.
    if (epoch != installEpoch_) return;
    if (!(snapIndex <= commitIndex_ && snapIndex > lastIncluded_)) return;
  }
  // M5.2（I9）：compact 内含 fsync + rename + fsyncDir，必须在锁外执行。
  // B12: only advance the boundary once compaction really succeeded; a false
  // return means the on-disk log no longer matches our in-memory state.
  if (!log_.compact(snapIndex, snapTerm)) {
    throw std::runtime_error("raft: log compaction failed");
  }
  {
    std::lock_guard<ProbedMutex> lock(mu_);
    // 期间可能有更新的 InstallSnapshot 落地：那种情况下本次 compact 结果保留，
    // 但边界/快照字节让位给更新的快照（绝不回退）。
    if (epoch != installEpoch_) return;
    lastIncluded_ = snapIndex;
    lastIncludedTerm_ = snapTerm;
    if (metrics_ != nullptr) metrics_->onSnapshot(encoded.size());
    snapshotBytes_ = std::move(encoded);
    for (auto& kv : snapshotSendOffset_) kv.second = 0;  // restart peer sends
    if (snapIndex > syncedIndex_) syncedIndex_ = snapIndex;
  }
}

void RaftNode::triggerSnapshot() {
  std::lock_guard<ProbedMutex> lock(mu_);
  snapshotRequested_ = true;
}

Index RaftNode::lastIncludedIndex() const {
  std::lock_guard<ProbedMutex> lock(mu_);
  return lastIncluded_;
}

Term RaftNode::lastIncludedTerm() const {
  std::lock_guard<ProbedMutex> lock(mu_);
  return lastIncludedTerm_;
}

InstallSnapshotReply RaftNode::onInstallSnapshot(const InstallSnapshotArgs& args) {
  {
    std::lock_guard<ProbedMutex> lock(mu_);
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
    std::lock_guard<ProbedMutex> lock(mu_);
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
    // M5.2（I9）：compact（fsync + rename + fsyncDir）在锁外执行；状态机 restore 仍在
    // 锁内（状态机读路径受 mu_ 保护）。
    pendingInstallCompact_ = true;
    pendingInstallIndex_ = installed.lastIncludedIndex;
    pendingInstallTerm_ = installed.lastIncludedTerm;
  }
  if (pendingInstallCompact_) {
    // B12: compaction must succeed before the boundary is advanced.
    if (!log_.compact(pendingInstallIndex_, pendingInstallTerm_)) {
      throw std::runtime_error("raft: log compaction failed (InstallSnapshot)");
    }
    pendingInstallCompact_ = false;
  }
  {
    std::lock_guard<ProbedMutex> lock(mu_);
    if (installed.lastIncludedIndex <= lastIncluded_) {
      return {currentTerm(), true, 0};  // raced with a newer install
    }
    ++installEpoch_;  // B5: invalidates any in-flight maybeSnapshot() save
    // M4.3 / M4 评审 B4：安装快照即继承其配置。日志前缀刚被 compact，快照里的
    // 配置必须**无条件**成为新的回滚基线——哪怕 version 更小：我们日志里那条更新
    // 的配置条目已经落在边界之下、被 compact 掉了，继续保留它就是拓扑泄漏。
    // currConfig_ 随后由「基线 + 边界之上的剩余日志」重算（见 recomputeConfigLocked）。
    bool snapshotConfigAdopted = false;
    if (!installed.config.empty()) {
      ClusterConfig sc;
      if (decodeClusterConfig(installed.config.data(), installed.config.size(),
                              sc)) {
        baseConfig_ = sc;
        snapshotConfigAdopted = true;
      }
    }
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
    // 先按新基线重算配置（含在途标记/C_old/送达集合），再推进提交点
    if (snapshotConfigAdopted) recomputeConfigLocked();
    advanceCommitAndApply();
  }
  return {currentTerm(), true, 0};
}

// ---- M4 scaffolding（#2 TDD 阶段：仅为让测试可编译；真实实现见 M4.1-M4.5）----
// 启动配置重建：seed（--peers）-> 日志中的配置条目（按 index 升序）。
// J3：版本严格单调；任何回退都视为日志损坏 -> 拒绝启动。
void RaftNode::rebuildConfigFromSeedAndLog() {
  if (currConfig_.members.empty()) {  // 快照已提供配置时保持不变
    currConfig_ = seedConfig_;
  }
  if (currConfig_.members.empty()) {
    // 未提供 seed（M2/M3 风格调用点）-> 退化为 cfg_.peerIds（无地址），version = 0
    currConfig_.version = 0;
    Member self;
    self.id = cfg_.selfId;
    currConfig_.members.push_back(self);
    for (const int p : cfg_.peerIds) {
      Member m;
      m.id = p;
      currConfig_.members.push_back(m);
    }
    std::sort(currConfig_.members.begin(), currConfig_.members.end(),
              [](const Member& a, const Member& b) { return a.id < b.id; });
  }

  baseConfig_ = currConfig_;  // 回滚基线：快照携带的配置，或 seed/peerIds 退化配置
  prevConfig_ = currConfig_;
  inFlightConfigIndex_ = kNoIndex;

  const Index first = log_.firstIndex();
  const Index last = log_.lastIndex();
  if (last < first) return;
  const auto entries = log_.slice(
      first, static_cast<size_t>(last - first + 1),
      std::numeric_limits<size_t>::max());
  for (const LogEntry& e : entries) {
    if (e.op != OpCode::kConfig) continue;
    ClusterConfig c;
    if (!decodeClusterConfig(reinterpret_cast<const Byte*>(e.value.data()),
                             e.value.size(), c)) {
      throw std::runtime_error("raft: undecodable config entry at index " +
                               std::to_string(e.index));
    }
    if (c.version <= currConfig_.version) {
      throw std::runtime_error(
          "raft: config version regressed at index " +
          std::to_string(e.index) + " (have " +
          std::to_string(currConfig_.version) + ", saw " +
          std::to_string(c.version) + ")");
    }
    prevConfig_ = currConfig_;  // 本条的 C_old
    currConfig_ = c;
    // M4（评审 B4）：commitIndex 是易失状态——重启后快照边界之上的条目一律视为
    // 未确认提交，因此日志里最后一条配置条目必须重新标记为"在途"，否则 J1（一次
    // 一个）与 J2（双重多数派）在重启后同时失效，可以追加第二条配置。
    inFlightConfigIndex_ = e.index;
  }
  if (inFlightConfigIndex_ != kNoIndex) {
    // 配置条目确认送达前，被移除的节点仍是复制目标（设计 §5.2 / v1.4(a)）
    const uint64_t deadline = clock_.nowMs() + cfg_.catchUpTimeoutMs;
    for (const Member& m : prevConfig_.members) {
      if (m.id != cfg_.selfId && !currConfig_.contains(m.id)) {
        drainingPeers_[m.id] = DrainState{inFlightConfigIndex_, deadline};
      }
    }
  }
}

ClusterConfig RaftNode::clusterConfig() const {
  std::lock_guard<ProbedMutex> lock(mu_);
  return currConfig_;
}

uint64_t RaftNode::configVersion() const { return clusterConfig().version; }

// m4-design v1.2 §5.1/§5.6：未入配置或被移除 = 不竞选、不接受写，但仍接收复制
bool RaftNode::retiredLocked() const { return !currConfig_.isVoting(cfg_.selfId); }

bool RaftNode::retired() const {
  std::lock_guard<ProbedMutex> lock(mu_);
  return retiredLocked();
}

// ---- M4.2: membership helpers (m4-design v1.2 §5.3-§5.5) ---------------------

// 多数派唯一入口：self 仅当它是该配置的投票成员、且自身已持久化到 index 时计数。
bool RaftNode::hasMajorityLocked(const ClusterConfig& c, Index index) const {
  const size_t maj = c.majority();
  if (maj == 0) return false;  // 空配置没有多数派（m4-prerequisites §6.4）
  size_t replicated = 0;
  if (c.isVoting(cfg_.selfId) && index <= syncedIndex_) ++replicated;
  for (const int peer : c.votingIds()) {
    if (peer == cfg_.selfId) continue;
    const auto it = matchIndex_.find(peer);
    if (it != matchIndex_.end() && it->second >= index) ++replicated;
  }
  return replicated >= maj;
}

// 复制目标 = 当前配置成员 ∪ CatchUp 目标 ∪ 尚未确认离开的旧成员
std::vector<int> RaftNode::replicationTargetsLocked() const {
  std::vector<int> out;
  for (const Member& m : currConfig_.members) {
    if (m.id != cfg_.selfId) out.push_back(m.id);
  }
  for (const auto& kv : pendingPeers_) {
    if (kv.first != cfg_.selfId && !currConfig_.contains(kv.first)) {
      out.push_back(kv.first);
    }
  }
  for (const auto& kv : drainingPeers_) {
    const int id = kv.first;
    if (id != cfg_.selfId && !currConfig_.contains(id) &&
        pendingPeers_.count(id) == 0 &&
        std::find(out.begin(), out.end(), id) == out.end()) {
      out.push_back(id);
    }
  }
  return out;
}

void RaftNode::erasePeerStateLocked(int peerId) {
  nextIndex_.erase(peerId);
  matchIndex_.erase(peerId);
  lastSentEndIndex_.erase(peerId);
  snapshotSendOffset_.erase(peerId);
  snapshotChunkEnd_.erase(peerId);
  readAcks_.erase(peerId);  // 评审 O4：ReadIndex 应答表也要回收
}

// 已彻底离开配置的 peer（评审 O2 抽出的单一入口，adoptConfigLocked 与
// advanceCommitAndApply 共用）。
std::vector<int> RaftNode::removablePeersLocked() const {
  std::vector<int> gone;
  for (const auto& kv : nextIndex_) {
    const int id = kv.first;
    if (id == cfg_.selfId || currConfig_.contains(id)) continue;
    if (pendingPeers_.count(id) != 0) continue;      // CatchUp 目标：保留
    if (drainingPeers_.count(id) != 0) continue;     // 送达目标：确认/超时前保留
    gone.push_back(id);
  }
  return gone;
}

// 配置条目"追加即生效"（决策③）：切换 currConfig_、记录在途标记与送达集合。
void RaftNode::applyAppendedConfigLocked(const std::vector<LogEntry>& appended) {
  for (const LogEntry& e : appended) {
    if (e.op != OpCode::kConfig) continue;
    ClusterConfig c;
    if (!decodeClusterConfig(reinterpret_cast<const Byte*>(e.value.data()),
                             e.value.size(), c)) {
      continue;  // 解码失败：忽略（启动期才 fatal，见 rebuildConfigFromSeedAndLog）
    }
    if (c.version <= currConfig_.version) continue;  // J3：更小版本不得生效

    adoptConfigLocked(c, e.index, /*computeDraining=*/true);
  }
}

// 切换配置后的统一收尾（配置条目追加 / 安装快照 共用）：
// 清理 CatchUp 登记、计算送达集合、注册新地址（锁外）、清理彻底离开的节点状态。
void RaftNode::adoptConfigLocked(const ClusterConfig& sc, Index inFlightIndex,
                                 bool computeDraining) {
  const ClusterConfig old = currConfig_;
  prevConfig_ = old;
  currConfig_ = sc;
  inFlightConfigIndex_ = inFlightIndex;

  for (auto it = pendingPeers_.begin(); it != pendingPeers_.end();) {
    if (currConfig_.contains(it->first)) {
      it = pendingPeers_.erase(it);
    } else {
      ++it;
    }
  }
  drainingPeers_.clear();
  if (computeDraining) {  // 被移除的节点：确认收到该配置条目之前仍要送达
    const uint64_t deadline = clock_.nowMs() + cfg_.catchUpTimeoutMs;
    for (const Member& m : old.members) {
      if (m.id != cfg_.selfId && !currConfig_.contains(m.id)) {
        drainingPeers_[m.id] = DrainState{inFlightIndex, deadline};
      }
    }
  }
  for (const Member& m : currConfig_.members) {  // 新成员地址：出锁后注册（L10）
    if (m.id != cfg_.selfId && !m.addr.empty()) {
      peerAddQueue_.emplace_back(m.id, m.addr);
    }
  }
  const std::vector<int> gone = removablePeersLocked();
  for (const int id : gone) {
    erasePeerStateLocked(id);
    peerRemoveQueue_.push_back(id);
  }
}

// 配置回滚（设计 §5.2）：日志后缀被截断后，把配置退回
// "已持久基线（快照 / 已提交配置）+ 剩余日志中的配置条目" 重算的结果。
void RaftNode::recomputeConfigLocked() {
  currConfig_ = baseConfig_;
  prevConfig_ = currConfig_;
  inFlightConfigIndex_ = kNoIndex;


  const Index first = log_.firstIndex();
  const Index last = log_.lastIndex();
  if (last >= first) {
    const auto entries = log_.slice(
        first, static_cast<size_t>(last - first + 1),
        std::numeric_limits<size_t>::max());
    for (const LogEntry& e : entries) {
      if (e.op != OpCode::kConfig) continue;
      ClusterConfig c;
      if (!decodeClusterConfig(reinterpret_cast<const Byte*>(e.value.data()),
                               e.value.size(), c)) {
        continue;
      }
      if (c.version <= currConfig_.version) continue;  // J3：只推进
      // 评审 O3：C_old 必须是"本条配置之前的那个配置"，否则 J2 会拿错误的旧配置
      // 做多数派判定（日志里有多条配置条目时尤其明显）。
      prevConfig_ = currConfig_;
      currConfig_ = c;
      inFlightConfigIndex_ = e.index;  // 保守：视为仍未提交
    }
  }
  // 评审 O3：送达集合必须一并重算——若截断后仍有在途的 remove，被移除的节点在
  // 配置条目提交前仍然要收到复制，否则它可能永远学不到"自己被移除"。
  drainingPeers_.clear();
  if (inFlightConfigIndex_ != kNoIndex) {
    const uint64_t deadline = clock_.nowMs() + cfg_.catchUpTimeoutMs;
    for (const Member& m : prevConfig_.members) {
      if (m.id != cfg_.selfId && !currConfig_.contains(m.id)) {
        drainingPeers_[m.id] = DrainState{inFlightConfigIndex_, deadline};
      }
    }
  }
  // 回滚后重新属于配置的成员：地址重新登记（锁外执行，L10）
  for (const Member& m : currConfig_.members) {
    if (m.id != cfg_.selfId && !m.addr.empty()) {
      peerAddQueue_.emplace_back(m.id, m.addr);
    }
  }
  for (auto it = pendingPeers_.begin(); it != pendingPeers_.end();) {
    if (currConfig_.contains(it->first)) {
      it = pendingPeers_.erase(it);
    } else {
      ++it;
    }
  }
}

// 给某个 peer 的复制作业：落后于快照边界 -> 一个快照块；否则 AppendEntries。
// tick / propose / CatchUp 共用（M3 评审 B13：组提交路径不得绕过快照路由）。
RaftNode::PeerJob RaftNode::buildPeerJobLocked(int peer) {
  PeerJob job;
  if (lastIncluded_ != kNoIndex && !snapshotBytes_.empty() &&
      nextIndex_[peer] <= lastIncluded_) {
    const uint64_t total = snapshotBytes_.size();
    uint64_t start = snapshotSendOffset_[peer];
    if (start >= total) start = 0;
    const uint64_t remaining = total - start;
    const uint64_t take = std::min<uint64_t>(cfg_.snapshotChunkBytes, remaining);
    job.isSnapshot = true;
    job.snapshot.term = currentTerm_;
    job.snapshot.leaderId = cfg_.selfId;
    job.snapshot.lastIncludedIndex = lastIncluded_;
    job.snapshot.lastIncludedTerm = lastIncludedTerm_;
    job.snapshot.offset = start;
    job.snapshot.done = (start + take >= total);
    job.snapshot.data.assign(snapshotBytes_.begin() + start,
                             snapshotBytes_.begin() + start + take);
    snapshotChunkEnd_[peer] = start + take;
  } else {
    job.append = buildAppendEntries(peer);
  }
  return job;
}

// L10：地址簿更新只在锁外执行
void RaftNode::drainPeerQueues() {
  std::vector<std::pair<int, std::string>> adds;
  std::vector<int> removes;
  {
    std::lock_guard<ProbedMutex> lock(mu_);
    adds.swap(peerAddQueue_);
    removes.swap(peerRemoveQueue_);
  }
  for (const auto& a : adds) transport_.addPeer(a.first, a.second);
  for (const int id : removes) transport_.removePeer(id);
}

// 送达目标收尾：已经确认收到移除条目的、或超出送达预算的，移出复制目标并回收
// 每 peer 状态（评审 O2 + 设计 v1.4(a)）。Leader 每个 tick 调用一次（持锁）。
void RaftNode::purgeDrainsLocked() {
  if (drainingPeers_.empty()) return;
  bool changed = false;
  for (auto it = drainingPeers_.begin(); it != drainingPeers_.end();) {
    const auto mit = matchIndex_.find(it->first);
    const bool acked = mit != matchIndex_.end() && mit->second >= it->second.until;
    if (acked || clock_.nowMs() >= it->second.deadlineMs) {
      it = drainingPeers_.erase(it);
      changed = true;
    } else {
      ++it;
    }
  }
  if (!changed) return;
  for (const int id : removablePeersLocked()) {
    erasePeerStateLocked(id);
    peerRemoveQueue_.push_back(id);  // 锁外摘除（L10）
  }
}

// add 的前置：把新节点追平到"拥有本节点全部日志"（非投票、不计多数派）。
bool RaftNode::catchUpPeer(int peerId, uint64_t timeoutMs) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
  for (;;) {
    PeerJob job;
    {
      std::lock_guard<ProbedMutex> lock(mu_);
      if (role_ != Role::kLeader) return false;
      // 设计 §5.4 步骤 5（冻结判据）：matchIndex >= commitIndex **且** 本任期
      // 至少应答过一次 AppendEntries（评审 O7：原实现用的是 log_.lastIndex()，
      // 在持续写入/组提交下会把"追平"判得比设计更严，add 可能反复超时）。
      const auto mit = matchIndex_.find(peerId);
      const auto ait = ackedTerm_.find(peerId);
      if (mit != matchIndex_.end() && mit->second >= commitIndex_ &&
          ait != ackedTerm_.end() && ait->second == currentTerm_) {
        return true;  // 已追平
      }
      job = buildPeerJobLocked(peerId);
    }
    if (job.isSnapshot) {
      transport_.sendInstallSnapshot(
          peerId, job.snapshot,
          [this, peerId](const InstallSnapshotReply& reply) {
            onInstallSnapshotReply(peerId, reply);
          });
    } else {
      transport_.sendAppendEntries(
          peerId, job.append,
          [this, peerId](const AppendEntriesReply& reply) {
            onAppendEntriesReply(peerId, reply);
          });
    }
    if (std::chrono::steady_clock::now() >= deadline) return false;
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
}

// 成员变更：仅 Leader；一次一个（J1）；add 先 CatchUp 再追加配置条目，remove 直接追加。
// 配置条目走普通 propose 路径（组提交 + 复制 + J2 双重多数派提交）。
ClientReply RaftNode::changeMembership(MembershipOp op, int targetId,
                                       const std::string& targetAddr,
                                       uint64_t timeoutMs) {
  const bool add = (op == MembershipOp::kAdd);
  // M4 评审 B6：整个成员变更（校验 -> CatchUp -> 追加配置条目 -> 等提交）串行化。
  // 并发的第二次变更立即被拒绝——不做无谓的 CatchUp，也不可能"两个变更同时进入"。
  std::unique_lock<MembershipMutex> changeLock(membershipMu_, std::try_to_lock);
  if (!changeLock.owns_lock()) {
    return {ClientStatus::kErr, "membership change already in progress", -1};
  }
  {
    std::lock_guard<ProbedMutex> lock(mu_);
    if (role_ != Role::kLeader) return {ClientStatus::kNotLeader, "", leaderId_};
    if (retiredLocked()) return {ClientStatus::kNotLeader, "", leaderId_};
    if (targetId <= 0) return {ClientStatus::kErr, "invalid target id", -1};
    // add self 无意义；remove self 合法（§5.7 Leader 自我移除）
    if (add && targetId == cfg_.selfId) {
      return {ClientStatus::kErr, "cannot add self", -1};
    }
    if (inFlightConfigIndex_ != kNoIndex) {
      return {ClientStatus::kErr, "membership change already in flight", -1};
    }
    if (add) {
      if (currConfig_.contains(targetId) || pendingPeers_.count(targetId) != 0) {
        return {ClientStatus::kErr, "already a member", -1};
      }
      if (targetAddr.empty()) {
        return {ClientStatus::kErr, "address required for add", -1};
      }
      if (targetAddr.size() > 0xFFFFu) {  // 评审 O8：wire 的 addrLen 只有 16 位
        return {ClientStatus::kErr, "address too long", -1};
      }
      pendingPeers_[targetId] = targetAddr;
      peerAddQueue_.emplace_back(targetId, targetAddr);
      nextIndex_[targetId] = log_.lastIndex() + 1;
      matchIndex_[targetId] = kNoIndex;
      ackedTerm_.erase(targetId);  // 追平判据要求"本任期"的新应答
      lastSentEndIndex_[targetId] = kNoIndex;
      snapshotSendOffset_[targetId] = 0;
      snapshotChunkEnd_[targetId] = 0;
    } else if (!currConfig_.contains(targetId)) {
      return {ClientStatus::kErr, "not a member", -1};
    }
  }
  drainPeerQueues();

  if (add) {
    // 追平可能要多轮 RPC（大日志），预算取调用方超时与 catchUpTimeoutMs 的较大者
    const uint64_t budget =
        std::max<uint64_t>(timeoutMs, cfg_.catchUpTimeoutMs);
    if (!catchUpPeer(targetId, budget)) {
    {
      std::lock_guard<ProbedMutex> lock(mu_);
      pendingPeers_.erase(targetId);
      erasePeerStateLocked(targetId);
      peerRemoveQueue_.push_back(targetId);
    }
      drainPeerQueues();
      return {ClientStatus::kErr, "catch-up failed or timed out", -1};
    }
  }

  // M4 评审 B6：条目 index/term 与配置 version 必须在同一个 mu_ 临界区内确定并立刻
  // 追加。旧实现先算出 next.version = lastIndex()+1、解锁后再走 propose()，两次加锁
  // 之间可能被别的客户端 PUT 插队，导致 version != 条目 index（设计不变量），进而让
  // maybeSnapshot 的边界保护与重启期的 J3 校验同时失真。
  LogEntry ce;
  {
    std::lock_guard<ProbedMutex> lock(mu_);
    if (role_ != Role::kLeader) return {ClientStatus::kNotLeader, "", leaderId_};
    if (inFlightConfigIndex_ != kNoIndex) {
      return {ClientStatus::kErr, "membership change already in flight", -1};
    }
    ClusterConfig next = currConfig_;
    if (add) {
      Member m;
      m.id = targetId;
      const auto it = pendingPeers_.find(targetId);
      m.addr = (it != pendingPeers_.end()) ? it->second : targetAddr;
      m.voting = true;
      next.members.push_back(m);
    } else {
      next.members.erase(std::remove_if(next.members.begin(), next.members.end(),
                                        [targetId](const Member& m) {
                                          return m.id == targetId;
                                        }),
                         next.members.end());
    }
    std::sort(next.members.begin(), next.members.end(),
              [](const Member& a, const Member& b) { return a.id < b.id; });

    ce.index = log_.lastIndex() + 1;
    ce.term = currentTerm_;
    next.version = ce.index;  // 设计：version == 产生它的配置条目 index
    ce.op = OpCode::kConfig;
    const Bytes payload = encodeClusterConfig(next);
    ce.value.assign(reinterpret_cast<const char*>(payload.data()),
                    payload.size());
    ce.clientId = 0;
    ce.requestId = 0;
    // 追加即生效（决策③）；J2 的提交判定在 advanceCommitAndApply
    if (!appendEntryLocked(ce)) {
      return {ClientStatus::kErr, "log append failed", -1};
    }
  }
  drainPeerQueues();  // 新成员地址 / 离开节点的清理在锁外执行（L10）

  const ClientReply reply = awaitCommit(ce.index, ce.term, timeoutMs);
  if (reply.status == ClientStatus::kOk && metrics_ != nullptr) {
    metrics_->onConfigChange();
  }
  if (reply.status != ClientStatus::kOk && add) {
    std::lock_guard<ProbedMutex> lock(mu_);
    pendingPeers_.erase(targetId);
  }
  return reply;
}

// ReadIndex 多数派判定：投票成员中已确认">= seq"的数量（含 self）。
bool RaftNode::readQuorumLocked(uint64_t seq) const {
  const size_t maj = currConfig_.majority();
  if (maj == 0) return false;
  size_t acks = 0;
  for (const int peer : currConfig_.votingIds()) {
    const auto it = readAcks_.find(peer);
    if (it != readAcks_.end() && it->second >= seq) ++acks;
  }
  if (acks < maj) return false;
  // 设计 §5.8（评审 O1）：变更在途时读探针同 J2 双重判定——C_old 多数派也必须
  // 确认过本任期领导权，否则未来放宽"一次一个"时会留下线性一致读的口子。
  if (inFlightConfigIndex_ != kNoIndex) {
    const size_t oldMaj = prevConfig_.majority();
    if (oldMaj == 0) return false;
    size_t oldAcks = 0;
    for (const int peer : prevConfig_.votingIds()) {
      const auto it = readAcks_.find(peer);
      if (it != readAcks_.end() && it->second >= seq) ++oldAcks;
    }
    if (oldAcks < oldMaj) return false;
  }
  return true;
}

// M4 评审 B5（§8 读屏障）：commitIndex_ 处的条目属于当前任期，才说明本 Leader
// 已经提交过自己任期的条目，commitIndex_ 才覆盖了"上一任 Leader 已提交的全部
// 条目"。否则新 Leader 的 commitIndex_ 可能落后于真实提交点，按它做 ReadIndex
// 会读到陈旧值（旧值 / NOT_FOUND）。
bool RaftNode::hasCurrentTermCommitLocked() const {
  return commitIndex_ != kNoIndex && log_.termAt(commitIndex_) == currentTerm_;
}

// 线性一致读（m4-design v1.2 §5.8）：记录 readIndex -> quorum 探针确认同任期领导权
// -> 等 lastApplied >= readIndex -> 读状态机。任一步失败都返回错误，绝不返回可能陈旧的值。
ClientReply RaftNode::linearizableGet(const std::string& key,
                                      uint64_t timeoutMs) {
  const auto start = std::chrono::steady_clock::now();
  const auto deadline = start + std::chrono::milliseconds(timeoutMs);
  // 探针阶段预算：设计 §4.3 的 readIndexTimeoutMs（默认 500ms），但绝不放宽
  // 调用方给的总预算（评审 O6：该配置项此前完全没被使用）。
  const auto probeDeadline =
      start + std::chrono::milliseconds(
                  std::min<uint64_t>(timeoutMs, cfg_.readIndexTimeoutMs));
  Index readIndex = kNoIndex;
  Term term = kNoTerm;
  uint64_t seq = 0;
  std::vector<std::pair<int, ReadProbeArgs>> jobs;
  {
    std::unique_lock<ProbedMutex> lock(mu_);
    if (role_ != Role::kLeader) return {ClientStatus::kNotLeader, "", leaderId_};
    const Term term0 = currentTerm_;
    // §8 屏障（评审 B5）：先等到"本任期有条目已提交"，否则 readIndex 可能是
    // 陈旧提交点。生产默认 appendNoop=true，tick() 会补一条本任期 no-op 并提交。
    if (!hasCurrentTermCommitLocked()) {
      cv_.wait_until(lock, probeDeadline, [&] {
        return hasCurrentTermCommitLocked() || role_ != Role::kLeader ||
               currentTerm_ != term0;
      });
    }
    if (role_ != Role::kLeader || currentTerm_ != term0) {
      return {ClientStatus::kNotLeader, "", leaderId_};
    }
    if (!hasCurrentTermCommitLocked()) {
      // 宁可失败也不返回可能陈旧的值
      return {ClientStatus::kErr, "read index timeout (no current-term commit)",
              -1};
    }
    readIndex = commitIndex_;
    term = currentTerm_;
    seq = ++readSeq_;
    if (currConfig_.isVoting(cfg_.selfId)) readAcks_[cfg_.selfId] = seq;
    for (const int peer : currConfig_.votingIds()) {
      if (peer == cfg_.selfId) continue;
      ReadProbeArgs a;
      a.term = currentTerm_;
      a.leaderId = cfg_.selfId;
      a.seq = seq;
      jobs.emplace_back(peer, a);
    }
  }

  for (auto& j : jobs) {  // 锁外发送（L2）
    transport_.sendReadProbe(
        j.first, j.second,
        [this, peer = j.first](const ReadProbeReply& reply) {
          onReadProbeReply(peer, reply);
        });
  }

  std::unique_lock<ProbedMutex> lock(mu_);
  if (!readQuorumLocked(seq)) {
    cv_.wait_until(lock, probeDeadline, [&] {
      return readQuorumLocked(seq) || role_ != Role::kLeader ||
             currentTerm_ != term;
    });
  }
  if (role_ != Role::kLeader || currentTerm_ != term) {
    return {ClientStatus::kNotLeader, "", leaderId_};
  }
  if (!readQuorumLocked(seq)) {
    return {ClientStatus::kErr, "read index timeout (no quorum)", -1};
  }
  if (lastApplied_ < readIndex) {
    cv_.wait_until(lock, deadline, [&] {
      return lastApplied_ >= readIndex || role_ != Role::kLeader ||
             currentTerm_ != term;
    });
  }
  if (role_ != Role::kLeader || currentTerm_ != term) {
    return {ClientStatus::kNotLeader, "", leaderId_};
  }
  if (lastApplied_ < readIndex) {
    return {ClientStatus::kErr, "read index timeout (apply lag)", -1};
  }
  std::string out;
  if (sm_.get(key, out)) return {ClientStatus::kOk, out, -1};
  return {ClientStatus::kNotFound, "", -1};
}

// 探针接收侧：承认同任期领导权（等价于一次心跳），回带 seq。
ReadProbeReply RaftNode::onReadProbe(const ReadProbeArgs& args) {
  std::lock_guard<ProbedMutex> lock(mu_);
  if (args.term < currentTerm_) return {currentTerm_, false, args.seq};
  if (args.term > currentTerm_) {
    becomeFollower(args.term);
  } else if (role_ == Role::kCandidate) {
    role_ = Role::kFollower;
  }
  leaderId_ = args.leaderId;
  lastHeartbeatMs_ = clock_.nowMs();
  return {currentTerm_, true, args.seq};
}

void RaftNode::onReadProbeReply(int peerId, const ReadProbeReply& reply) {
  std::lock_guard<ProbedMutex> lock(mu_);
  if (reply.term < currentTerm_) return;  // 陈旧回包
  if (reply.term > currentTerm_) {
    becomeFollower(reply.term);
    return;
  }
  if (!reply.ok || !currConfig_.isVoting(peerId)) return;
  const auto it = readAcks_.find(peerId);
  if (it == readAcks_.end() || it->second < reply.seq) {
    readAcks_[peerId] = reply.seq;  // 单调：更大的 seq 覆盖
  }
  cv_.notify_all();
}

void RaftNode::onInstallSnapshotReply(int peerId,
                                      const InstallSnapshotReply& reply) {
  std::lock_guard<ProbedMutex> lock(mu_);
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
