#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "kv/kv_state_machine.h"
#include "raft/cluster_config.h"
#include "raft/clock.h"
#include "raft/lock_probe.h"
#include "raft/log_store.h"
#include "raft/raft_node.h"
#include "raft/snapshot_store.h"
#include "raft/transport.h"

namespace raftkv::raft::test {

struct TestNode {
  std::unique_ptr<MemoryLogStore> log;
  std::unique_ptr<StateMachine> sm;
  std::unique_ptr<RaftNode> node;
  // M3 (additive): nullptr for M2 clusters, set for snapshot clusters.
  std::unique_ptr<MemorySnapshotStore> snapshots;
};

struct Cluster {
  std::shared_ptr<FakeClock> clock;
  std::shared_ptr<MemoryTransport> transport;
  std::vector<TestNode> nodes;
};

// Builds an n-node in-memory cluster sharing one FakeClock and one
// MemoryTransport. Each node owns its own MemoryLogStore + KvStateMachine.
inline std::shared_ptr<Cluster> makeCluster(int n, bool appendNoop = true) {
  auto c = std::make_shared<Cluster>();
  c->clock = std::make_shared<FakeClock>();
  c->transport = std::make_shared<MemoryTransport>();
  c->nodes.resize(static_cast<size_t>(n));
  for (int id = 1; id <= n; ++id) {
    TestNode tn;
    tn.log = std::make_unique<MemoryLogStore>();
    tn.sm = std::make_unique<raftkv::KvStateMachine>();
    RaftConfig cfg;
    cfg.selfId = id;
    cfg.appendNoop = appendNoop;
    for (int p = 1; p <= n; ++p) {
      if (p != id) cfg.peerIds.push_back(p);
    }
    tn.node = std::make_unique<RaftNode>(cfg, *tn.log, *tn.sm, *c->transport,
                                         *c->clock);
    c->transport->addNode(id, tn.node.get());
    c->nodes[static_cast<size_t>(id - 1)] = std::move(tn);
  }
  return c;
}

// Additive (M3 review): snapshot-enabled cluster with a caller-supplied
// transport, so a test can inject dropped replies / one-way partitions that
// plain MemoryTransport cannot express.
inline std::shared_ptr<Cluster> makeSnapshotClusterWith(
    int n, size_t threshold, size_t chunkBytes,
    std::shared_ptr<MemoryTransport> transport) {
  auto c = std::make_shared<Cluster>();
  c->clock = std::make_shared<FakeClock>();
  c->transport = std::move(transport);
  c->nodes.resize(static_cast<size_t>(n));
  for (int id = 1; id <= n; ++id) {
    TestNode tn;
    tn.log = std::make_unique<MemoryLogStore>();
    tn.sm = std::make_unique<raftkv::KvStateMachine>();
    tn.snapshots = std::make_unique<MemorySnapshotStore>();
    RaftConfig cfg;
    cfg.selfId = id;
    cfg.snapshotThresholdEntries = threshold;
    cfg.snapshotChunkBytes = chunkBytes;
    for (int p = 1; p <= n; ++p) {
      if (p != id) cfg.peerIds.push_back(p);
    }
    tn.node = std::make_unique<RaftNode>(cfg, *tn.log, *tn.sm, *c->transport,
                                         *c->clock, tn.snapshots.get());
    c->transport->addNode(id, tn.node.get());
    c->nodes[static_cast<size_t>(id - 1)] = std::move(tn);
  }
  return c;
}

// Additive (M3): snapshot-enabled cluster. Each node owns a MemorySnapshotStore
// and is configured with the given threshold/chunk size. Existing makeCluster()
// and all M2 test cases are untouched.
inline std::shared_ptr<Cluster> makeSnapshotCluster(int n, size_t threshold,
                                                    size_t chunkBytes = 64) {
  return makeSnapshotClusterWith(
      n, threshold, chunkBytes, std::make_shared<MemoryTransport>());
}

// ---- M4 (additive): membership-enabled clusters ----
// seed 配置：n 个投票成员，地址形如 127.0.0.1:700<id>（version = 0）。
inline ClusterConfig makeSeedConfig(int n) {
  ClusterConfig s;
  s.version = 0;
  for (int id = 1; id <= n; ++id) {
    Member m;
    m.id = id;
    m.addr = "127.0.0.1:700" + std::to_string(id);
    m.voting = true;
    s.members.push_back(m);
  }
  return s;
}

// 成员变更测试用集群：带 SnapshotStore（快照携带配置）、可指定 appendNoop 与阈值。
inline std::shared_ptr<Cluster> makeMembershipClusterWith(
    int n, bool appendNoop, size_t threshold, size_t chunkBytes,
    std::shared_ptr<MemoryTransport> transport) {
  auto c = std::make_shared<Cluster>();
  c->clock = std::make_shared<FakeClock>();
  c->transport = std::move(transport);
  c->nodes.resize(static_cast<size_t>(n));
  const ClusterConfig seed = makeSeedConfig(n);
  for (int id = 1; id <= n; ++id) {
    TestNode tn;
    tn.log = std::make_unique<MemoryLogStore>();
    tn.sm = std::make_unique<raftkv::KvStateMachine>();
    tn.snapshots = std::make_unique<MemorySnapshotStore>();
    RaftConfig cfg;
    cfg.selfId = id;
    cfg.appendNoop = appendNoop;
    cfg.snapshotThresholdEntries = threshold;
    cfg.snapshotChunkBytes = chunkBytes;
    for (const Member& m : seed.members) {
      if (m.id != id) cfg.peerIds.push_back(m.id);
    }
    tn.node = std::make_unique<RaftNode>(cfg, *tn.log, *tn.sm, *c->transport,
                                        *c->clock, tn.snapshots.get(), seed);
    c->transport->addNode(id, tn.node.get());
    c->nodes[static_cast<size_t>(id - 1)] = std::move(tn);
  }
  return c;
}

inline std::shared_ptr<Cluster> makeMembershipCluster(
    int n, bool appendNoop = true, size_t threshold = 1000000) {
  return makeMembershipClusterWith(n, appendNoop, threshold, 64,
                                   std::make_shared<MemoryTransport>());
}

// 只驱动指定节点：隔离中的节点若继续 tick 会竞选，抬高任期并逼 Leader 下台。
inline void tickNodesOnly(Cluster& c, const std::vector<int>& ids, int count,
                     uint64_t stepMs) {
  for (int i = 0; i < count; ++i) {
    c.clock->advance(stepMs);
    for (const int id : ids) c.nodes[static_cast<size_t>(id - 1)].node->tick();
  }
}

inline RaftNode* findLeader(const Cluster& c) {
  for (const auto& n : c.nodes) {
    if (n.node->role() == Role::kLeader) return n.node.get();
  }
  return nullptr;
}

inline void driveTicks(Cluster& c, int count, uint64_t stepMs) {
  for (int i = 0; i < count; ++i) {
    c.clock->advance(stepMs);
    for (auto& n : c.nodes) n.node->tick();
  }
}

// Spy that counts persistMeta() calls (asserts vote durability ordering).
class SpyMemoryLogStore : public MemoryLogStore {
 public:
  bool persistMeta(Term term, int votedFor) override {
    ++persistMetaCalls;
    return MemoryLogStore::persistMeta(term, votedFor);
  }
  int persistMetaCallCount() const { return persistMetaCalls; }

 private:
  int persistMetaCalls = 0;
};


// ============================ M5 新增（仅追加，不改既有桩） ============================

// SpyLogStore：记录"持 Raft 锁期间"发生的 durable 调用（I9 的断言载体）。
// MemoryLogStore 语义完全不变（M2/M3/M4 用例零影响）。
class SpyLogStore : public MemoryLogStore {
 public:
  bool append(const std::vector<LogEntry>& entries) override {
    if (lockprobe::consensusHeld()) ++lockedAppends;
    ++appends;
    return MemoryLogStore::append(entries);
  }
  bool sync() override {
    if (lockprobe::consensusHeld()) ++lockedSyncs;
    ++syncs;
    return MemoryLogStore::sync();
  }
  bool persistMeta(Term term, int votedFor) override {
    if (lockprobe::consensusHeld()) ++lockedMetaPersists;
    ++metaPersists;
    return MemoryLogStore::persistMeta(term, votedFor);
  }
  bool compact(Index upTo, Term termAtUpTo) override {
    if (lockprobe::consensusHeld()) ++lockedCompacts;
    ++compacts;
    return MemoryLogStore::compact(upTo, termAtUpTo);
  }
  bool appendNoSync(const std::vector<LogEntry>& entries) override {
    if (lockprobe::consensusHeld()) ++lockedAppendNoSyncs;  // 允许（I9：锁内只 write）
    return MemoryLogStore::appendNoSync(entries);
  }
  // M5.2 评审 B1：含 fsync 的截断不得在持 mu_ 时发生；锁内只允许 no-sync 变体
  bool truncateSuffix(Index fromIndex) override {
    if (lockprobe::consensusHeld()) ++lockedTruncates;
    ++truncates;
    return MemoryLogStore::truncateSuffix(fromIndex);
  }
  bool truncateSuffixNoSync(Index fromIndex) override {
    if (lockprobe::consensusHeld()) ++lockedTruncateNoSyncs;
    ++noSyncTruncates;  // 无论是否持锁，都记录 no-sync 变体确实被用过
    return MemoryLogStore::truncateSuffixNoSync(fromIndex);
  }

  int lockedDurableCalls() const {
    return lockedAppends + lockedSyncs + lockedMetaPersists + lockedCompacts +
           lockedTruncates;  // 含 fsync 的截断同样算 durable 调用
  }
  int appends = 0, syncs = 0, metaPersists = 0, compacts = 0, truncates = 0,
      noSyncTruncates = 0;
  int lockedAppends = 0, lockedSyncs = 0, lockedMetaPersists = 0,
      lockedCompacts = 0, lockedAppendNoSyncs = 0, lockedTruncates = 0,
      lockedTruncateNoSyncs = 0;
};

// BlockingLogStore：在 sync()/persistMeta() 上升起"已进入"信号并阻塞，直到测试放行。
// 用于断言"锁外落盘"（进入阻塞点时必须没有持锁）。
class BlockingLogStore : public MemoryLogStore {
 public:
  void blockOnSync(bool block) { blockSync_.store(block); }
  void blockOnMeta(bool block) { blockMeta_.store(block); }

  // 说明：刻意**不用 mutex/condition_variable** —— 该桩工作在"节点线程里阻塞、测试线程放行"
  // 的模式下，用 mutex+cv 会被 TSan 报 double-lock/data-race（都是桩自身的簿记问题，不是
  // 被测代码）。全部字段用原子 + 1ms 轮询，语义不变（测试本就带 2-3s 超时）。
  bool sync() override {
    if (blockSync_.load()) {
      inSync_.store(true);
      holdWhileLocked_.store(lockprobe::consensusHeld());
      while (!released_.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
      inSync_.store(false);
    }
    return MemoryLogStore::sync();
  }

  bool persistMeta(Term term, int votedFor) override {
    if (blockMeta_.load()) {
      inMeta_.store(true);
      holdWhileMetaLocked_.store(lockprobe::consensusHeld());
      while (!released_.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
      inMeta_.store(false);
    }
    return MemoryLogStore::persistMeta(term, votedFor);
  }

  bool waitInsideSync(uint64_t timeoutMs) {
    for (uint64_t waited = 0; waited < timeoutMs; waited += 2) {
      if (inSync_.load()) return true;
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return inSync_.load();
  }
  bool waitInsideMeta(uint64_t timeoutMs) {
    for (uint64_t waited = 0; waited < timeoutMs; waited += 2) {
      if (inMeta_.load()) return true;
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return inMeta_.load();
  }
  void release() { released_.store(true); }  // 粘性：放行后不再阻塞
  bool heldWhileSyncEntered() const { return holdWhileLocked_.load(); }
  bool heldWhileMetaEntered() const { return holdWhileMetaLocked_.load(); }

 private:
  std::atomic<bool> blockSync_{false};
  std::atomic<bool> blockMeta_{false};
  std::atomic<bool> inSync_{false};
  std::atomic<bool> inMeta_{false};
  std::atomic<bool> released_{false};
  std::atomic<bool> holdWhileLocked_{false};
  std::atomic<bool> holdWhileMetaLocked_{false};
};

// SpyTransport：记录"持 Raft 锁期间"发生的网络发送（I9 的第二半）。
class SpyTransport : public MemoryTransport {
 public:
  void sendRequestVote(int peerId, const RequestVoteArgs& a, VoteCb cb) override {
    ++sends;
    if (lockprobe::consensusHeld()) ++lockedSends;
    MemoryTransport::sendRequestVote(peerId, a, std::move(cb));
  }
  void sendAppendEntries(int peerId, const AppendEntriesArgs& a,
                         AppendCb cb) override {
    ++sends;
    if (lockprobe::consensusHeld()) ++lockedSends;
    MemoryTransport::sendAppendEntries(peerId, a, std::move(cb));
  }
  void sendInstallSnapshot(int peerId, const InstallSnapshotArgs& a,
                           InstallCb cb) override {
    ++sends;
    if (lockprobe::consensusHeld()) ++lockedSends;
    MemoryTransport::sendInstallSnapshot(peerId, a, std::move(cb));
  }
  void sendReadProbe(int peerId, const ReadProbeArgs& a,
                     ReadProbeCb cb) override {
    ++sends;
    if (lockprobe::consensusHeld()) ++lockedSends;
    MemoryTransport::sendReadProbe(peerId, a, std::move(cb));
  }
  int sends = 0;
  int lockedSends = 0;
};

}  // namespace raftkv::raft::test
