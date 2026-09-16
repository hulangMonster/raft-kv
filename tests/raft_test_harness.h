#pragma once

#include <memory>
#include <vector>

#include "kv/kv_state_machine.h"
#include "raft/cluster_config.h"
#include "raft/clock.h"
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

}  // namespace raftkv::raft::test
