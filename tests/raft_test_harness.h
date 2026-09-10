#pragma once

#include <memory>
#include <vector>

#include "kv/kv_state_machine.h"
#include "raft/clock.h"
#include "raft/log_store.h"
#include "raft/raft_node.h"
#include "raft/transport.h"

namespace raftkv::raft::test {

struct TestNode {
  std::unique_ptr<MemoryLogStore> log;
  std::unique_ptr<StateMachine> sm;
  std::unique_ptr<RaftNode> node;
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
