// M4 (membership + linearizable read) tests — #2 TDD 阶段：全部用例预期失败（RED）。
//
// A 组：确定性（FakeClock + MemoryTransport + MemoryLogStore + MemorySnapshotStore，无磁盘/网络）
// B 组：真实磁盘（FileLogStore + FileSnapshotStore + 临时目录）
//
// 用例编号与 docs/m4-design.md §8 / docs/m4-prerequisites.md §8 一一对应。
// 注意：B4 是 "RKS1 v1 兼容守门" 用例，从 #2 起即通过（M4 不得破坏 v1 解码）。
#include <gtest/gtest.h>

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include <unistd.h>

#include "kv/kv_state_machine.h"
#include "raft/cluster_config.h"
#include "raft/message.h"
#include "raft/snapshot_store.h"
#include "raft_test_harness.h"

using namespace raftkv;
using namespace raftkv::raft;

namespace {

ClientRequest putReq(uint64_t requestId, const std::string& key,
                     const std::string& value) {
  ClientRequest r;
  r.op = OpCode::kPut;
  r.key = key;
  r.value = value;
  r.clientId = 1;
  r.requestId = requestId;
  return r;
}

std::string tempDir() {
  static uint64_t counter = 0;
  auto p = std::filesystem::temp_directory_path() /
           ("raftkv_m4_test_" + std::to_string(::getpid()) + "_" +
            std::to_string(counter++));
  std::filesystem::create_directories(p);
  return p.string();
}

std::string toStr(const Bytes& b) {
  return std::string(reinterpret_cast<const char*>(b.data()), b.size());
}

// 配置条目：op=kConfig，value = encodeClusterConfig(payload)
LogEntry configEntry(Index index, Term term, uint64_t version,
                     const std::vector<Member>& members) {
  LogEntry e;
  e.index = index;
  e.term = term;
  e.op = OpCode::kConfig;
  ClusterConfig c;
  c.version = version;
  c.members = members;
  e.value = toStr(encodeClusterConfig(c));
  return e;
}

Index configEntryIndex(test::TestNode& tn) {
  const auto all = tn.log->slice(tn.log->firstIndex(), 100000, 1u << 30);
  for (const LogEntry& e : all) {
    if (e.op == OpCode::kConfig) return e.index;
  }
  return kNoIndex;
}

bool logHasConfigEntry(test::TestNode& tn) {
  return configEntryIndex(tn) != kNoIndex;
}

ClusterConfig seedMembers(const std::vector<int>& ids) {
  ClusterConfig s;
  for (const int id : ids) {
    Member m;
    m.id = id;
    m.addr = "127.0.0.1:700" + std::to_string(id);
    m.voting = true;
    s.members.push_back(m);
  }
  return s;
}

// 一个"尚未进入配置"的节点：以 seed 启动并接到 transport 上（等价于运维先起进程，Leader 再 add）
struct ExtraNode {
  std::unique_ptr<MemoryLogStore> log;
  std::unique_ptr<KvStateMachine> sm;
  std::unique_ptr<MemorySnapshotStore> snapshots;
  std::unique_ptr<RaftNode> node;
};

void attachExtraNode(test::Cluster& c, int id, ExtraNode& n,
                     const ClusterConfig& seed) {
  n.log = std::make_unique<MemoryLogStore>();
  n.sm = std::make_unique<KvStateMachine>();
  n.snapshots = std::make_unique<MemorySnapshotStore>();
  RaftConfig cfg;
  cfg.selfId = id;
  for (const Member& m : seed.members) {
    if (m.id != id) cfg.peerIds.push_back(m.id);
  }
  n.node = std::make_unique<RaftNode>(cfg, *n.log, *n.sm, *c.transport,
                                      *c.clock, n.snapshots.get(), seed);
  c.transport->addNode(id, n.node.get());
}

// 丢掉"包含配置条目"的 AppendEntries 回包（用于构造 C_old 多数派可用、C_new 多数派不可用）
class ConfigReplyDropper : public MemoryTransport {
 public:
  void dropConfigRepliesFrom(const std::vector<int>& ids) { dropIds_ = ids; }

  void sendAppendEntries(int peerId, const AppendEntriesArgs& args,
                         AppendCb cb) override {
    bool hasConfig = false;
    for (const LogEntry& e : args.entries) {
      if (e.op == OpCode::kConfig) hasConfig = true;
    }
    const bool drop = hasConfig &&
                      std::find(dropIds_.begin(), dropIds_.end(), peerId) !=
                          dropIds_.end();
    if (drop) {
      MemoryTransport::sendAppendEntries(
          peerId, args, [](const AppendEntriesReply&) {});
      return;
    }
    MemoryTransport::sendAppendEntries(peerId, args, std::move(cb));
  }

 private:
  std::vector<int> dropIds_;
};

// ---------------- B 组：真实磁盘集群 ----------------

struct DiskNode {
  std::unique_ptr<FileLogStore> log;
  std::unique_ptr<KvStateMachine> sm;
  std::unique_ptr<FileSnapshotStore> snapshots;
  std::unique_ptr<RaftNode> node;
};

struct DiskCluster {
  std::shared_ptr<FakeClock> clock;
  std::shared_ptr<MemoryTransport> transport;
  std::vector<DiskNode> nodes;
  ClusterConfig seed;
  std::string root;
};

// fresh=true 时清空目录；fresh=false 时在原有数据上重建节点（模拟重启）
void buildDiskCluster(DiskCluster& c, int n, const std::string& root,
                      size_t threshold, bool fresh) {
  if (fresh) std::filesystem::remove_all(root);
  std::filesystem::create_directories(root);
  c.clock = std::make_shared<FakeClock>();
  c.transport = std::make_shared<MemoryTransport>();
  c.seed = test::makeSeedConfig(n);
  c.root = root;
  c.nodes.clear();
  c.nodes.resize(static_cast<size_t>(n));
  for (int id = 1; id <= n; ++id) {
    DiskNode& dn = c.nodes[static_cast<size_t>(id - 1)];
    const std::string dir = root + "/n" + std::to_string(id);
    dn.log = std::make_unique<FileLogStore>(dir);
    dn.sm = std::make_unique<KvStateMachine>();
    dn.snapshots = std::make_unique<FileSnapshotStore>(dir);
    RaftConfig cfg;
    cfg.selfId = id;
    cfg.snapshotThresholdEntries = threshold;
    for (const Member& m : c.seed.members) {
      if (m.id != id) cfg.peerIds.push_back(m.id);
    }
    dn.node = std::make_unique<RaftNode>(cfg, *dn.log, *dn.sm, *c.transport,
                                         *c.clock, dn.snapshots.get(), c.seed);
    c.transport->addNode(id, dn.node.get());
  }
}

void driveDiskTicks(DiskCluster& c, int count, uint64_t stepMs) {
  for (int i = 0; i < count; ++i) {
    c.clock->advance(stepMs);
    for (auto& n : c.nodes) n.node->tick();
  }
}

RaftNode* findDiskLeader(DiskCluster& c) {
  for (auto& n : c.nodes) {
    if (n.node->role() == Role::kLeader) return n.node.get();
  }
  return nullptr;
}

int diskLeaderId(DiskCluster& c) {
  RaftNode* l = findDiskLeader(c);
  return l == nullptr ? -1 : l->leaderId();
}

// RKS1 v1 手写构造（M3 布局），用于 B4 兼容守门
uint32_t crc32Of(const Byte* data, size_t len) {
  static uint32_t table[256];
  static const bool ready = [] {
    for (uint32_t i = 0; i < 256; ++i) {
      uint32_t c = i;
      for (int k = 0; k < 8; ++k) c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
      table[i] = c;
    }
    return true;
  }();
  (void)ready;
  uint32_t crc = 0xFFFFFFFFu;
  for (size_t i = 0; i < len; ++i) crc = table[(crc ^ data[i]) & 0xFFu] ^ (crc >> 8);
  return crc ^ 0xFFFFFFFFu;
}

Bytes v1SnapshotBytes(Index index, Term term, const Bytes& payload) {
  Bytes out;
  out.push_back('R');
  out.push_back('K');
  out.push_back('S');
  out.push_back('1');
  out.push_back(0x01);
  for (int i = 7; i >= 0; --i) out.push_back(static_cast<Byte>((index >> (i * 8)) & 0xff));
  for (int i = 7; i >= 0; --i) out.push_back(static_cast<Byte>((term >> (i * 8)) & 0xff));
  const uint32_t plen = static_cast<uint32_t>(payload.size());
  for (int i = 3; i >= 0; --i) out.push_back(static_cast<Byte>((plen >> (i * 8)) & 0xff));
  const uint32_t crc = crc32Of(payload.data(), payload.size());
  for (int i = 3; i >= 0; --i) out.push_back(static_cast<Byte>((crc >> (i * 8)) & 0xff));
  out.insert(out.end(), payload.begin(), payload.end());
  return out;
}

}  // namespace

// ================================ A 组 ================================

TEST(RaftMembership, A1_ConfigSerdeRoundTrip) {
  ClusterConfig c;
  c.version = 7;
  c.members = {Member{1, "127.0.0.1:1", true}, Member{2, "127.0.0.1:2", false},
               Member{3, "127.0.0.1:3", true}};
  const Bytes e = encodeClusterConfig(c);
  EXPECT_FALSE(e.empty());
  ClusterConfig out;
  ASSERT_TRUE(decodeClusterConfig(e.data(), e.size(), out));
  EXPECT_EQ(out, c);
  EXPECT_FALSE(decodeClusterConfig(e.data(), e.size() - 1, out));  // 截断
  EXPECT_FALSE(decodeClusterConfig(nullptr, 0, out));              // 空
  // 数据访问器语义
  EXPECT_EQ(c.votingCount(), 2u);
  EXPECT_EQ(c.majority(), 2u);
  EXPECT_TRUE(c.isVoting(1));
  EXPECT_FALSE(c.isVoting(2));
  EXPECT_EQ(c.votingIds(), (std::vector<int>{1, 3}));
  ClusterConfig empty;
  EXPECT_EQ(empty.majority(), 0u);  // 空配置没有多数派
}

TEST(RaftMembership, A2_AddReplicatesConfigToAllNodes) {
  auto c = test::makeMembershipCluster(3);
  test::driveTicks(*c, 60, 10);
  RaftNode* leader = test::findLeader(*c);
  ASSERT_NE(leader, nullptr);
  const ClusterConfig seed = test::makeSeedConfig(3);

  ExtraNode n4;
  attachExtraNode(*c, 4, n4, seed);
  const auto reply =
      leader->changeMembership(MembershipOp::kAdd, 4, "127.0.0.1:7004", 2000);
  ASSERT_EQ(reply.status, ClientStatus::kOk);

  test::driveTicks(*c, 60, 10);
  n4.node->tick();

  EXPECT_TRUE(leader->clusterConfig().isVoting(4));
  EXPECT_GT(leader->configVersion(), static_cast<uint64_t>(0));
  for (auto& tn : c->nodes) {
    EXPECT_EQ(tn.node->clusterConfig().version, leader->configVersion());
    EXPECT_TRUE(tn.node->clusterConfig().isVoting(4));
  }
  EXPECT_TRUE(n4.node->clusterConfig().isVoting(4));
}

TEST(RaftMembership, A3_SeedBecomesInitialConfig) {
  auto c = test::makeMembershipCluster(3);
  const ClusterConfig cfg0 = c->nodes[0].node->clusterConfig();
  EXPECT_EQ(cfg0.version, static_cast<uint64_t>(0));
  EXPECT_EQ(cfg0.votingCount(), 3u);
  EXPECT_TRUE(cfg0.isVoting(1));
  ASSERT_NE(cfg0.find(1), nullptr);
  EXPECT_EQ(cfg0.find(1)->addr, "127.0.0.1:7001");
  EXPECT_FALSE(cfg0.contains(4));
}

TEST(RaftMembership, A4_UnreachableNewNodeIsNotAddedAndDoesNotCount) {
  auto c = test::makeMembershipCluster(3);
  test::driveTicks(*c, 60, 10);
  RaftNode* leader = test::findLeader(*c);
  ASSERT_NE(leader, nullptr);

  // 第 4 个节点根本没启动 -> CatchUp 不可能完成 -> 不得写入配置
  const auto reply =
      leader->changeMembership(MembershipOp::kAdd, 4, "127.0.0.1:7004", 300);
  EXPECT_EQ(reply.status, ClientStatus::kErr);
  EXPECT_FALSE(leader->clusterConfig().isVoting(4));
  EXPECT_EQ(leader->clusterConfig().votingCount(), 3u);

  // 集群仍按旧配置正常提交
  EXPECT_EQ(leader->propose(putReq(1, "a", "1"), 500).status,
            ClientStatus::kOk);
}

TEST(RaftMembership, A5_AddCommitsAndTakesEffect) {
  auto c = test::makeMembershipCluster(3);
  test::driveTicks(*c, 60, 10);
  RaftNode* leader = test::findLeader(*c);
  ASSERT_NE(leader, nullptr);
  const int lid = leader->leaderId();

  ExtraNode n4;
  attachExtraNode(*c, 4, n4, test::makeSeedConfig(3));
  ASSERT_EQ(leader->changeMembership(MembershipOp::kAdd, 4, "127.0.0.1:7004",
                                     2000)
                .status,
            ClientStatus::kOk);
  test::driveTicks(*c, 60, 10);
  n4.node->tick();

  // 配置条目已提交（commitIndex 越过它）
  const Index ci = configEntryIndex(c->nodes[static_cast<size_t>(lid - 1)]);
  ASSERT_GT(ci, kNoIndex);
  EXPECT_GE(leader->commitIndex(), ci);
  EXPECT_EQ(leader->clusterConfig().version, ci);
  // 新配置下仍能提交
  EXPECT_EQ(leader->propose(putReq(2, "b", "2"), 500).status,
            ClientStatus::kOk);
}

TEST(RaftMembership, A6_RemoveTakesEffectAndStopsCounting) {
  auto c = test::makeMembershipCluster(4);
  test::driveTicks(*c, 60, 10);
  RaftNode* leader = test::findLeader(*c);
  ASSERT_NE(leader, nullptr);

  ASSERT_EQ(leader->changeMembership(MembershipOp::kRemove, 4, "", 2000).status,
            ClientStatus::kOk);
  test::driveTicks(*c, 60, 10);

  EXPECT_FALSE(leader->clusterConfig().contains(4));
  EXPECT_EQ(leader->clusterConfig().votingCount(), 3u);
  EXPECT_GT(leader->configVersion(), static_cast<uint64_t>(0));
  // 新配置下仍能提交
  EXPECT_EQ(leader->propose(putReq(3, "c", "3"), 500).status,
            ClientStatus::kOk);
  // 被移除节点必须退役
  EXPECT_TRUE(c->nodes[3].node->retired());
}

TEST(RaftMembership, A7_SecondChangeRejectedWhileOneInFlight) {
  auto c = test::makeMembershipCluster(5);
  test::driveTicks(*c, 80, 10);
  RaftNode* leader = test::findLeader(*c);
  ASSERT_NE(leader, nullptr);
  const int lid = leader->leaderId();

  // 只留 leader + 1 个可达节点：5 节点集群凑不够多数派 -> 配置条目挂起（在途）
  std::vector<int> keep{lid};
  for (int id = 1; id <= 5; ++id) {
    if (id != lid) {
      keep.push_back(id);
      break;
    }
  }
  for (int id = 1; id <= 5; ++id) {
    if (std::find(keep.begin(), keep.end(), id) == keep.end()) {
      c->transport->isolate(id);
    }
  }

  test::TestNode& ltn = c->nodes[static_cast<size_t>(lid - 1)];
  // 只移除"非 leader"的节点（移除 leader 自己属于 §5.7，另行覆盖）
  const int target = (lid == 5) ? 4 : 5;
  const int other = (target == 4) ? 3 : 4;
  const Index before = ltn.log->lastIndex();
  EXPECT_EQ(leader->changeMembership(MembershipOp::kRemove, target, "", 300).status,
            ClientStatus::kErr);  // 未凑齐多数派 -> 超时
  const Index afterFirst = ltn.log->lastIndex();
  EXPECT_GT(afterFirst, before);  // 条目已追加：变更在途

  // J1：在途期间第二个变更必须被拒绝，且不得追加新条目
  EXPECT_EQ(leader->changeMembership(MembershipOp::kRemove, other, "", 300).status,
            ClientStatus::kErr);
  EXPECT_EQ(ltn.log->lastIndex(), afterFirst);
}

TEST(RaftMembership, A8_ConfigCommitNeedsBothMajorities) {
  auto transport = std::make_shared<ConfigReplyDropper>();
  auto c = test::makeMembershipClusterWith(3, true, 1000000, 64, transport);
  test::driveTicks(*c, 60, 10);
  RaftNode* leader = test::findLeader(*c);
  ASSERT_NE(leader, nullptr);
  const int lid = leader->leaderId();

  // 4 号节点可达（能追平），但对"配置条目"的回包被丢掉：
  // 只允许 leader + 1 个旧成员 ack（满足 C_old 多数派 2/3，但不满足 C_new 多数派 3/4）
  ExtraNode n4;
  attachExtraNode(*c, 4, n4, test::makeSeedConfig(3));
  std::vector<int> oldFollowers;
  for (int id = 1; id <= 3; ++id) {
    if (id != lid) oldFollowers.push_back(id);
  }
  ASSERT_EQ(oldFollowers.size(), 2u);
  transport->dropConfigRepliesFrom({oldFollowers[1], 4});

  test::TestNode& ltn = c->nodes[static_cast<size_t>(lid - 1)];
  const Index before = ltn.log->lastIndex();
  const auto r =
      leader->changeMembership(MembershipOp::kAdd, 4, "127.0.0.1:7004", 300);

  EXPECT_EQ(r.status, ClientStatus::kErr);        // 没能提交
  EXPECT_GT(ltn.log->lastIndex(), before);        // 但条目已经追加
  EXPECT_TRUE(logHasConfigEntry(ltn));
  // J2：C_old={1,2,3} 多数派可用（leader+2），C_new={1,2,3,4} 多数派(3)不可用
  //     -> 该条目绝不能提交
  EXPECT_LT(leader->commitIndex(), ltn.log->lastIndex());
  // 追加即生效（决策③）：本地配置已切到 C_new
  EXPECT_TRUE(leader->clusterConfig().isVoting(4));
}

TEST(RaftMembership, A9_RemovedNodeCannotWinElection) {
  auto c = test::makeMembershipCluster(3);
  test::driveTicks(*c, 60, 10);
  RaftNode* leader = test::findLeader(*c);
  ASSERT_NE(leader, nullptr);
  const int removed = (leader->leaderId() == 3) ? 2 : 3;

  ASSERT_EQ(leader->changeMembership(MembershipOp::kRemove, removed, "", 2000)
                .status,
            ClientStatus::kOk);
  test::driveTicks(*c, 60, 10);

  RaftNode* rn = c->nodes[static_cast<size_t>(removed - 1)].node.get();
  EXPECT_TRUE(rn->retired());
  for (int i = 0; i < 200; ++i) {
    c->clock->advance(50);
    rn->tick();
  }
  EXPECT_NE(rn->role(), Role::kLeader);
}

TEST(RaftMembership, A10_RemovedNodeRetiresAndRedirects) {
  auto c = test::makeMembershipCluster(3);
  test::driveTicks(*c, 60, 10);
  RaftNode* leader = test::findLeader(*c);
  ASSERT_NE(leader, nullptr);
  const int removed = (leader->leaderId() == 3) ? 2 : 3;

  ASSERT_EQ(leader->changeMembership(MembershipOp::kRemove, removed, "", 2000)
                .status,
            ClientStatus::kOk);
  test::driveTicks(*c, 60, 10);

  RaftNode* rn = c->nodes[static_cast<size_t>(removed - 1)].node.get();
  EXPECT_TRUE(rn->retired());
  const auto w = rn->propose(putReq(9, "x", "1"), 100);
  EXPECT_EQ(w.status, ClientStatus::kNotLeader);
  // 仍能回答配置（可运维观察），且自身已不在配置中
  EXPECT_FALSE(rn->clusterConfig().contains(removed));
  EXPECT_GT(rn->configVersion(), static_cast<uint64_t>(0));
}

TEST(RaftMembership, A11_StartupRejectsNonMonotonicConfigVersions) {
  const std::vector<Member> ms = seedMembers({1, 2, 3}).members;
  MemoryLogStore log;
  std::vector<LogEntry> es;
  es.push_back(configEntry(1, 1, 1, ms));
  es.push_back(configEntry(2, 1, 3, ms));
  es.push_back(configEntry(3, 1, 2, ms));  // 版本回退 -> 必须拒绝启动
  ASSERT_TRUE(log.append(es));

  KvStateMachine sm;
  MemoryTransport transport;
  FakeClock clock;
  RaftConfig cfg;
  cfg.selfId = 1;
  cfg.peerIds = {2, 3};
  EXPECT_THROW(RaftNode node(cfg, log, sm, transport, clock), std::runtime_error);
}

TEST(RaftMembership, A12_SnapshotCarriesConfigAndRestartKeepsTopology) {
  auto c = test::makeMembershipCluster(3, true, /*threshold=*/8);
  test::driveTicks(*c, 60, 10);
  RaftNode* leader = test::findLeader(*c);
  ASSERT_NE(leader, nullptr);
  const int lid = leader->leaderId();

  ExtraNode n4;
  attachExtraNode(*c, 4, n4, test::makeSeedConfig(3));
  ASSERT_EQ(leader->changeMembership(MembershipOp::kAdd, 4, "127.0.0.1:7004",
                                     2000)
                .status,
            ClientStatus::kOk);
  for (int i = 1; i <= 20; ++i) {
    leader->propose(putReq(static_cast<uint64_t>(i), "k" + std::to_string(i), "v"),
                    1000);
  }
  leader->triggerSnapshot();
  test::driveTicks(*c, 100, 10);

  test::TestNode& ltn = c->nodes[static_cast<size_t>(lid - 1)];
  SnapshotData snap;
  ASSERT_TRUE(ltn.snapshots->load(snap));
  // 快照必须携带配置（否则 InstallSnapshot 后拓扑丢失）
  ASSERT_FALSE(snap.config.empty());
  ClusterConfig sc;
  ASSERT_TRUE(decodeClusterConfig(snap.config.data(), snap.config.size(), sc));
  EXPECT_EQ(sc.version, leader->configVersion());
  EXPECT_TRUE(sc.isVoting(4));

  // 模拟重启：同一批 store 重建节点，配置必须来自快照
  RaftConfig cfg;
  cfg.selfId = lid;
  for (const Member& m : test::makeSeedConfig(3).members) {
    if (m.id != lid) cfg.peerIds.push_back(m.id);
  }
  RaftNode restarted(cfg, *ltn.log, *ltn.sm, *c->transport, *c->clock,
                     ltn.snapshots.get(), test::makeSeedConfig(3));
  EXPECT_EQ(restarted.clusterConfig().version, sc.version);
  EXPECT_TRUE(restarted.clusterConfig().isVoting(4));
}

TEST(RaftMembership, A13_ReadIsNotServedByStaleLeader) {
  auto c = test::makeMembershipCluster(3);
  test::driveTicks(*c, 60, 10);
  RaftNode* leader = test::findLeader(*c);
  ASSERT_NE(leader, nullptr);
  ASSERT_EQ(leader->propose(putReq(1, "k", "v"), 1000).status,
            ClientStatus::kOk);

  // 健康 Leader：线性一致读可用
  const auto good = leader->linearizableGet("k", 500);
  EXPECT_EQ(good.status, ClientStatus::kOk);
  EXPECT_EQ(good.value, "v");

  // 用更高任期的投票请求把 Leader 打下去：绝不能返回陈旧值
  RequestVoteArgs hv;
  hv.term = leader->currentTerm() + 5;
  hv.candidateId = 99;
  hv.lastLogIndex = 0;
  hv.lastLogTerm = 0;
  leader->onRequestVote(hv);
  const auto stale = leader->linearizableGet("k", 200);
  EXPECT_NE(stale.status, ClientStatus::kOk);
}

TEST(RaftMembership, A14_ReadIndexRequiresQuorum) {
  auto c = test::makeMembershipCluster(3);
  test::driveTicks(*c, 60, 10);
  RaftNode* leader = test::findLeader(*c);
  ASSERT_NE(leader, nullptr);
  const int lid = leader->leaderId();
  ASSERT_EQ(leader->propose(putReq(1, "k", "v"), 1000).status,
            ClientStatus::kOk);
  EXPECT_EQ(leader->linearizableGet("k", 500).status, ClientStatus::kOk);

  // 隔离两个 follower：少数派 Leader 不得用本地状态机回答读
  for (int id = 1; id <= 3; ++id) {
    if (id != lid) c->transport->isolate(id);
  }
  const auto r = leader->linearizableGet("k", 200);
  EXPECT_NE(r.status, ClientStatus::kOk);
}

TEST(RaftMembership, A15_SupraRuleHoldsAfterMembershipChange) {
  // 先做一次成员变更（6 -> 5），再复刻 M2 的 §5.4.2 反例构造
  auto c = test::makeMembershipCluster(6, /*appendNoop=*/false);
  test::driveTicks(*c, 80, 10);
  RaftNode* leader = test::findLeader(*c);
  ASSERT_NE(leader, nullptr);
  const int leaderId = leader->leaderId();
  const int removed = (leaderId == 6) ? 5 : 6;

  ASSERT_EQ(leader->changeMembership(MembershipOp::kRemove, removed, "", 2000)
                .status,
            ClientStatus::kOk);
  test::driveTicks(*c, 60, 10);
  ASSERT_FALSE(leader->clusterConfig().contains(removed));

  // 1) 只留 leader + 1 个 follower：追加一条无法提交的旧任期条目
  int reachable = 0;
  for (int id = 1; id <= 6; ++id) {
    if (id != leaderId && id != removed) {
      reachable = id;
      break;
    }
  }
  ASSERT_NE(reachable, 0);
  for (int id = 1; id <= 6; ++id) {
    if (id != leaderId && id != reachable) c->transport->isolate(id);
  }
  const auto r1 = leader->propose(putReq(1, "old", "v1"), 50);
  EXPECT_NE(r1.status, ClientStatus::kOk);

  test::TestNode& ltn = c->nodes[static_cast<size_t>(leaderId - 1)];
  const Index oldIndex = ltn.log->lastIndex();
  ASSERT_GT(oldIndex, kNoIndex);
  const Term oldTerm = ltn.log->lastTerm();

  // 2) 逼老 leader 下台，并让它投票给持有该条目的节点
  RequestVoteArgs stepDown;
  stepDown.term = leader->currentTerm() + 1;
  stepDown.candidateId = reachable;
  stepDown.lastLogIndex = oldIndex;
  stepDown.lastLogTerm = oldTerm;
  leader->onRequestVote(stepDown);
  for (int id = 1; id <= 6; ++id) c->transport->heal(id);

  // 3) 只 tick 该节点，确保它当选
  RaftNode* carrier = c->nodes[static_cast<size_t>(reachable - 1)].node.get();
  for (int i = 0; i < 200 && carrier->role() != Role::kLeader; ++i) {
    c->clock->advance(10);
    carrier->tick();
  }
  ASSERT_EQ(carrier->role(), Role::kLeader);

  // 4) 心跳把旧任期条目复制到多数派，但 §5.4.2 禁止据此提交
  //    （配置条目本身是它自己任期的，已提交；未提交的是 oldIndex 处的旧任期条目）
  test::tickNodesOnly(*c, {carrier->leaderId()}, 200, 10);
  EXPECT_LT(carrier->commitIndex(), oldIndex);

  // 5) 本任期条目提交后把旧条目一起带上去
  const auto r2 = carrier->propose(putReq(2, "new", "v2"), 1000);
  EXPECT_EQ(r2.status, ClientStatus::kOk);
  EXPECT_EQ(carrier->commitIndex(), oldIndex + 1);
}

TEST(RaftMembership, A16_LeaderSelfRemovalStepsDownAfterCommit) {
  auto c = test::makeMembershipCluster(3);
  test::driveTicks(*c, 60, 10);
  RaftNode* leader = test::findLeader(*c);
  ASSERT_NE(leader, nullptr);
  const int lid = leader->leaderId();

  ASSERT_EQ(leader->changeMembership(MembershipOp::kRemove, lid, "", 2000).status,
            ClientStatus::kOk);
  test::driveTicks(*c, 60, 10);

  RaftNode* old = c->nodes[static_cast<size_t>(lid - 1)].node.get();
  EXPECT_TRUE(old->retired());
  EXPECT_NE(old->role(), Role::kLeader);

  // 集群仍可用：新 Leader 能提交
  RaftNode* nl = test::findLeader(*c);
  ASSERT_NE(nl, nullptr);
  EXPECT_EQ(nl->propose(putReq(77, "z", "1"), 1000).status, ClientStatus::kOk);
}

TEST(RaftMembership, A17_ConfigMessageCodec) {
  ConfigRequestArgs req;
  req.action = 1;
  req.targetId = 7;
  req.addr = "127.0.0.1:7007";
  const Bytes re = encodeConfigRequest(req);
  ConfigRequestArgs reqOut;
  ASSERT_TRUE(decodeConfigRequest(re.data(), re.size(), reqOut));
  EXPECT_EQ(reqOut.action, req.action);
  EXPECT_EQ(reqOut.targetId, req.targetId);
  EXPECT_EQ(reqOut.addr, req.addr);
  EXPECT_FALSE(decodeConfigRequest(re.data(), re.size() - 1, reqOut));

  ConfigReplyArgs rep;
  rep.term = 5;
  rep.ok = true;
  rep.leaderHint = 2;
  rep.config.version = 9;
  rep.config.members = {Member{1, "127.0.0.1:1", true},
                        Member{2, "127.0.0.1:2", false}};
  const Bytes pe = encodeConfigReply(rep);
  ConfigReplyArgs repOut;
  ASSERT_TRUE(decodeConfigReply(pe.data(), pe.size(), repOut));
  EXPECT_EQ(repOut.term, rep.term);
  EXPECT_TRUE(repOut.ok);
  EXPECT_EQ(repOut.leaderHint, rep.leaderHint);
  EXPECT_EQ(repOut.config, rep.config);
  EXPECT_FALSE(decodeConfigReply(pe.data(), pe.size() - 1, repOut));
}

// ================================ B 组 ================================

TEST(RaftMembershipDisk, B1_ConfigPersistsAcrossRestart) {
  const std::string root = tempDir();
  DiskCluster c;
  buildDiskCluster(c, 3, root, 1000000, /*fresh=*/true);
  driveDiskTicks(c, 80, 10);
  const int lid = diskLeaderId(c);
  ASSERT_GT(lid, 0);
  RaftNode* leader = c.nodes[static_cast<size_t>(lid - 1)].node.get();

  const int removed = (lid == 3) ? 2 : 3;
  ASSERT_EQ(
      leader->changeMembership(MembershipOp::kRemove, removed, "", 2000).status,
      ClientStatus::kOk);
  driveDiskTicks(c, 60, 10);
  const uint64_t ver = leader->configVersion();
  ASSERT_GT(ver, static_cast<uint64_t>(0));

  // 重启：同一目录重建节点，配置必须从日志恢复
  buildDiskCluster(c, 3, root, 1000000, /*fresh=*/false);
  driveDiskTicks(c, 80, 10);
  RaftNode* repl = c.nodes[static_cast<size_t>(lid - 1)].node.get();
  EXPECT_EQ(repl->configVersion(), ver);
  EXPECT_FALSE(repl->clusterConfig().contains(removed));
  std::filesystem::remove_all(root);
}

TEST(RaftMembershipDisk, B2_ConfigCompactedThenRecoveredFromSnapshot) {
  const std::string root = tempDir();
  DiskCluster c;
  buildDiskCluster(c, 3, root, /*threshold=*/8, /*fresh=*/true);
  driveDiskTicks(c, 80, 10);
  const int lid = diskLeaderId(c);
  ASSERT_GT(lid, 0);
  RaftNode* leader = c.nodes[static_cast<size_t>(lid - 1)].node.get();

  const int removed = (lid == 3) ? 2 : 3;
  ASSERT_EQ(
      leader->changeMembership(MembershipOp::kRemove, removed, "", 2000).status,
      ClientStatus::kOk);
  for (int i = 1; i <= 30; ++i) {
    leader->propose(putReq(static_cast<uint64_t>(i), "k" + std::to_string(i), "v"),
                    1000);
  }
  leader->triggerSnapshot();
  driveDiskTicks(c, 120, 10);

  const uint64_t ver = leader->configVersion();
  ASSERT_GT(ver, static_cast<uint64_t>(0));
  DiskNode& ldn = c.nodes[static_cast<size_t>(lid - 1)];
  ASSERT_GT(ldn.log->lastIncludedIndex(), static_cast<Index>(0));  // 已 compact
  SnapshotData snap;
  ASSERT_TRUE(ldn.snapshots->load(snap));
  ASSERT_FALSE(snap.config.empty());  // 配置必须随快照持久化

  buildDiskCluster(c, 3, root, /*threshold=*/8, /*fresh=*/false);
  driveDiskTicks(c, 80, 10);
  RaftNode* repl = c.nodes[static_cast<size_t>(lid - 1)].node.get();
  EXPECT_EQ(repl->configVersion(), ver);
  EXPECT_FALSE(repl->clusterConfig().contains(removed));
  std::filesystem::remove_all(root);
}

TEST(RaftMembershipDisk, B3_CrashDuringConfigChangeKeepsConsistentTopology) {
  const std::string root = tempDir();
  DiskCluster c;
  buildDiskCluster(c, 3, root, 1000000, /*fresh=*/true);
  driveDiskTicks(c, 80, 10);
  const int lid = diskLeaderId(c);
  ASSERT_GT(lid, 0);
  RaftNode* leader = c.nodes[static_cast<size_t>(lid - 1)].node.get();

  // 让配置条目无法提交（隔离 2 个 follower），然后"崩溃"（销毁节点对象）
  const int removed = (lid == 3) ? 2 : 3;
  for (int id = 1; id <= 3; ++id) {
    if (id != lid) c.transport->isolate(id);
  }
  const auto r =
      leader->changeMembership(MembershipOp::kRemove, removed, "", 300);
  EXPECT_EQ(r.status, ClientStatus::kErr);  // 未提交
  c.nodes.clear();                          // 模拟 kill -9（日志文件保留在磁盘上）

  // 重启：拓扑必须是自洽的（由日志决定），且集群能重新选主并提交
  buildDiskCluster(c, 3, root, 1000000, /*fresh=*/false);
  driveDiskTicks(c, 100, 10);
  RaftNode* repl = c.nodes[static_cast<size_t>(lid - 1)].node.get();
  const ClusterConfig cfg = repl->clusterConfig();
  EXPECT_EQ(cfg.votingCount() == 2u || cfg.votingCount() == 3u, true);
  EXPECT_EQ(cfg.contains(removed), cfg.votingCount() == 3u);
  std::filesystem::remove_all(root);
}

TEST(RaftMembershipDisk, B4_Rks1V1SnapshotStillLoads) {
  const std::string dir = tempDir();
  std::filesystem::create_directories(dir + "/raft");
  const Bytes payload = Bytes{'k', 'v'};
  const Bytes v1 = v1SnapshotBytes(/*index=*/5, /*term=*/2, payload);
  {
    std::ofstream out(dir + "/raft/snapshot.dat", std::ios::binary | std::ios::trunc);
    out.write(reinterpret_cast<const char*>(v1.data()),
              static_cast<std::streamsize>(v1.size()));
  }
  FileSnapshotStore store(dir);
  SnapshotData out;
  ASSERT_TRUE(store.load(out));  // M3 的 v1 快照必须仍可解码
  EXPECT_EQ(out.lastIncludedIndex, static_cast<Index>(5));
  EXPECT_EQ(out.lastIncludedTerm, static_cast<Term>(2));
  EXPECT_EQ(out.payload, payload);
  EXPECT_TRUE(out.config.empty());  // v1 未携带配置
  std::filesystem::remove_all(dir);
}
