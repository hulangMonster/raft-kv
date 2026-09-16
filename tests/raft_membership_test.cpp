// M4 (membership + linearizable read) tests — #2 TDD 阶段：全部用例预期失败（RED）。
//
// A 组：确定性（FakeClock + MemoryTransport + MemoryLogStore + MemorySnapshotStore，无磁盘/网络）
// B 组：真实磁盘（FileLogStore + FileSnapshotStore + 临时目录）
//
// 用例编号与 docs/m4-design.md §8 / docs/m4-prerequisites.md §8 一一对应。
// 注意：B4 是 "RKS1 v1 兼容守门" 用例，从 #2 起即通过（M4 不得破坏 v1 解码）。
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
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

// 可以在指定 peer 上"闸住" AppendEntries 的传输：用于确定性地把成员变更卡在
// CatchUp 阶段（B6 的 check-then-act 窗口）。被闸住的发送直接丢弃，catchUpPeer
// 会按自己的退避节奏重试，闸门一开就正常投递。
class GatedTransport : public MemoryTransport {
 public:
  void gate(const std::vector<int>& ids) {
    std::lock_guard<std::mutex> lk(mu_);
    gated_ = ids;
    open_ = false;
    blocked_.clear();
  }
  void openGate() {
    std::lock_guard<std::mutex> lk(mu_);
    open_ = true;
  }
  // 有多少个不同的 peer 曾被闸住（= 有多少个成员变更真的进入了 CatchUp）
  size_t blockedDistinctPeers() const {
    std::lock_guard<std::mutex> lk(mu_);
    return blocked_.size();
  }

  void sendAppendEntries(int peerId, const AppendEntriesArgs& args,
                         AppendCb cb) override {
    {
      std::lock_guard<std::mutex> lk(mu_);
      if (!open_ && std::find(gated_.begin(), gated_.end(), peerId) !=
                        gated_.end()) {
        blocked_.insert(peerId);
        return;  // 丢弃：调用方超时/重试
      }
    }
    MemoryTransport::sendAppendEntries(peerId, args, std::move(cb));
  }

 private:
  mutable std::mutex mu_;
  std::vector<int> gated_;
  std::set<int> blocked_;
  bool open_ = false;
};

// 记录每个 peer 的 AppendEntries 次数与 transport_.removePeer 调用，用于验证
// "配置条目提交后被移除的节点真的被回收了"（评审 O2）。
class PeerBookTransport : public MemoryTransport {
 public:
  void removePeer(int id) override {
    {
      std::lock_guard<std::mutex> lk(mu_);
      removed_.insert(id);
    }
    MemoryTransport::removePeer(id);
  }

  void sendAppendEntries(int peerId, const AppendEntriesArgs& args,
                         AppendCb cb) override {
    {
      std::lock_guard<std::mutex> lk(mu_);
      ++sends_[peerId];
    }
    MemoryTransport::sendAppendEntries(peerId, args, std::move(cb));
  }

  bool wasRemoved(int id) const {
    std::lock_guard<std::mutex> lk(mu_);
    return removed_.count(id) != 0;
  }
  int sendsTo(int id) const {
    std::lock_guard<std::mutex> lk(mu_);
    const auto it = sends_.find(id);
    return it == sends_.end() ? 0 : it->second;
  }

 private:
  mutable std::mutex mu_;
  std::set<int> removed_;
  std::map<int, int> sends_;
};

// ---- 评审批 1 新增测试桩 ----

// 丢掉来自指定 peer 的所有 AppendEntries 回包：目标节点仍会收到并落盘日志（心跳也
// 照收，因此不会超时竞选），但永远不被计为 ack。用于精确构造"某个多数派集合缺席"。
class ReplyDropper : public MemoryTransport {
 public:
  void dropRepliesFrom(const std::vector<int>& ids) { dropIds_ = ids; }
  void healReplies() { dropIds_.clear(); }

  void sendAppendEntries(int peerId, const AppendEntriesArgs& args,
                         AppendCb cb) override {
    if (std::find(dropIds_.begin(), dropIds_.end(), peerId) == dropIds_.end()) {
      MemoryTransport::sendAppendEntries(peerId, args, std::move(cb));
      return;
    }
    MemoryTransport::sendAppendEntries(peerId, args,
                                       [](const AppendEntriesReply&) {});
  }

 private:
  std::vector<int> dropIds_;
};

// 把发给指定 peer 的 AppendEntries 的 leaderCommit 抹成 0：该 peer 会持有条目，
// 但 commitIndex 永远不推进（复现"新 Leader 尚未学到旧 Leader 的提交点"）。
class CommitNoticeSuppressor : public MemoryTransport {
 public:
  void suppressFor(const std::vector<int>& ids) { ids_ = ids; }

  void sendAppendEntries(int peerId, const AppendEntriesArgs& args,
                         AppendCb cb) override {
    if (std::find(ids_.begin(), ids_.end(), peerId) == ids_.end()) {
      MemoryTransport::sendAppendEntries(peerId, args, std::move(cb));
      return;
    }
    AppendEntriesArgs a = args;
    a.leaderCommit = kNoIndex;
    MemoryTransport::sendAppendEntries(peerId, a, std::move(cb));
  }

 private:
  std::vector<int> ids_;
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

  // 9/14：ReadIndex 探针
  ReadProbeArgs probe;
  probe.term = 3;
  probe.leaderId = 1;
  probe.seq = 42;
  const Bytes pb = encodeReadProbe(probe);
  ReadProbeArgs probeOut;
  ASSERT_TRUE(decodeReadProbe(pb.data(), pb.size(), probeOut));
  EXPECT_EQ(probeOut.term, probe.term);
  EXPECT_EQ(probeOut.leaderId, probe.leaderId);
  EXPECT_EQ(probeOut.seq, probe.seq);

  ReadProbeReply pr;
  pr.term = 3;
  pr.ok = true;
  pr.seq = 42;
  const Bytes prb = encodeReadProbeReply(pr);
  ReadProbeReply prOut;
  ASSERT_TRUE(decodeReadProbeReply(prb.data(), prb.size(), prOut));
  EXPECT_EQ(prOut.term, pr.term);
  EXPECT_TRUE(prOut.ok);
  EXPECT_EQ(prOut.seq, pr.seq);
  EXPECT_FALSE(decodeReadProbeReply(prb.data(), prb.size() - 1, prOut));
}

TEST(RaftMembership, A18_InstalledSnapshotCarriesConfig) {
  // 配置条目被 compact 掉之后，靠 InstallSnapshot 追平的节点只能从快照里学到拓扑
  auto c = test::makeMembershipCluster(3, /*appendNoop=*/true, /*threshold=*/8);
  test::driveTicks(*c, 60, 10);
  RaftNode* leader = test::findLeader(*c);
  ASSERT_NE(leader, nullptr);
  const int lid = leader->leaderId();
  const int lagId = (lid == 3) ? 2 : 3;

  // 让 lagId 掉队，其余节点完成一次成员变更
  c->transport->isolate(lagId);
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
  test::driveTicks(*c, 120, 10);

  test::TestNode& ltn = c->nodes[static_cast<size_t>(lid - 1)];
  ASSERT_GT(ltn.log->lastIncludedIndex(), static_cast<Index>(0));  // 已 compact
  const uint64_t ver = leader->configVersion();
  ASSERT_GT(ver, static_cast<uint64_t>(0));

  // lagId 回来：落后于快照边界 -> 只能经 InstallSnapshot 追平，并继承快照里的配置
  c->transport->heal(lagId);
  test::driveTicks(*c, 600, 10);
  RaftNode* lag = c->nodes[static_cast<size_t>(lagId - 1)].node.get();
  EXPECT_GT(lag->lastIncludedIndex(), static_cast<Index>(0));
  EXPECT_EQ(lag->configVersion(), ver);
  EXPECT_TRUE(lag->clusterConfig().isVoting(4));
}

TEST(RaftMembership, A19_ConfigEntryTruncationRollsBackConfig) {
  // 设计 §5.2「回滚」：未提交的配置条目被截断后，节点必须把配置退回
  // 「快照/seed 基线 + 剩余日志」重算的结果，绝不保留日志里已不存在的配置。
  auto c = test::makeMembershipCluster(3, /*appendNoop=*/false);
  test::driveTicks(*c, 60, 10);
  RaftNode* l1 = test::findLeader(*c);
  ASSERT_NE(l1, nullptr);
  const int lid = l1->leaderId();
  int victim = 0;
  for (int id = 1; id <= 3; ++id) {
    if (id != lid) { victim = id; break; }
  }
  ASSERT_NE(victim, 0);

  // 1) 隔离其余节点：变更条目只能"追加即生效"，永远提交不了
  for (int id = 1; id <= 3; ++id) {
    if (id != lid) c->transport->isolate(id);
  }
  const Index before = c->nodes[static_cast<size_t>(lid - 1)].log->lastIndex();
  EXPECT_EQ(l1->changeMembership(MembershipOp::kRemove, victim, "", 300).status,
            ClientStatus::kErr);
  const Index cfgIdx = c->nodes[static_cast<size_t>(lid - 1)].log->lastIndex();
  ASSERT_GT(cfgIdx, before);                       // 条目已追加（在途）
  EXPECT_FALSE(l1->clusterConfig().contains(victim));  // 追加即生效
  const uint64_t staleVer = l1->configVersion();
  ASSERT_GE(staleVer, static_cast<uint64_t>(cfgIdx));

  // 2) 恢复网络，让"没有这条条目的节点"在更高任期当选
  for (int id = 1; id <= 3; ++id) c->transport->heal(id);
  RaftNode* nl = c->nodes[static_cast<size_t>(victim - 1)].node.get();
  RequestVoteArgs stepDown;
  stepDown.term = l1->currentTerm() + 1;
  stepDown.candidateId = victim;
  stepDown.lastLogIndex = c->nodes[static_cast<size_t>(victim - 1)].log->lastIndex();
  stepDown.lastLogTerm = c->nodes[static_cast<size_t>(victim - 1)].log->lastTerm();
  l1->onRequestVote(stepDown);
  for (int i = 0; i < 200 && nl->role() != Role::kLeader; ++i) {
    c->clock->advance(10);
    nl->tick();
  }
  ASSERT_EQ(nl->role(), Role::kLeader);

  // 3) 新 Leader 在 cfgIdx 处写入冲突条目 -> 旧 Leader 必须截断并回滚配置
  ASSERT_EQ(nl->propose(putReq(1, "k", "v"), 1000).status, ClientStatus::kOk);
  test::tickNodesOnly(*c, {nl->leaderId()}, 200, 10);

  EXPECT_LT(l1->configVersion(), staleVer);            // 回滚（版本下降）
  EXPECT_EQ(l1->configVersion(), nl->configVersion());  // 与当前 Leader 一致
  EXPECT_TRUE(l1->clusterConfig().contains(victim));    // victim 不再是"已移除"
}

TEST(RaftMembership, A20_J2IsNotBypassedByLaterEntry) {
  // 评审 B2（安全性）：配置条目在途时，后一条普通条目哪怕先拿到 C_new 多数派，
  // 也绝不能让 commitIndex_ 越过配置条目。6 -> 5 的偶数旧配置下 C_old 多数派(4)
  // 严格大于 C_new 多数派(3)：一旦越过，J2 被完全绕过，已提交配置/条目可能被
  // 新 Leader 覆盖（§5.4.2 反例）。
  auto transport = std::make_shared<ReplyDropper>();
  auto c = test::makeMembershipClusterWith(6, /*appendNoop=*/false, 1000000, 64,
                                           transport);
  test::driveTicks(*c, 80, 10);
  RaftNode* leader = test::findLeader(*c);
  ASSERT_NE(leader, nullptr);
  const int lid = leader->leaderId();
  const int victim = (lid == 6) ? 5 : 6;

  // 只保留 2 个 peer 的应答（self + 2 = 3）：满足 C_new 多数派，不满足 C_old 多数派
  std::vector<int> keep;
  for (int id = 1; id <= 6; ++id) {
    if (id != lid && id != victim && keep.size() < 2) keep.push_back(id);
  }
  ASSERT_EQ(keep.size(), 2u);
  std::vector<int> drop;
  for (int id = 1; id <= 6; ++id) {
    if (id != lid && std::find(keep.begin(), keep.end(), id) == keep.end()) {
      drop.push_back(id);
    }
  }
  transport->dropRepliesFrom(drop);
  test::driveTicks(*c, 200, 10);

  test::TestNode& ltn = c->nodes[static_cast<size_t>(lid - 1)];
  const Index before = ltn.log->lastIndex();
  const auto r =
      leader->changeMembership(MembershipOp::kRemove, victim, "", 300);
  EXPECT_EQ(r.status, ClientStatus::kErr);  // C_old 多数派缺席 -> 不能提交
  const Index cfgIdx = ltn.log->lastIndex();
  ASSERT_GT(cfgIdx, before);
  EXPECT_LT(leader->commitIndex(), cfgIdx);
  EXPECT_FALSE(leader->clusterConfig().contains(victim));  // 追加即生效

  // 关键断言：后一条普通条目先拿到 C_new 多数派，也不得把 commitIndex_ 带过 cfgIdx
  const auto w = leader->propose(putReq(1, "k", "v"), 300);
  EXPECT_EQ(w.status, ClientStatus::kErr);
  EXPECT_LT(leader->commitIndex(), cfgIdx);
  std::string v;
  EXPECT_FALSE(ltn.sm->get("k", v));  // 未提交 -> 状态机里没有这个 key

  // 恢复应答：C_old 与 C_new 多数派都到位后，配置条目与后续条目才可以提交
  transport->healReplies();
  test::driveTicks(*c, 200, 10);
  EXPECT_GE(leader->commitIndex(), cfgIdx);
  EXPECT_TRUE(ltn.sm->get("k", v));
}

TEST(RaftMembership, A21_NewLeaderWithStaleCommitIndexMustNotServeStaleRead) {
  // 评审 B5（线性一致读）：新 Leader 可能已持有已提交条目、却还没学到 commitIndex。
  // 在它提交本任期条目之前按陈旧 commitIndex 做 ReadIndex，会返回 NOT_FOUND/旧值。
  auto transport = std::make_shared<CommitNoticeSuppressor>();
  auto c = test::makeMembershipClusterWith(3, /*appendNoop=*/false, 1000000, 64,
                                           transport);
  test::driveTicks(*c, 60, 10);
  RaftNode* leader = test::findLeader(*c);
  ASSERT_NE(leader, nullptr);
  const int lid = leader->leaderId();
  std::vector<int> followers;
  for (int id = 1; id <= 3; ++id) {
    if (id != lid) followers.push_back(id);
  }
  ASSERT_EQ(followers.size(), 2u);

  // 两个 follower 都收到条目，但 leaderCommit 被抹成 0 -> 它们持有条目却 applied=0
  transport->suppressFor(followers);
  ASSERT_EQ(leader->propose(putReq(1, "k", "v"), 1000).status,
            ClientStatus::kOk);
  for (const int f : followers) {
    test::TestNode& ftn = c->nodes[static_cast<size_t>(f - 1)];
    EXPECT_EQ(ftn.log->lastIndex(), static_cast<Index>(1));  // 条目已持有
    EXPECT_EQ(ftn.node->commitIndex(), kNoIndex);            // 但未提交
    EXPECT_EQ(ftn.node->lastApplied(), kNoIndex);
  }

  // 隔离旧 Leader：剩下两个 follower 选出新 Leader（它的 commitIndex 是陈旧的 0）
  c->transport->isolate(lid);
  test::tickNodesOnly(*c, followers, 200, 10);
  RaftNode* nl = nullptr;
  for (const int f : followers) {
    if (c->nodes[static_cast<size_t>(f - 1)].node->role() == Role::kLeader) {
      nl = c->nodes[static_cast<size_t>(f - 1)].node.get();
    }
  }
  ASSERT_NE(nl, nullptr);
  EXPECT_NE(nl->leaderId(), lid);
  EXPECT_EQ(nl->commitIndex(), kNoIndex);  // 陈旧提交点

  // 宁可报错/超时，也绝不能返回陈旧值（NOT_FOUND / 旧值）
  const auto rd = nl->linearizableGet("k", 300);
  EXPECT_NE(rd.status, ClientStatus::kNotFound);
  EXPECT_NE(rd.status, ClientStatus::kOk);
  EXPECT_EQ(rd.status, ClientStatus::kErr);
}

TEST(RaftMembership, A22_CodecRejectsMaliciousInput) {
  // 评审 B7/O8：恶意/边界帧必须被干净拒绝，既不抛异常也不做天量分配。
  {
    // AppendEntries：40 字节帧 + count = 0xFFFFFFFF（旧实现 reserve 几百 GB）
    Bytes payload(40, 0);
    payload[36] = 0xFF;
    payload[37] = 0xFF;
    payload[38] = 0xFF;
    payload[39] = 0xFF;
    AppendEntriesArgs out;
    EXPECT_NO_THROW({
      EXPECT_FALSE(decodeAppendEntries(payload.data(), payload.size(), out));
    });
  }
  {
    // AppendEntries：count=1 但条目的 keyLen 声明远超剩余字节
    Bytes payload(81, 0);
    payload[36] = 0x00;
    payload[37] = 0x00;
    payload[38] = 0x00;
    payload[39] = 0x01;  // count = 1
    payload[40 + 16] = 0x02;  // op = kGet（合法）
    payload[40 + 17] = 0xFF;  // keyLen = 0xFFFFFF00
    payload[40 + 18] = 0xFF;
    payload[40 + 19] = 0xFF;
    payload[40 + 20] = 0x00;
    AppendEntriesArgs out;
    EXPECT_NO_THROW({
      EXPECT_FALSE(decodeAppendEntries(payload.data(), payload.size(), out));
    });
  }
  {
    // 配置请求：未知 action 必须被拒绝（否则会落进 remove 分支）
    Bytes p;
    p.push_back(9);  // action
    p.push_back(0); p.push_back(0); p.push_back(0); p.push_back(1);
    p.push_back(0); p.push_back(0);
    ConfigRequestArgs out;
    EXPECT_FALSE(decodeConfigRequest(p.data(), p.size(), out));

    // addrLen 超出剩余字节
    Bytes q;
    q.push_back(0);
    q.push_back(0); q.push_back(0); q.push_back(0); q.push_back(1);
    q.push_back(0); q.push_back(5);
    q.push_back('a');
    EXPECT_FALSE(decodeConfigRequest(q.data(), q.size(), out));
  }
  {
    // 编码器：超长地址必须写出"addrLen 与字节数一致"的帧，而不是自相矛盾的帧
    ConfigRequestArgs req;
    req.action = 1;
    req.targetId = 2;
    req.addr.assign(70000, 'x');
    const Bytes e = encodeConfigRequest(req);
    ASSERT_FALSE(e.empty());
    ConfigRequestArgs out;
    ASSERT_TRUE(decodeConfigRequest(e.data(), e.size(), out));
    EXPECT_EQ(out.addr.size(), static_cast<size_t>(0xFFFF));
  }
  {
    // 重复 id 的配置：votingIds()/多数派会对同一节点重复计数 -> 必须拒绝
    ClusterConfig dup;
    dup.version = 1;
    dup.members = {Member{1, "127.0.0.1:1", true},
                   Member{1, "127.0.0.1:2", true}};
    const Bytes ce = encodeClusterConfig(dup);
    ClusterConfig cout;
    EXPECT_FALSE(decodeClusterConfig(ce.data(), ce.size(), cout));

    ConfigReplyArgs rep;
    rep.config = dup;
    const Bytes pe = encodeConfigReply(rep);
    ConfigReplyArgs repOut;
    EXPECT_FALSE(decodeConfigReply(pe.data(), pe.size(), repOut));
  }
  {
    // 配置 payload 的 count 上界：解码前必须先与剩余字节核对（防超大分配）
    Bytes p;
    p.push_back(1);  // cfgVer
    for (int i = 0; i < 8; ++i) p.push_back(0);
    p.push_back(0xFF); p.push_back(0xFF); p.push_back(0xFF); p.push_back(0xFF);
    ClusterConfig out;
    EXPECT_NO_THROW({
      EXPECT_FALSE(decodeClusterConfig(p.data(), p.size(), out));
    });
  }
}

TEST(RaftMembership, A23_OnlyVotingMembersGetVotesAndRetiredNodesDoNotVote) {
  // 评审 B1（安全性，J4）：非投票成员 / 已退役节点都不得投票，也不得拿到票。
  // 否则一个已被移除、但仍自认持有 C_old 的分区节点可以拿到 C_old 多数派当选，
  // 再用更高任期截断已提交条目。
  auto c = test::makeMembershipCluster(3);
  test::driveTicks(*c, 60, 10);
  RaftNode* leader = test::findLeader(*c);
  ASSERT_NE(leader, nullptr);
  const int lid = leader->leaderId();
  int victim = 0;
  int other = 0;
  for (int id = 1; id <= 3; ++id) {
    if (id == lid) continue;
    if (victim == 0) {
      victim = id;
    } else {
      other = id;
    }
  }
  ASSERT_NE(victim, 0);
  ASSERT_NE(other, 0);

  ASSERT_EQ(leader->changeMembership(MembershipOp::kRemove, victim, "", 2000)
                .status,
            ClientStatus::kOk);
  test::driveTicks(*c, 60, 10);
  ASSERT_FALSE(leader->clusterConfig().contains(victim));

  test::TestNode& rtn = c->nodes[static_cast<size_t>(victim - 1)];
  RaftNode* rn = rtn.node.get();
  ASSERT_TRUE(rn->retired());
  RaftNode* on = c->nodes[static_cast<size_t>(other - 1)].node.get();

  // 1) stale 的已移除节点竞选（日志并不落后）：持有 C_new 的成员必须拒票
  RequestVoteArgs stale;
  stale.term = on->currentTerm() + 1;
  stale.candidateId = victim;
  stale.lastLogIndex = rtn.log->lastIndex();
  stale.lastLogTerm = rtn.log->lastTerm();
  EXPECT_FALSE(on->onRequestVote(stale).voteGranted);

  // 2) 退役节点自己也不投票（即使对方任期更高、日志更新）
  test::TestNode& ltn = c->nodes[static_cast<size_t>(lid - 1)];
  RequestVoteArgs fromMember;
  fromMember.term = rn->currentTerm() + 1;
  fromMember.candidateId = lid;
  fromMember.lastLogTerm =
      std::max(ltn.log->lastTerm(), rtn.log->lastTerm());
  fromMember.lastLogIndex =
      std::max(ltn.log->lastIndex(), rtn.log->lastIndex()) + 1;
  EXPECT_FALSE(rn->onRequestVote(fromMember).voteGranted);

  // 3) 从没进过配置的节点（CatchUp 目标）同样拿不到票
  RequestVoteArgs outsider;
  outsider.term = leader->currentTerm() + 1;
  outsider.candidateId = 9;
  outsider.lastLogIndex = ltn.log->lastIndex() + 1;
  outsider.lastLogTerm = std::max(ltn.log->lastTerm(), leader->currentTerm());
  EXPECT_FALSE(leader->onRequestVote(outsider).voteGranted);
}

TEST(RaftMembership, A24_SnapshotNeverCarriesInFlightConfig) {
  // 评审 B3（持久化一致性）：配置条目"追加即生效"，所以 in-flight 配置的 version
  // 可能大于快照边界。若把它写进快照，重启时重放同一条目会因版本回退直接抛异常。
  auto c = test::makeMembershipCluster(3, /*appendNoop=*/false, 1000000);
  test::driveTicks(*c, 60, 10);
  RaftNode* leader = test::findLeader(*c);
  ASSERT_NE(leader, nullptr);
  const int lid = leader->leaderId();
  int victim = 0;
  for (int id = 1; id <= 3; ++id) {
    if (id != lid) {
      victim = id;
      break;
    }
  }
  test::TestNode& ltn = c->nodes[static_cast<size_t>(lid - 1)];

  // 先提交一条普通条目：让 lastApplied_ > lastIncluded_，快照才有内容可做
  ASSERT_EQ(leader->propose(putReq(1, "k", "v"), 1000).status,
            ClientStatus::kOk);
  ASSERT_EQ(leader->lastApplied(), static_cast<Index>(1));

  // 隔离其余节点：remove 条目只能"追加即生效"，提交不了（在途）
  for (int id = 1; id <= 3; ++id) {
    if (id != lid) c->transport->isolate(id);
  }
  EXPECT_EQ(leader->changeMembership(MembershipOp::kRemove, victim, "", 300)
                .status,
            ClientStatus::kErr);
  const Index cfgIdx = ltn.log->lastIndex();
  ASSERT_EQ(cfgIdx, static_cast<Index>(2));
  ASSERT_LT(leader->commitIndex(), cfgIdx);                 // 仍然在途
  EXPECT_FALSE(leader->clusterConfig().contains(victim));    // 但已生效

  leader->triggerSnapshot();
  leader->tick();

  SnapshotData snap;
  ASSERT_TRUE(ltn.snapshots->load(snap));
  ASSERT_FALSE(snap.config.empty());
  ClusterConfig sc;
  ASSERT_TRUE(decodeClusterConfig(snap.config.data(), snap.config.size(), sc));
  EXPECT_LE(sc.version, snap.lastIncludedIndex);  // 快照配置不得越界（B3）
  EXPECT_TRUE(sc.contains(victim));               // 边界内的拓扑仍是 C_old

  // 同一批 store 重启（模拟崩溃恢复）：绝不能抛"config version regressed"
  const ClusterConfig seed = test::makeSeedConfig(3);
  RaftConfig rcfg;
  rcfg.selfId = lid;
  rcfg.appendNoop = false;
  rcfg.snapshotThresholdEntries = 1000000;
  rcfg.snapshotChunkBytes = 64;
  for (const Member& m : seed.members) {
    if (m.id != lid) rcfg.peerIds.push_back(m.id);
  }
  std::unique_ptr<RaftNode> restarted;
  EXPECT_NO_THROW(
      restarted = std::make_unique<RaftNode>(rcfg, *ltn.log, *ltn.sm,
                                            *c->transport, *c->clock,
                                            ltn.snapshots.get(), seed));
  ASSERT_NE(restarted, nullptr);
  // 拓扑与日志一致：快照给 C_old，日志里那条在途条目把它推进到 C_new
  EXPECT_EQ(restarted->configVersion(), cfgIdx);
  EXPECT_FALSE(restarted->clusterConfig().contains(victim));
}

TEST(RaftMembership, A25_RestartKeepsConfigChangeInFlight) {
  // 评审 B4：commitIndex 是易失状态，重启后日志尾部的配置条目必须重新标记为
  // "在途"，否则 J1（一次一个）失效 -> 可以追加第二条配置条目。
  auto c = test::makeMembershipCluster(3, /*appendNoop=*/false, 1000000);
  test::driveTicks(*c, 60, 10);
  RaftNode* leader = test::findLeader(*c);
  ASSERT_NE(leader, nullptr);
  const int lid = leader->leaderId();
  int victim = 0;
  int other = 0;
  for (int id = 1; id <= 3; ++id) {
    if (id == lid) continue;
    if (victim == 0) {
      victim = id;
    } else {
      other = id;
    }
  }
  test::TestNode& ltn = c->nodes[static_cast<size_t>(lid - 1)];

  // 隔离其余节点：remove 条目追加成功但无法提交（在途）
  for (int id = 1; id <= 3; ++id) {
    if (id != lid) c->transport->isolate(id);
  }
  EXPECT_EQ(leader->changeMembership(MembershipOp::kRemove, victim, "", 300)
                .status,
            ClientStatus::kErr);
  const Index cfgIdx = ltn.log->lastIndex();
  ASSERT_GT(cfgIdx, kNoIndex);
  ASSERT_LT(leader->commitIndex(), cfgIdx);

  // kill -9 + 重新拉起（同一批 store）
  ltn.node.reset();
  ltn.sm = std::make_unique<KvStateMachine>();
  const ClusterConfig seed = test::makeSeedConfig(3);
  RaftConfig rcfg;
  rcfg.selfId = lid;
  rcfg.appendNoop = false;
  rcfg.snapshotThresholdEntries = 1000000;
  rcfg.snapshotChunkBytes = 64;
  for (const Member& m : seed.members) {
    if (m.id != lid) rcfg.peerIds.push_back(m.id);
  }
  ltn.node = std::make_unique<RaftNode>(rcfg, *ltn.log, *ltn.sm, *c->transport,
                                        *c->clock, ltn.snapshots.get(), seed);
  c->transport->addNode(lid, ltn.node.get());
  RaftNode* restarted = ltn.node.get();

  // 只有它 tick：它竞选并当选（其它节点会同步应答投票）
  for (int id = 1; id <= 3; ++id) c->transport->heal(id);
  test::tickNodesOnly(*c, {lid}, 200, 10);
  ASSERT_EQ(restarted->role(), Role::kLeader);

  // J1 必须在重启后依然有效：第二条配置条目不允被追加
  const Index before = ltn.log->lastIndex();
  const auto r2 =
      restarted->changeMembership(MembershipOp::kRemove, other, "", 300);
  EXPECT_EQ(r2.status, ClientStatus::kErr);
  EXPECT_EQ(ltn.log->lastIndex(), before);                  // 日志没有增长
  EXPECT_EQ(restarted->clusterConfig().version, cfgIdx);
}

TEST(RaftMembership, A26_InstalledSnapshotReseatsConfigBaseline) {
  // 评审 B4（安装路径）：日志前缀被 compact 后，快照携带的配置必须无条件成为新的
  // 回滚基线，哪怕 version 更小——否则会保留一条已经不在日志里的"在途配置"（拓扑泄漏）。
  MemoryLogStore log;
  KvStateMachine sm;
  MemorySnapshotStore snaps;
  MemoryTransport transport;
  FakeClock clock;
  const ClusterConfig seed = test::makeSeedConfig(3);

  // 日志里一条更新的配置条目（v5，去掉了 3 号）
  ClusterConfig v5 = seed;
  v5.version = 5;
  v5.members.erase(std::remove_if(v5.members.begin(), v5.members.end(),
                                  [](const Member& m) { return m.id == 3; }),
                   v5.members.end());
  LogEntry ce;
  ce.index = 1;
  ce.term = 1;
  ce.op = OpCode::kConfig;
  ce.value = toStr(encodeClusterConfig(v5));
  ASSERT_TRUE(log.append({ce}));

  RaftConfig cfg;
  cfg.selfId = 1;
  cfg.snapshotThresholdEntries = 1000000;
  cfg.snapshotChunkBytes = 64;
  RaftNode node(cfg, log, sm, transport, clock, &snaps, seed);
  ASSERT_EQ(node.configVersion(), static_cast<uint64_t>(5));
  ASSERT_FALSE(node.clusterConfig().contains(3));  // v5 已把 3 号移出配置

  // 构造一个边界更大（index=8）、配置更旧（v0）的合法快照，直接走安装路径
  KvStateMachine srcSm;  // 生成合法 payload
  LogEntry put;
  put.index = 8;
  put.term = 1;
  put.op = OpCode::kPut;
  put.key = "k";
  put.value = "v";
  put.clientId = 1;
  put.requestId = 1;
  srcSm.apply(put);

  SnapshotData sd;
  sd.lastIncludedIndex = 8;
  sd.lastIncludedTerm = 1;
  sd.payload = srcSm.snapshotView()->serialize();
  sd.config = encodeClusterConfig(seed);  // v0：3 个成员都在

  InstallSnapshotArgs args;
  args.term = node.currentTerm();
  args.leaderId = 2;
  args.lastIncludedIndex = 8;
  args.lastIncludedTerm = 1;
  args.offset = 0;
  args.done = true;
  args.data = encodeSnapshotFile(sd);
  ASSERT_TRUE(node.onInstallSnapshot(args).success);

  EXPECT_EQ(node.lastIncludedIndex(), static_cast<Index>(8));
  EXPECT_EQ(node.configVersion(), static_cast<uint64_t>(0));  // 基线被重置
  EXPECT_TRUE(node.clusterConfig().contains(3));
  EXPECT_FALSE(node.retired());
}

TEST(RaftMembership, A27_ConcurrentMembershipChangesAreSerialized) {
  // 评审 B6：changeMembership 是 check-then-act —— 两个并发的变更都能通过
  // in-flight 检查、都进入 CatchUp，最后后一次覆盖 inFlightConfigIndex_，让前一条
  // 配置条目被隐式提交（无 C_old 多数派）；version 还是"猜出来的 index"。
  // 正确实现：整个变更串行化，第二个变更立刻被拒绝、绝不进入 CatchUp。
  auto transport = std::make_shared<GatedTransport>();
  auto c = test::makeMembershipClusterWith(3, /*appendNoop=*/true, 1000000, 64,
                                           transport);
  test::driveTicks(*c, 60, 10);
  RaftNode* leader = test::findLeader(*c);
  ASSERT_NE(leader, nullptr);
  const int lid = leader->leaderId();
  test::TestNode& ltn = c->nodes[static_cast<size_t>(lid - 1)];

  ExtraNode n4;
  ExtraNode n5;
  attachExtraNode(*c, 4, n4, test::makeSeedConfig(3));
  attachExtraNode(*c, 5, n5, test::makeSeedConfig(3));

  std::atomic<int> ok{0};
  std::atomic<int> rejected{0};
  transport->gate({4, 5});

  auto job = [&](int targetId) {
    const auto r = leader->changeMembership(
        MembershipOp::kAdd, targetId,
        "127.0.0.1:700" + std::to_string(targetId), 2000);
    if (r.status == ClientStatus::kOk) {
      ok.fetch_add(1);
    } else {
      rejected.fetch_add(1);
    }
  };

  std::thread t1(job, 4);
  for (int i = 0; i < 500 && transport->blockedDistinctPeers() < 1; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  ASSERT_EQ(transport->blockedDistinctPeers(), 1u);  // 第一个变更已在 CatchUp 中

  std::thread t2(job, 5);
  // 给第二个变更足够时间：正确实现里它会被立即拒绝，不会进入 CatchUp
  for (int i = 0; i < 150 && transport->blockedDistinctPeers() < 2; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  const size_t blocked = transport->blockedDistinctPeers();
  transport->openGate();
  t1.join();
  t2.join();
  test::driveTicks(*c, 200, 10);

  EXPECT_EQ(blocked, 1u) << "第二个成员变更不得同时进入 CatchUp（J1 串行化）";
  EXPECT_EQ(ok.load(), 1);
  EXPECT_EQ(rejected.load(), 1);

  // 日志里只能有一条配置条目，且 version 恒等于它自己的 index（设计不变量）
  int configs = 0;
  const auto all = ltn.log->slice(ltn.log->firstIndex(), 100000, 1u << 30);
  for (const LogEntry& e : all) {
    if (e.op != OpCode::kConfig) continue;
    ++configs;
    ClusterConfig cc;
    ASSERT_TRUE(decodeClusterConfig(
        reinterpret_cast<const Byte*>(e.value.data()), e.value.size(), cc));
    EXPECT_EQ(cc.version, e.index);
  }
  EXPECT_EQ(configs, 1);
}

TEST(RaftMembership, A28_RemovedPeerIsReclaimedAfterCommit) {
  // 评审 O2/O4：配置条目提交后被移除的节点不再需要送达，它的每 peer 状态
  // （nextIndex_/matchIndex_/快照进度/readAcks_）与地址簿条目必须被回收，
  // 而不是拖到下一次配置变更。提交前（在途）则必须继续送达。
  auto transport = std::make_shared<PeerBookTransport>();
  auto c = test::makeMembershipClusterWith(3, /*appendNoop=*/true, 1000000, 64,
                                           transport);
  test::driveTicks(*c, 60, 10);
  RaftNode* leader = test::findLeader(*c);
  ASSERT_NE(leader, nullptr);
  const int lid = leader->leaderId();
  const int victim = (lid == 3) ? 2 : 3;

  ASSERT_EQ(leader->changeMembership(MembershipOp::kRemove, victim, "", 2000)
                .status,
            ClientStatus::kOk);
  test::driveTicks(*c, 5, 10);

  // 提交后：地址簿里的条目必须已经被摘掉
  EXPECT_TRUE(transport->wasRemoved(victim));
  // 提交后不再给被移除节点发任何 RPC（复制目标里已经没有它）
  const int sendsAfterCommit = transport->sendsTo(victim);
  test::driveTicks(*c, 200, 10);
  EXPECT_EQ(transport->sendsTo(victim), sendsAfterCommit);

  // 仍然在配置里的节点当然不受影响（注意排除 Leader 自己：它不给自己发 RPC）
  int alive = 0;
  for (int id = 1; id <= 3; ++id) {
    if (id != victim && id != lid) alive = id;
  }
  ASSERT_NE(alive, 0);
  EXPECT_GT(transport->sendsTo(alive), 0);
  EXPECT_FALSE(transport->wasRemoved(alive));
  EXPECT_FALSE(leader->clusterConfig().contains(victim));
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
