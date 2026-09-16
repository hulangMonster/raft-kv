// M5 (#2 TDD 阶段) 契约用例：先写测试，实测 RED，再在 M5.1–M5.4 里实现。
//
// 用例编号对应 docs/m5-design.md §11 测试矩阵。
//   A 组：确定性契约（FakeClock + Memory* 适配器 + spy/blocking 桩，零 flaky）
//   #2 阶段标注：
//     [RED]   = 新增契约，当前实现必然失败（已实测，输出见提交信息）
//     [GUARD] = 既有行为的回归守门，本轮即绿（M5 改动不得让它变红）
//
// 说明：#2 阶段 metrics 是计数桩、锁探针 ProbedMutex 已接入 RaftNode，
// 因此 A1/A2/A3/A6 是 RED；B 组中依赖 M5.4 新接口的流式快照/断点续传用例
// 按子阶段 RED-first 在 M5.4 补写（见 docs/m5-design.md §15 修订记录 v1.1）。

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "raft/lock_probe.h"
#include "raft/metrics.h"
#include "raft_test_harness.h"

using namespace raftkv;
using namespace raftkv::raft;
using namespace raftkv::raft::test;

namespace {

// ---- 本地集群桩：可注入 SpyLogStore / SpyTransport --------------------------

struct SpyCluster {
  std::shared_ptr<FakeClock> clock;
  std::shared_ptr<SpyTransport> transport;
  std::vector<std::unique_ptr<SpyLogStore>> logs;
  std::vector<std::unique_ptr<KvStateMachine>> sms;
  std::vector<std::unique_ptr<RaftNode>> nodes;

  RaftNode* node(int id) { return nodes[static_cast<size_t>(id - 1)].get(); }
  SpyLogStore& log(int id) { return *logs[static_cast<size_t>(id - 1)]; }
};

SpyCluster makeSpyCluster(int n, bool appendNoop = true) {
  SpyCluster c;
  c.clock = std::make_shared<FakeClock>();
  c.transport = std::make_shared<SpyTransport>();
  for (int id = 1; id <= n; ++id) {
    auto log = std::make_unique<SpyLogStore>();
    auto sm = std::make_unique<KvStateMachine>();
    RaftConfig cfg;
    cfg.selfId = id;
    cfg.appendNoop = appendNoop;
    for (int p = 1; p <= n; ++p) {
      if (p != id) cfg.peerIds.push_back(p);
    }
    auto node = std::make_unique<RaftNode>(cfg, *log, *sm, *c.transport,
                                           *c.clock);
    c.transport->addNode(id, node.get());
    c.logs.push_back(std::move(log));
    c.sms.push_back(std::move(sm));
    c.nodes.push_back(std::move(node));
  }
  return c;
}

void driveSpy(SpyCluster& c, int count, uint64_t stepMs) {
  for (int i = 0; i < count; ++i) {
    c.clock->advance(stepMs);
    for (auto& n : c.nodes) n->tick();
  }
}

RaftNode* spyLeader(SpyCluster& c) {
  for (auto& n : c.nodes) {
    if (n->role() == Role::kLeader) return n.get();
  }
  return nullptr;
}

ClientRequest put(uint64_t rid, const std::string& k, const std::string& v) {
  ClientRequest r;
  r.op = OpCode::kPut;
  r.key = k;
  r.value = v;
  r.clientId = 1;
  r.requestId = rid;
  return r;
}

// sync() 第一次失败的 store（I10 回归守门）。
class FailingSyncLogStore : public MemoryLogStore {
 public:
  bool sync() override {
    if (failNext_) {
      failNext_ = false;
      return false;
    }
    return MemoryLogStore::sync();
  }
  void failNextSync() { failNext_ = true; }

 private:
  bool failNext_ = false;
};

}  // namespace

// ============================ A 组：契约 ============================

// M5.A1 [RED] I9：持 mu_ 期间零 durable 调用、零网络发送。
TEST(RaftPerf, A1_LockHeldDurableCallsAndSendsAreZero) {
  auto c = makeSpyCluster(3);
  driveSpy(c, 60, 10);
  RaftNode* leader = spyLeader(c);
  ASSERT_NE(leader, nullptr);
  const int lid = leader->leaderId();

  for (int i = 1; i <= 20; ++i) {
    leader->propose(put(static_cast<uint64_t>(i), "k" + std::to_string(i), "v"),
                    1000);
  }
  driveSpy(c, 40, 10);
  ASSERT_GE(leader->commitIndex(), static_cast<Index>(20));

  // 先证明路径确实跑过（避免空转通过）
  EXPECT_GT(c.log(lid).syncs, 0);
  EXPECT_GT(c.transport->sends, 0);  // 发送路径确实跑过（避免空转通过）

  int lockedDurable = 0;
  for (auto& l : c.logs) lockedDurable += l->lockedDurableCalls();
  EXPECT_EQ(lockedDurable, 0)
      << "I9：持 mu_ 期间发生了 fsync/persistMeta/append(durable)/compact";
  EXPECT_EQ(c.transport->lockedSends, 0) << "I9：持 mu_ 期间发生了网络发送";
}

// M5.A2 [RED] I9 + I10：#2 阶段实测发现——follower 的新条目路径**从不显式调用 sync()**，
// 它依赖 `LogStore::append()` 内含 fsync 的实现细节（FileLogStore 有、MemoryLogStore 没有），
// 因此"durable 之后再 ack"这条契约在 Memory* 适配器上根本无法表达。
// M5.2 必须改成显式 `appendNoSync()` + 锁外 `sync()`：本用例断言"进入 sync 时锁必须空闲"。
TEST(RaftPerf, A2_LockIsFreeWhileFsyncInProgress) {
  auto c = std::make_shared<FakeClock>();
  auto transport = std::make_shared<MemoryTransport>();
  auto leaderLog = std::make_unique<SpyLogStore>();
  auto followerLog = std::make_unique<BlockingLogStore>();
  auto leaderSm = std::make_unique<KvStateMachine>();
  auto followerSm = std::make_unique<KvStateMachine>();
  BlockingLogStore* blocked = followerLog.get();

  RaftConfig cfg1;
  cfg1.selfId = 1;
  cfg1.peerIds = {2};
  RaftNode leader(cfg1, *leaderLog, *leaderSm, *transport, *c);
  RaftConfig cfg2;
  cfg2.selfId = 2;
  cfg2.peerIds = {1};
  RaftNode follower(cfg2, *followerLog, *followerSm, *transport, *c);
  transport->addNode(1, &leader);
  transport->addNode(2, &follower);

  for (int i = 0; i < 60 && leader.role() != Role::kLeader; ++i) {
    c->advance(10);
    leader.tick();
    follower.tick();
  }
  ASSERT_EQ(leader.role(), Role::kLeader);

  // 让 follower 的下一次 sync 阻塞；写请求会在"leader 复制到 follower"时命中它
  blocked->blockOnSync(true);
  std::atomic<bool> proposeDone{false};
  std::thread proposer([&] {
    (void)leader.propose(put(1, "k", "v"), 1500);
    proposeDone.store(true);
  });

  const bool enteredSync = blocked->waitInsideSync(3000);
  bool lockWasFree = false;
  bool heldAtEntry = true;
  if (enteredSync) {
    heldAtEntry = blocked->heldWhileSyncEntered();
    std::atomic<bool> observed{false};
    std::thread observer([&] {
      (void)follower.currentTerm();  // 需要拿 mu_
      observed.store(true);
    });
    for (int i = 0; i < 60 && !observed.load(); ++i) {
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    lockWasFree = observed.load();
    blocked->release();
    if (observer.joinable()) observer.join();
  }
  blocked->release();  // 兜底，避免提早失败时挂住线程
  proposer.join();
  blocked->blockOnSync(false);

  EXPECT_TRUE(enteredSync) << "未进入 follower 的 sync()";
  EXPECT_FALSE(heldAtEntry) << "I9：follower 的 fsync 发生在持锁状态";
  EXPECT_TRUE(lockWasFree) << "I9：fsync 期间 mu_ 被占，其它调用被阻塞";
}

// M5.A3 [RED] I11：persistMeta 期间锁必须空闲，且授权仍发生在 durable 之后。
TEST(RaftPerf, A3_MetaPersistOutsideLockThenGrant) {
  auto log = std::make_unique<BlockingLogStore>();
  BlockingLogStore* blocked = log.get();
  KvStateMachine sm;
  MemoryTransport transport;
  FakeClock clock;
  RaftConfig cfg;
  cfg.selfId = 1;
  cfg.peerIds = {2, 3};
  RaftNode node(cfg, *log, sm, transport, clock);

  blocked->blockOnMeta(true);

  std::atomic<bool> granted{false};
  RequestVoteArgs args;
  args.term = 1;
  args.candidateId = 2;
  args.lastLogIndex = kNoIndex;
  args.lastLogTerm = kNoTerm;
  std::thread voter([&] {
    const auto reply = node.onRequestVote(args);
    granted.store(reply.voteGranted);
  });

  const bool enteredMeta = blocked->waitInsideMeta(3000);
  bool lockWasFree = false;
  bool heldAtEntry = true;
  if (enteredMeta) {
    heldAtEntry = blocked->heldWhileMetaEntered();
    std::atomic<bool> observed{false};
    std::thread observer([&] {
      (void)node.currentTerm();
      observed.store(true);
    });
    for (int i = 0; i < 60 && !observed.load(); ++i) {
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    lockWasFree = observed.load();
    blocked->release();
    if (observer.joinable()) observer.join();
  }
  blocked->release();
  voter.join();
  blocked->blockOnMeta(false);

  EXPECT_TRUE(enteredMeta) << "未进入 persistMeta()";
  EXPECT_FALSE(heldAtEntry) << "I5/I9：persistMeta 发生在持锁状态";
  EXPECT_TRUE(lockWasFree) << "I9：persistMeta 期间 mu_ 被占";
  EXPECT_TRUE(granted.load()) << "I5：durable 之后应当授权";
}

// M5.A4 [GUARD] I10：sync 失败不得 ack、不得推进 commitIndex。
TEST(RaftPerf, A4_FailedSyncDoesNotAckOrAdvance) {
  auto c = std::make_shared<FakeClock>();
  auto transport = std::make_shared<MemoryTransport>();
  auto log = std::make_unique<FailingSyncLogStore>();
  FailingSyncLogStore* failing = log.get();
  auto sm = std::make_unique<KvStateMachine>();
  RaftConfig cfg;
  cfg.selfId = 1;
  RaftNode node(cfg, *log, *sm, *transport, *c);
  transport->addNode(1, &node);

  // 单节点集群：推进时钟并 tick 一次即当选（否则 propose 只会拿到 kNotLeader）
  c->advance(1000);
  node.tick();
  ASSERT_EQ(node.role(), Role::kLeader);

  const Index before = node.commitIndex();
  failing->failNextSync();
  const auto r1 = node.propose(put(1, "k", "v"), 100);
  EXPECT_EQ(r1.status, ClientStatus::kErr);
  EXPECT_EQ(node.commitIndex(), before);

  const auto r2 = node.propose(put(2, "k", "v"), 1000);
  EXPECT_EQ(r2.status, ClientStatus::kOk);
  EXPECT_GT(node.commitIndex(), before);
}

// M5.A5 [GUARD] R1 守门：no-op 必须能让 §8 读屏障满足（否则所有线性一致读都会失败）。
TEST(RaftPerf, A5_NoopUnblocksReadBarrier) {
  auto c = test::makeCluster(3);  // appendNoop = true
  test::driveTicks(*c, 60, 10);
  RaftNode* leader = test::findLeader(*c);
  ASSERT_NE(leader, nullptr);
  // key 不存在 -> kNotFound 表示"屏障已满足且真的读了状态机"；kErr 说明屏障没满足
  EXPECT_EQ(leader->linearizableGet("absent", 500).status,
            ClientStatus::kNotFound);
}

// M5.A6 [RED] I13：指标计数可用、可渲染、只在请求时读取。
TEST(RaftPerf, A6_MetricsCountersAndStatusFragment) {
  Metrics m;
  m.onFsync(7);
  m.onFsync(3);
  m.onBatch(4);
  m.onWriteCompleted(120);
  m.onWriteCompleted(20000);  // 20ms：必须落在 20000us 桶而不是溢出桶
  m.onElection();
  m.onSnapshot(4096);
  m.onLockWait(9);
  EXPECT_EQ(m.fsyncCalls(), 2u);
  EXPECT_EQ(m.fsyncUs(), 10u);
  EXPECT_EQ(m.batches(), 1u);
  EXPECT_EQ(m.batchEntries(), 4u);
  EXPECT_EQ(m.writes(), 2u);
  EXPECT_EQ(m.latencyP50Us(), 200u);    // {120,20000} -> 中位样本在 200us 桶
  EXPECT_EQ(m.latencyP99Us(), 20000u);  // 溢出桶不得把 ms 级延迟报成 0
  EXPECT_EQ(m.elections(), 1u);
  EXPECT_EQ(m.snapshots(), 1u);
  EXPECT_EQ(m.lockWaitUsTotal(), 9u);

  const std::string frag = m.statusFragment();
  EXPECT_NE(frag.find("fsync_calls="), std::string::npos) << frag;
  EXPECT_NE(frag.find("lat_p99_us="), std::string::npos) << frag;
  EXPECT_NE(frag.find("batch_max="), std::string::npos) << frag;
}

// M5.A7 [GUARD] R2 守门：两段式改造不得破坏成员变更语义（J1/J2 仍成立）。
TEST(RaftPerf, A7_ConfigChangeStillCommits) {
  auto c = makeSpyCluster(3);
  driveSpy(c, 60, 10);
  RaftNode* leader = spyLeader(c);
  ASSERT_NE(leader, nullptr);
  const int lid = leader->leaderId();
  const int victim = (lid == 3) ? 2 : 3;
  const uint64_t v0 = leader->configVersion();

  ASSERT_EQ(leader->changeMembership(MembershipOp::kRemove, victim, "", 2000)
                .status,
            ClientStatus::kOk);
  driveSpy(c, 60, 10);
  EXPECT_GT(leader->configVersion(), v0);
  EXPECT_FALSE(leader->clusterConfig().contains(victim));
}
