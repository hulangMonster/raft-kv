// M3 snapshot tests. Phase #2: skeleton only — all cases are expected to FAIL
// until M3.1..M3.4 land the implementations.
//
// A-group: FakeClock + MemoryTransport + MemoryLogStore + MemorySnapshotStore
// B-group: FileLogStore + FileSnapshotStore with real temp dirs
#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

#include "kv/kv_state_machine.h"
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

LogEntry entry(Index index, Term term, const std::string& key,
               const std::string& value) {
  LogEntry e;
  e.index = index;
  e.term = term;
  e.op = OpCode::kPut;
  e.key = key;
  e.value = value;
  e.clientId = 1;
  e.requestId = index;
  return e;
}

std::string tempDir() {
  static uint64_t counter = 0;
  auto p = std::filesystem::temp_directory_path() /
           ("raftkv_m3_test_" + std::to_string(::getpid()) + "_" +
            std::to_string(counter++));
  std::filesystem::create_directories(p);
  return p.string();
}

// Elects a leader and writes `n` committed entries through it.
void writeEntries(test::Cluster& c, RaftNode* leader, int n) {
  (void)c;  // kept for symmetry with cluster-based call sites
  for (int i = 1; i <= n; ++i) {
    leader->propose(putReq(static_cast<uint64_t>(i),
                           "k" + std::to_string(i), "v"),
                    1000);
  }
}

// Ticks only the listed nodes. An isolated node must NOT keep ticking in these
// tests: it would campaign, raise the term and force the leader to step down,
// which would swamp the behaviour under test.
void tickOnly(test::Cluster& c, const std::vector<int>& ids, int count,
              uint64_t stepMs) {
  for (int i = 0; i < count; ++i) {
    c.clock->advance(stepMs);
    for (const int id : ids) c.nodes[static_cast<size_t>(id - 1)].node->tick();
  }
}

}  // namespace

// ---------------------------------------------------------------- A-group ---

TEST(RaftSnapshot, SnapshotCreatedAtThreshold) {
  auto c = test::makeSnapshotCluster(3, /*threshold=*/8);
  test::driveTicks(*c, 50, 10);
  RaftNode* leader = test::findLeader(*c);
  ASSERT_NE(leader, nullptr);

  writeEntries(*c, leader, 30);
  test::driveTicks(*c, 100, 10);  // ticker must trigger+persist a snapshot

  SnapshotData snap;
  ASSERT_TRUE(c->nodes[leader->leaderId() - 1].snapshots->load(snap));
  EXPECT_GE(snap.lastIncludedIndex, static_cast<Index>(8));
}

TEST(RaftSnapshot, SnapshotOnlyCoversAppliedPrefix) {
  auto c = test::makeSnapshotCluster(3, 8);
  test::driveTicks(*c, 50, 10);
  RaftNode* leader = test::findLeader(*c);
  ASSERT_NE(leader, nullptr);

  writeEntries(*c, leader, 30);
  test::driveTicks(*c, 100, 10);

  SnapshotData snap;
  ASSERT_TRUE(c->nodes[leader->leaderId() - 1].snapshots->load(snap));
  EXPECT_LE(snap.lastIncludedIndex, leader->lastApplied());
  EXPECT_LE(snap.lastIncludedIndex, leader->commitIndex());
}

TEST(RaftSnapshot, CompactTruncatesLogAndFirstIndex) {
  auto c = test::makeSnapshotCluster(3, 8);
  test::driveTicks(*c, 50, 10);
  RaftNode* leader = test::findLeader(*c);
  ASSERT_NE(leader, nullptr);

  writeEntries(*c, leader, 30);
  test::driveTicks(*c, 100, 10);

  const auto& log = *c->nodes[leader->leaderId() - 1].log;
  ASSERT_GT(log.lastIncludedIndex(), kNoIndex);
  EXPECT_EQ(log.firstIndex(), log.lastIncludedIndex() + 1);
  EXPECT_LE(log.lastIndex() - log.firstIndex() + 1, static_cast<Index>(30));
}

TEST(RaftSnapshot, TermAtCompactedBoundarySemantics) {
  auto c = test::makeSnapshotCluster(3, 8);
  test::driveTicks(*c, 50, 10);
  RaftNode* leader = test::findLeader(*c);
  ASSERT_NE(leader, nullptr);

  writeEntries(*c, leader, 30);
  test::driveTicks(*c, 100, 10);

  const auto& log = *c->nodes[leader->leaderId() - 1].log;
  ASSERT_GT(log.lastIncludedIndex(), kNoIndex);
  EXPECT_EQ(log.termAt(log.lastIncludedIndex()), log.lastIncludedTerm());
  if (log.lastIncludedIndex() > 1) {
    EXPECT_EQ(log.termAt(log.lastIncludedIndex() - 1), kNoTerm);
  }
  // slice must clamp below the boundary: compacted entries never come back.
  // (A fully compacted log may legitimately have no entries left.)
  const auto clamped = log.slice(1, 8, 1u << 20);
  for (const auto& e : clamped) {
    EXPECT_GE(e.index, log.firstIndex());
  }
}

TEST(RaftSnapshot, InstallSnapshotCatchesUpLaggingFollower) {
  auto c = test::makeSnapshotCluster(3, 8);
  test::driveTicks(*c, 50, 10);
  RaftNode* leader = test::findLeader(*c);
  ASSERT_NE(leader, nullptr);

  // Pick one follower and keep it behind while the leader writes + compacts.
  const int lagging = (leader->leaderId() == 1) ? 2 : 1;
  c->transport->isolate(lagging);
  writeEntries(*c, leader, 30);
  // Tick only the reachable nodes: the isolated one must not campaign (see
  // tickOnly) or the leader would step down mid-test.
  std::vector<int> reachable;
  for (int id = 1; id <= 3; ++id) {
    if (id != lagging) reachable.push_back(id);
  }
  tickOnly(*c, reachable, 100, 10);
  ASSERT_EQ(leader->role(), Role::kLeader);

  SnapshotData snap;
  ASSERT_TRUE(c->nodes[leader->leaderId() - 1].snapshots->load(snap));
  const Index boundary = snap.lastIncludedIndex;
  ASSERT_GT(boundary, kNoIndex);

  InstallSnapshotArgs args;
  args.term = leader->currentTerm();
  args.leaderId = leader->leaderId();
  args.lastIncludedIndex = boundary;
  args.lastIncludedTerm = snap.lastIncludedTerm;
  args.offset = 0;
  args.done = true;
  args.data = encodeSnapshotFile(snap);

  RaftNode* follower = c->nodes[lagging - 1].node.get();
  const auto reply = follower->onInstallSnapshot(args);
  EXPECT_TRUE(reply.success);
  EXPECT_EQ(follower->lastIncludedIndex(), boundary);
  EXPECT_GE(follower->commitIndex(), boundary);
}

TEST(RaftSnapshot, FollowerKeepsSuffixBeyondSnapshot) {
  // Large threshold: snapshot only on explicit trigger, so the follower keeps
  // lastIncluded == 0 while its log already has entries beyond the boundary.
  auto c = test::makeSnapshotCluster(3, /*threshold=*/1000000);
  test::driveTicks(*c, 50, 10);
  RaftNode* leader = test::findLeader(*c);
  ASSERT_NE(leader, nullptr);

  writeEntries(*c, leader, 20);
  leader->triggerSnapshot();
  test::driveTicks(*c, 100, 10);
  SnapshotData snap;
  ASSERT_TRUE(c->nodes[leader->leaderId() - 1].snapshots->load(snap));
  const Index boundary = snap.lastIncludedIndex;
  ASSERT_GT(boundary, kNoIndex);

  writeEntries(*c, leader, 10);  // indices boundary+1 .. boundary+10
  test::driveTicks(*c, 100, 10);

  const int followerId = (leader->leaderId() == 1) ? 2 : 1;
  RaftNode* follower = c->nodes[followerId - 1].node.get();
  ASSERT_EQ(follower->lastIncludedIndex(), kNoIndex);  // never snapshotted

  InstallSnapshotArgs args;
  args.term = leader->currentTerm();
  args.leaderId = leader->leaderId();
  args.lastIncludedIndex = boundary;
  args.lastIncludedTerm = snap.lastIncludedTerm;
  args.offset = 0;
  args.done = true;
  args.data = encodeSnapshotFile(snap);

  const auto reply = follower->onInstallSnapshot(args);
  EXPECT_TRUE(reply.success);
  EXPECT_EQ(follower->lastIncludedIndex(), boundary);
  // Entries beyond the snapshot must be preserved.
  EXPECT_GT(c->nodes[followerId - 1].log->lastIndex(), boundary);
}

TEST(RaftSnapshot, StaleSnapshotIgnored) {
  auto c = test::makeSnapshotCluster(3, 8);
  test::driveTicks(*c, 50, 10);
  RaftNode* leader = test::findLeader(*c);
  ASSERT_NE(leader, nullptr);
  writeEntries(*c, leader, 30);
  test::driveTicks(*c, 100, 10);

  SnapshotData snap;
  ASSERT_TRUE(c->nodes[leader->leaderId() - 1].snapshots->load(snap));
  const Index boundary = snap.lastIncludedIndex;
  ASSERT_GT(boundary, kNoIndex);

  const int followerId = (leader->leaderId() == 1) ? 2 : 1;
  RaftNode* follower = c->nodes[followerId - 1].node.get();

  // Control: a fresh snapshot is accepted.
  InstallSnapshotArgs fresh;
  fresh.term = leader->currentTerm();
  fresh.leaderId = leader->leaderId();
  fresh.lastIncludedIndex = boundary;
  fresh.lastIncludedTerm = snap.lastIncludedTerm;
  fresh.done = true;
  fresh.data = encodeSnapshotFile(snap);
  EXPECT_TRUE(follower->onInstallSnapshot(fresh).success);

  // Stale: a snapshot at/below the follower's own boundary is ignored.
  InstallSnapshotArgs stale = fresh;
  stale.lastIncludedIndex = boundary;
  EXPECT_FALSE(follower->onInstallSnapshot(stale).success);
  EXPECT_EQ(follower->lastIncludedIndex(), boundary);
}

TEST(RaftSnapshot, Sync542StillHoldsAcrossCompaction) {
  auto c = test::makeSnapshotCluster(3, 8);
  test::driveTicks(*c, 50, 10);
  RaftNode* leader = test::findLeader(*c);
  ASSERT_NE(leader, nullptr);

  writeEntries(*c, leader, 30);
  test::driveTicks(*c, 100, 10);
  ASSERT_GT(leader->lastIncludedIndex(), kNoIndex);

  const auto reply = leader->propose(putReq(1000, "post-compact", "v"), 1000);
  EXPECT_EQ(reply.status, ClientStatus::kOk);
  EXPECT_GT(leader->commitIndex(), leader->lastIncludedIndex());
  EXPECT_LE(leader->commitIndex(), leader->lastApplied());
}

TEST(RaftSnapshot, ProposeNotLeaderUnaffectedBySnapshot) {
  auto c = test::makeSnapshotCluster(3, 8);
  test::driveTicks(*c, 50, 10);
  RaftNode* leader = test::findLeader(*c);
  ASSERT_NE(leader, nullptr);

  writeEntries(*c, leader, 30);
  leader->triggerSnapshot();  // manual trigger path
  test::driveTicks(*c, 100, 10);
  ASSERT_GT(leader->lastIncludedIndex(), kNoIndex);

  RequestVoteArgs higher;
  higher.term = leader->currentTerm() + 5;
  // M5.2：非成员候选者不再被采纳任期，改用配置内成员（前提修正，语义不变）
  higher.candidateId = 2;
  leader->onRequestVote(higher);

  const auto reply = leader->propose(putReq(2000, "x", "y"), 100);
  EXPECT_EQ(reply.status, ClientStatus::kNotLeader);
  EXPECT_GT(leader->lastIncludedIndex(), kNoIndex);
}

TEST(RaftSnapshot, DedupTableSurvivesSnapshot) {
  KvStateMachine sm;
  LogEntry e = entry(1, 1, "k", "v");
  e.clientId = 7;
  e.requestId = 42;
  sm.apply(e);

  const auto view = sm.snapshotView();
  ASSERT_NE(view, nullptr);
  const Bytes payload = view->serialize();

  KvStateMachine restored;
  ASSERT_TRUE(restored.restore(payload));

  // Same (clientId, requestId) replayed after restore must be deduplicated.
  LogEntry dup = e;
  dup.value = "MUST-NOT-APPLY";
  restored.apply(dup);

  std::string out;
  EXPECT_TRUE(restored.get("k", out));
  EXPECT_EQ(out, "v");
}

// ---------------------------------------------------------------- B-group ---

TEST(RaftSnapshotDisk, RestartLoadsSnapshotThenReplaysTail) {
  const std::string dir = tempDir();
  {
    FileLogStore log(dir);
    std::vector<LogEntry> head;
    for (Index i = 1; i <= 5; ++i) head.push_back(entry(i, 1, "a", "1"));
    ASSERT_TRUE(log.append(head));

    FileSnapshotStore snap(dir);
    SnapshotData d;
    d.lastIncludedIndex = 5;
    d.lastIncludedTerm = 1;
    d.payload = Bytes{'p', 'a', 'y'};
    ASSERT_TRUE(snap.save(d));
  }
  {
    FileSnapshotStore snap(dir);
    SnapshotData d;
    ASSERT_TRUE(snap.load(d));
    EXPECT_EQ(d.lastIncludedIndex, 5);

    FileLogStore log(dir);
    log.setBoundary(d.lastIncludedIndex, d.lastIncludedTerm);
    std::vector<LogEntry> tail;
    for (Index i = 6; i <= 8; ++i) tail.push_back(entry(i, 2, "b", "2"));
    ASSERT_TRUE(log.append(tail));

    Term term = 0;
    int votedFor = -1;
    Index lastIndex = 0;
    ASSERT_TRUE(log.load(term, votedFor, lastIndex));
    EXPECT_EQ(lastIndex, 8);
    EXPECT_EQ(log.firstIndex(), 6);
  }
  std::filesystem::remove_all(dir);
}

TEST(RaftSnapshotDisk, TornSnapshotDiscarded) {
  const std::string dir = tempDir();
  FileSnapshotStore snap(dir);
  SnapshotData d;
  d.lastIncludedIndex = 5;
  d.lastIncludedTerm = 1;
  d.payload = Bytes{'p', 'a', 'y'};
  ASSERT_TRUE(snap.save(d));  // control: a valid snapshot loads

  SnapshotData loaded;
  EXPECT_TRUE(snap.load(loaded));

  // Corrupt the snapshot file -> load must discard it.
  {
    std::ofstream out(dir + "/raft/snapshot.dat",
                      std::ios::binary | std::ios::trunc);
    const char junk[] = "corrupt";
    out.write(junk, sizeof(junk) - 1);
  }
  FileSnapshotStore snap2(dir);
  SnapshotData bad;
  EXPECT_FALSE(snap2.load(bad));
  std::filesystem::remove_all(dir);
}


// ===== M5.4（§8.3 流式序列化 / §8.4 断点续传）=====

namespace {

// 造一个有大 payload 的 state machine（每条 value 8KiB，便于验证分块路径）
std::unique_ptr<KvStateMachine> bigStateMachine(size_t entries) {
  auto sm = std::make_unique<KvStateMachine>();
  const std::string value(8 * 1024, 'v');
  for (size_t i = 0; i < entries; ++i) {
    LogEntry e = entry(static_cast<Index>(i + 1), 1,
                       "k" + std::to_string(i), value);
    sm->apply(e);
  }
  return sm;
}

}  // namespace

// §8.3 保真：分块流的字节序列必须与 serialize() 逐字节一致（否则磁盘格式就变了）
TEST(RaftSnapshotStream, ChunkedStreamMatchesSerializeByteForByte) {
  auto sm = bigStateMachine(64);  // ~512 KiB payload，跨越多个 64KiB 块
  auto view = sm->snapshotView();
  ASSERT_NE(view, nullptr);
  const Bytes oneShot = view->serialize();
  Bytes streamed;
  std::unique_ptr<SnapshotStream> st = view->stream();
  Bytes chunk;
  size_t maxSeen = 0;
  while (st->next(chunk, 64 * 1024)) {
    maxSeen = std::max(maxSeen, chunk.size());
    streamed.insert(streamed.end(), chunk.begin(), chunk.end());
  }
  EXPECT_EQ(streamed, oneShot) << "分块流与整块序列化必须逐字节相同";
  EXPECT_LE(maxSeen, 64 * 1024 + 8 * 1024 + 16)
      << "每块不得超过 maxChunk + 单条记录开销（实测 " << maxSeen << "）";
}

// §8.3 落盘保真：saveStreaming 写出的文件必须与 encodeSnapshotFile 完全一致（I14）
TEST(RaftSnapshotStream, SaveStreamingWritesIdenticalFile) {
  const std::string dir = tempDir();
  auto sm = bigStateMachine(32);
  auto view = sm->snapshotView();
  const Bytes cfg = Bytes{'c', 'f', 'g'};
  FileSnapshotStore snap(dir);
  ASSERT_TRUE(snap.saveStreaming(*view, /*lastIncludedIndex=*/99,
                                 /*lastIncludedTerm=*/7, cfg));
  EXPECT_EQ(snap.installedBytes(), std::filesystem::file_size(dir + "/raft/snapshot.dat"));

  // 期望文件：用同一份数据走老的整块编码
  SnapshotData expect;
  expect.lastIncludedIndex = 99;
  expect.lastIncludedTerm = 7;
  expect.config = cfg;
  expect.payload = view->serialize();
  const Bytes want = encodeSnapshotFile(expect);
  Bytes got;
  {
    std::ifstream in(dir + "/raft/snapshot.dat", std::ios::binary);
    got.assign(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
  }
  EXPECT_EQ(got, want) << "流式落盘的文件必须与整块编码逐字节一致";

  // 再读回来验一遍语义
  SnapshotData loaded;
  ASSERT_TRUE(snap.load(loaded));
  EXPECT_EQ(loaded.lastIncludedIndex, 99u);
  EXPECT_EQ(loaded.lastIncludedTerm, 7u);
  EXPECT_EQ(loaded.payload, expect.payload);
  EXPECT_EQ(loaded.config, cfg);
  std::filesystem::remove_all(dir);
}

// §8.3 惰性供片：readInstalled 必须按偏移返回文件里的对应切片（InstallSnapshot 用）
TEST(RaftSnapshotStream, ReadInstalledSlicesMatchFileContent) {
  const std::string dir = tempDir();
  auto sm = bigStateMachine(16);
  auto view = sm->snapshotView();
  FileSnapshotStore snap(dir);
  ASSERT_TRUE(snap.saveStreaming(*view, 42, 3, Bytes{}));
  const uint64_t total = snap.installedBytes();
  ASSERT_GT(total, 200u);

  Bytes head;
  ASSERT_TRUE(snap.readInstalled(0, 64, head));
  EXPECT_EQ(head.size(), 64u);
  EXPECT_EQ(std::string(head.begin(), head.begin() + 4), "RKS1");

  Bytes mid;
  ASSERT_TRUE(snap.readInstalled(100, 200, mid));
  Bytes all;
  ASSERT_TRUE(snap.readInstalled(0, total, all));
  EXPECT_EQ(std::string(mid.begin(), mid.end()),
            std::string(all.begin() + 100, all.begin() + 300));

  Bytes over;
  EXPECT_FALSE(snap.readInstalled(total - 1, 8, over)) << "越界必须拒绝";
  std::filesystem::remove_all(dir);
}

// §8.4 跨进程重启续传：新 store 实例（= 新进程）必须能从 .recv 尾部恢复进度并接着收
TEST(RaftSnapshotResume, ContinuesAfterStoreRestart) {
  const std::string dir = tempDir();
  auto sm = bigStateMachine(8);
  auto view = sm->snapshotView();
  SnapshotData full;
  full.lastIncludedIndex = 21;
  full.lastIncludedTerm = 2;
  full.payload = view->serialize();
  const Bytes file = encodeSnapshotFile(full);  // 线上传的就是这个文件内容
  ASSERT_GT(file.size(), 64u);

  const size_t firstLen = file.size() / 2;
  {
    FileSnapshotStore a(dir);
    Bytes first(file.begin(), file.begin() + static_cast<ptrdiff_t>(firstLen));
    ASSERT_TRUE(a.receiveChunk(21, 2, /*offset=*/0, first, /*done=*/false));
    EXPECT_EQ(a.recvProgress(), firstLen);
    // 让 a 析构：等价于接收方进程重启（.recv 与尾部元数据留在磁盘上）
  }
  {
    FileSnapshotStore b(dir);  // 新实例 = 重启后的进程
    EXPECT_EQ(b.recvProgress(), firstLen)
        << "新实例必须从 .recv 尾部恢复已收字节数";
    Bytes second(file.begin() + static_cast<ptrdiff_t>(firstLen), file.end());
    ASSERT_TRUE(b.receiveChunk(21, 2, firstLen, second, /*done=*/true))
        << "必须能从断点继续接收";
    SnapshotData loaded;
    ASSERT_TRUE(b.load(loaded));
    EXPECT_EQ(loaded.lastIncludedIndex, 21u);
    EXPECT_EQ(loaded.payload, full.payload) << "续传后安装的载荷必须完整";
  }

  // 对照：换一份更**新**的快照，重启后从头传（offset=0）仍然要能安装
  {
    SnapshotData newer = full;
    newer.lastIncludedIndex = 30;
    newer.lastIncludedTerm = 4;
    const Bytes file30 = encodeSnapshotFile(newer);
    FileSnapshotStore c(dir);
    ASSERT_TRUE(c.receiveChunk(30, 4, 0, file30, /*done=*/true));
    SnapshotData loaded;
    ASSERT_TRUE(c.load(loaded));
    EXPECT_EQ(loaded.lastIncludedIndex, 30u);
  }

  // 对照：从**错误的**偏移续传必须被拒（并要求 leader 从头来）
  {
    SnapshotData newer = full;
    newer.lastIncludedIndex = 40;
    newer.lastIncludedTerm = 5;
    const Bytes file40 = encodeSnapshotFile(newer);
    const size_t half = file40.size() / 2;
    FileSnapshotStore d(dir);
    Bytes first(file40.begin(), file40.begin() + static_cast<ptrdiff_t>(half));
    ASSERT_TRUE(d.receiveChunk(40, 5, 0, first, false));
    Bytes tail(file40.begin() + static_cast<ptrdiff_t>(half), file40.end());
    EXPECT_FALSE(d.receiveChunk(40, 5, half + 7, tail, true))
        << "偏移对不上必须拒绝，而不是拼出错文件";
  }
  std::filesystem::remove_all(dir);
}
TEST(RaftSnapshotDisk, TornTailAfterCompact) {
  const std::string dir = tempDir();
  {
    FileLogStore log(dir);
    std::vector<LogEntry> entries;
    for (Index i = 1; i <= 5; ++i) entries.push_back(entry(i, 1, "a", "1"));
    ASSERT_TRUE(log.append(entries));
    ASSERT_TRUE(log.compact(3, 1));  // drop prefix <= 3
    EXPECT_EQ(log.firstIndex(), 4);
  }
  {
    // Append garbage (torn tail) after compaction.
    std::ofstream out(dir + "/raft/raft.log", std::ios::binary | std::ios::app);
    const char junk[] = "torn-tail";
    out.write(junk, sizeof(junk) - 1);
  }
  {
    FileLogStore log(dir);
    log.setBoundary(3, 1);
    Term term = 0;
    int votedFor = -1;
    Index lastIndex = 0;
    ASSERT_TRUE(log.load(term, votedFor, lastIndex));
    EXPECT_EQ(lastIndex, 5);
    EXPECT_EQ(log.firstIndex(), 4);
  }
  std::filesystem::remove_all(dir);
}

TEST(RaftSnapshotDisk, SnapshotAndLogCombinedRecovery) {
  const std::string dir = tempDir();
  {
    FileLogStore log(dir);
    std::vector<LogEntry> head;
    for (Index i = 1; i <= 6; ++i) head.push_back(entry(i, 1, "a", "1"));
    ASSERT_TRUE(log.append(head));

    FileSnapshotStore snap(dir);
    SnapshotData d;
    d.lastIncludedIndex = 4;
    d.lastIncludedTerm = 1;
    d.payload = Bytes{'k', 'v'};
    ASSERT_TRUE(snap.save(d));
    ASSERT_TRUE(log.compact(4, 1));
  }
  {
    // Restart sequence: boundary first (from the snapshot), then load, then append.
    FileLogStore log(dir);
    log.setBoundary(4, 1);
    Term t = 0;
    int v = -1;
    Index last = 0;
    ASSERT_TRUE(log.load(t, v, last));
    ASSERT_EQ(last, 6);

    std::vector<LogEntry> tail;
    for (Index i = 7; i <= 9; ++i) tail.push_back(entry(i, 2, "b", "2"));
    ASSERT_TRUE(log.append(tail));
  }
  {
    FileSnapshotStore snap(dir);
    SnapshotData d;
    ASSERT_TRUE(snap.load(d));
    EXPECT_EQ(d.lastIncludedIndex, 4);

    FileLogStore log(dir);
    log.setBoundary(d.lastIncludedIndex, d.lastIncludedTerm);
    Term term = 0;
    int votedFor = -1;
    Index lastIndex = 0;
    ASSERT_TRUE(log.load(term, votedFor, lastIndex));
    EXPECT_EQ(lastIndex, 9);
    EXPECT_EQ(log.firstIndex(), 5);
  }
  std::filesystem::remove_all(dir);
}

// ------------------------------------------- review round (M3 + group commit) ---

namespace {

// Delivers InstallSnapshot but silently drops some replies: the follower
// applied the chunk, the leader just never learns about it (lost RPC reply).
class DropInstallReplyTransport : public MemoryTransport {
 public:
  void sendInstallSnapshot(int peerId, const InstallSnapshotArgs& args,
                           InstallCb cb) override {
    ++sends_;
    if (sends_ > allow_ && drops_ > 0) {
      --drops_;
      MemoryTransport::sendInstallSnapshot(
          peerId, args, [](const InstallSnapshotReply&) {});
      return;
    }
    MemoryTransport::sendInstallSnapshot(peerId, args, std::move(cb));
  }

  // Let the first `allow` sends through, then lose `count` replies in a row.
  // Losing a MIDDLE chunk (offset > 0) is what used to wedge the transfer.
  void loseRepliesAfter(int allow, int count) {
    allow_ = allow;
    drops_ = count;
    sends_ = 0;
  }

 private:
  int allow_ = 0;
  int drops_ = 0;
  int sends_ = 0;
};

}  // namespace

TEST(RaftSnapshot, SingleNodeProposeCommitsWithoutPeers) {
  // B1: with group commit the flusher has no peer jobs in a single-node
  // cluster, so it must advance the commit point itself (majority == self).
  auto c = test::makeCluster(1);
  RaftNode* n = c->nodes[0].node.get();
  c->clock->advance(400);
  n->tick();  // campaigns and wins (single-node majority == self)
  n->tick();  // leader appends + commits its no-op
  ASSERT_EQ(n->role(), Role::kLeader);
  ASSERT_GE(n->commitIndex(), static_cast<Index>(1));

  const auto reply = n->propose(putReq(1, "solo", "v"), 200);
  EXPECT_EQ(reply.status, ClientStatus::kOk);
  EXPECT_GE(n->commitIndex(), static_cast<Index>(2));
  EXPECT_EQ(n->lastApplied(), n->commitIndex());

  std::string out;
  ASSERT_TRUE(static_cast<KvStateMachine*>(c->nodes[0].sm.get())->get("solo", out));
  EXPECT_EQ(out, "v");
}

TEST(RaftSnapshot, InstallSnapshotRetransmitAfterLostReplies) {
  // B7: a lost chunk reply makes the leader resend the SAME chunk. The follower
  // must treat it as a duplicate (idempotent) instead of appending it twice,
  // which used to wedge the transfer forever.
  auto transport = std::make_shared<DropInstallReplyTransport>();
  auto c = test::makeSnapshotClusterWith(3, /*threshold=*/8, /*chunkBytes=*/64,
                                         transport);
  test::driveTicks(*c, 50, 10);
  RaftNode* leader = test::findLeader(*c);
  ASSERT_NE(leader, nullptr);
  const int leaderId = leader->leaderId();
  const int lagId = (leaderId % 3) + 1;

  c->transport->isolate(lagId);
  writeEntries(*c, leader, 30);
  tickOnly(*c, {leaderId}, 400, 10);  // leader snapshots + compacts
  ASSERT_GT(leader->lastIncludedIndex(), kNoIndex);

  // Let two chunks land, then lose the replies of the next two (a middle chunk
  // with offset > 0): the leader resends that same chunk.
  transport->loseRepliesAfter(2, 2);
  c->transport->heal(lagId);
  tickOnly(*c, {leaderId}, 600, 10);

  RaftNode* lag = c->nodes[static_cast<size_t>(lagId - 1)].node.get();
  EXPECT_EQ(lag->lastIncludedIndex(), leader->lastIncludedIndex());
  EXPECT_EQ(lag->lastApplied(), leader->lastApplied());

  std::string out;
  ASSERT_TRUE(
      static_cast<KvStateMachine*>(c->nodes[static_cast<size_t>(lagId - 1)].sm.get())
          ->get("k1", out));
}

TEST(RaftSnapshot, InstalledNodeCanServeSnapshotAsLeader) {
  // B6: a node whose only knowledge is an INSTALLED snapshot must be able to
  // serve that snapshot once it becomes leader (snapshotBytes_ used to stay
  // empty, so it fell back to AppendEntries into the compacted region forever).
  auto c = test::makeSnapshotCluster(5, /*threshold=*/1000, /*chunkBytes=*/64);
  test::driveTicks(*c, 80, 10);
  RaftNode* a = test::findLeader(*c);
  ASSERT_NE(a, nullptr);
  const int aid = a->leaderId();
  int bid = 0;
  int eid = 0;
  for (int id = 1; id <= 5; ++id) {
    if (id == aid) continue;
    if (bid == 0) {
      bid = id;
    } else if (eid == 0) {
      eid = id;
    }
  }
  ASSERT_NE(bid, 0);
  ASSERT_NE(eid, 0);

  std::vector<int> majority = {aid};
  for (int id = 1; id <= 5; ++id) {
    if (id != bid && id != eid) majority.push_back(id);
  }

  // Phase 1: everyone receives the first batch.
  writeEntries(*c, a, 10);
  tickOnly(*c, majority, 20, 10);

  // Phase 2: B and E fall behind while A (plus the two remaining nodes) commits
  // more and then snapshots past their log.
  c->transport->isolate(bid);
  c->transport->isolate(eid);
  for (int i = 11; i <= 30; ++i) {
    ASSERT_EQ(a->propose(putReq(static_cast<uint64_t>(i), "k" + std::to_string(i),
                                "v"),
                         1000)
                  .status,
              ClientStatus::kOk);
  }
  a->triggerSnapshot();
  tickOnly(*c, {aid}, 10, 10);
  const Index boundary = a->lastIncludedIndex();
  ASSERT_GE(boundary, static_cast<Index>(30));

  // Phase 3: B rejoins and can only catch up by installing A's snapshot.
  c->transport->heal(bid);
  tickOnly(*c, {aid}, 600, 10);
  RaftNode* b = c->nodes[static_cast<size_t>(bid - 1)].node.get();
  ASSERT_EQ(b->lastIncludedIndex(), boundary);

  // Phase 4: cut B off from A's heartbeats and run ONLY B: it times out,
  // campaigns with a higher term and wins (its log equals the others' after the
  // install), so it becomes leader knowing nothing but the snapshot.
  c->transport->isolate(bid);
  for (int i = 0; i < 400 && b->role() != Role::kLeader; ++i) {
    c->clock->advance(10);
    b->tick();
  }
  ASSERT_EQ(b->role(), Role::kLeader);

  // Phase 5: E rejoins - B must serve the snapshot it installed.
  c->transport->heal(eid);
  tickOnly(*c, {bid}, 800, 10);
  RaftNode* e = c->nodes[static_cast<size_t>(eid - 1)].node.get();
  EXPECT_EQ(e->lastIncludedIndex(), boundary);
  // The snapshot covers `boundary`; B's own no-op (boundary + 1) may follow it.
  EXPECT_GE(e->lastApplied(), boundary);

  std::string out;
  ASSERT_TRUE(
      static_cast<KvStateMachine*>(c->nodes[static_cast<size_t>(eid - 1)].sm.get())
          ->get("k1", out));
}

TEST(RaftSnapshotStore, StaleSaveNeverOverwritesNewerInstall) {
  // B5: a snapshot produced concurrently with a newer InstallSnapshot must not
  // move the durable boundary backwards.
  auto store = std::make_unique<MemorySnapshotStore>();
  SnapshotData newer;
  newer.lastIncludedIndex = 9;
  newer.lastIncludedTerm = 3;
  newer.payload = Bytes{'n', 'e', 'w'};
  ASSERT_TRUE(store->receiveChunk(9, 3, 0, encodeSnapshotFile(newer),
                                  /*done=*/true));

  SnapshotData older;
  older.lastIncludedIndex = 5;
  older.lastIncludedTerm = 2;
  older.payload = Bytes{'o', 'l', 'd'};
  EXPECT_FALSE(store->save(older));

  SnapshotData out;
  ASSERT_TRUE(store->load(out));
  EXPECT_EQ(out.lastIncludedIndex, static_cast<Index>(9));
  EXPECT_EQ(out.payload, newer.payload);
}

TEST(RaftSnapshotStore, ReceiveChunkIsIdempotentForRetransmits) {
  // B7 (receiver side): only offset == writtenEnd appends; a chunk that is
  // fully inside what we already have is a no-op success.
  auto store = std::make_unique<MemorySnapshotStore>();
  SnapshotData snap;
  snap.lastIncludedIndex = 7;
  snap.lastIncludedTerm = 2;
  snap.payload.assign(200, 'p');
  const Bytes file = encodeSnapshotFile(snap);
  const size_t chunk = 64;
  ASSERT_GT(file.size(), chunk * 2);

  Bytes c0(file.begin(), file.begin() + static_cast<long>(chunk));
  Bytes c1(file.begin() + static_cast<long>(chunk),
           file.begin() + static_cast<long>(chunk * 2));
  EXPECT_TRUE(store->receiveChunk(7, 2, 0, c0, false));
  EXPECT_TRUE(store->receiveChunk(7, 2, chunk, c1, false));
  EXPECT_TRUE(store->receiveChunk(7, 2, chunk, c1, false));  // retransmit

  size_t off = chunk * 2;
  while (off < file.size()) {
    const size_t n = std::min(chunk, file.size() - off);
    Bytes part(file.begin() + static_cast<long>(off),
               file.begin() + static_cast<long>(off + n));
    const bool done = (off + n == file.size());
    EXPECT_TRUE(store->receiveChunk(7, 2, off, part, done));
    off += n;
  }

  SnapshotData out;
  ASSERT_TRUE(store->load(out));
  EXPECT_EQ(out.lastIncludedIndex, static_cast<Index>(7));
}

// B-group: a real file-backed cluster (FileLogStore + FileSnapshotStore) must
// keep committing after compaction, exactly like the production node binary.
namespace {

struct DiskNode {
  std::unique_ptr<FileLogStore> log;
  std::unique_ptr<KvStateMachine> sm;
  std::unique_ptr<FileSnapshotStore> snapshots;
  std::unique_ptr<RaftNode> node;
};

}  // namespace

TEST(RaftSnapshotDisk, FileStoreClusterKeepsCommittingAfterCompaction) {
  const std::string root = tempDir();
  auto clock = std::make_shared<FakeClock>();
  auto transport = std::make_shared<MemoryTransport>();
  std::vector<DiskNode> nodes(3);
  for (int id = 1; id <= 3; ++id) {
    DiskNode& n = nodes[static_cast<size_t>(id - 1)];
    const std::string dir = root + "/n" + std::to_string(id);
    n.log = std::make_unique<FileLogStore>(dir);
    n.sm = std::make_unique<KvStateMachine>();
    n.snapshots = std::make_unique<FileSnapshotStore>(dir);
    RaftConfig cfg;
    cfg.selfId = id;
    cfg.snapshotThresholdEntries = 8;
    for (int p = 1; p <= 3; ++p) {
      if (p != id) cfg.peerIds.push_back(p);
    }
    n.node = std::make_unique<RaftNode>(cfg, *n.log, *n.sm, *transport, *clock,
                                        n.snapshots.get());
    transport->addNode(id, n.node.get());
  }

  for (int i = 0; i < 60; ++i) {
    clock->advance(10);
    for (auto& n : nodes) n.node->tick();
  }
  RaftNode* leader = nullptr;
  for (auto& n : nodes) {
    if (n.node->role() == Role::kLeader) leader = n.node.get();
  }
  ASSERT_NE(leader, nullptr);

  for (int i = 1; i <= 40; ++i) {
    // Tick between proposals like the production node's ticker does: snapshot
    // transfers are driven by ticks, not by propose().
    for (int t = 0; t < 10; ++t) {
      clock->advance(10);
      for (auto& n : nodes) n.node->tick();
    }
    const auto reply = leader->propose(
        putReq(static_cast<uint64_t>(i), "k" + std::to_string(i), "v"), 2000);
    if (reply.status != ClientStatus::kOk) {
      for (size_t k = 0; k < nodes.size(); ++k) {
        std::cerr << "node" << (k + 1)
                  << " role=" << static_cast<int>(nodes[k].node->role())
                  << " term=" << nodes[k].node->currentTerm()
                  << " commit=" << nodes[k].node->commitIndex()
                  << " applied=" << nodes[k].node->lastApplied()
                  << " snap=" << nodes[k].node->lastIncludedIndex()
                  << " logLast=" << nodes[k].log->lastIndex()
                  << " first=" << nodes[k].log->firstIndex() << "\n";
      }
      FAIL() << "write " << i << " did not commit";
      break;
    }
  }
  EXPECT_GT(leader->lastIncludedIndex(), kNoIndex);
  std::filesystem::remove_all(root);
}

TEST(RaftSnapshotDisk, ConcurrentSaveAndInstallKeepNewestSnapshot) {  // B5/B14: the two writers must be serialised inside the store; the newest
  // (installed) boundary always wins.
  const std::string dir = tempDir();
  FileSnapshotStore store(dir);

  std::thread saves([&] {
    for (int i = 1; i <= 40; ++i) {
      SnapshotData d;
      d.lastIncludedIndex = static_cast<Index>(i);
      d.lastIncludedTerm = 1;
      d.payload = Bytes{'s'};
      store.save(d);  // always older than the installs below -> rejected
    }
  });
  std::thread installs([&] {
    for (int i = 100; i < 140; ++i) {
      SnapshotData d;
      d.lastIncludedIndex = static_cast<Index>(i);
      d.lastIncludedTerm = 2;
      d.payload = Bytes{'i'};
      store.receiveChunk(d.lastIncludedIndex, d.lastIncludedTerm, 0,
                         encodeSnapshotFile(d), /*done=*/true);
    }
  });
  saves.join();
  installs.join();

  SnapshotData out;
  ASSERT_TRUE(store.load(out));
  EXPECT_EQ(out.lastIncludedIndex, static_cast<Index>(139));
  std::filesystem::remove_all(dir);
}
