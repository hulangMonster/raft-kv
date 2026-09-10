// A-group: log replication + conflict truncation + lagging follower.
#include <gtest/gtest.h>

#include <string>

#include "raft_test_harness.h"

using namespace raftkv::raft;
using raftkv::OpCode;

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

}  // namespace

TEST(RaftLog, ReplicationAllLogsMatch) {
  auto c = test::makeCluster(3);
  test::driveTicks(*c, 200, 10);
  RaftNode* leader = test::findLeader(*c);
  ASSERT_NE(leader, nullptr);

  for (int i = 1; i <= 20; ++i) {
    const auto reply =
        leader->propose(putReq(i, "k" + std::to_string(i), "v" + std::to_string(i)), 1000);
    EXPECT_EQ(reply.status, ClientStatus::kOk);
  }

  const auto& base = c->nodes[0].log->all();
  for (const auto& n : c->nodes) {
    const auto& l = n.log->all();
    ASSERT_EQ(l.size(), base.size());
    for (size_t i = 0; i < base.size(); ++i) {
      EXPECT_EQ(l[i].index, base[i].index);
      EXPECT_EQ(l[i].term, base[i].term);
    }
  }
}

TEST(RaftLog, ConflictingSuffixTruncatedOnLeaderChange) {
  // A follower with a conflicting suffix must truncate it once the new leader
  // sends AppendEntries. Full deterministic construction lands in M2.5
  // (needs leadership change + partition); contract asserted below.
  auto c = test::makeCluster(3);
  test::driveTicks(*c, 200, 10);
  RaftNode* leader = test::findLeader(*c);
  ASSERT_NE(leader, nullptr);

  // After a normal propose round, every node's log must be a prefix-consistent
  // copy of the leader's log (no divergent suffix survives).
  ASSERT_EQ(leader->propose(putReq(1, "a", "1"), 1000).status, ClientStatus::kOk);
  const auto& leaderLog = c->nodes[0].log->all();  // placeholder; refined M2.5
  for (const auto& n : c->nodes) {
    const auto& l = n.log->all();
    ASSERT_LE(l.size(), leaderLog.size());
    for (size_t i = 0; i < l.size(); ++i) {
      EXPECT_EQ(l[i].term, leaderLog[i].term) << "divergent suffix must be truncated";
    }
  }
}

TEST(RaftLog, LaggingFollowerCatchesUp) {
  auto c = test::makeCluster(3);
  test::driveTicks(*c, 200, 10);
  RaftNode* leader = test::findLeader(*c);
  ASSERT_NE(leader, nullptr);

  // Isolate one follower while the leader keeps writing, then heal it.
  // (follower id is whichever of the other two; isolate node id 1 for now,
  // refined when findLeader returns the actual leader id in M2.3)
  c->transport->isolate(1);
  for (int i = 1; i <= 10; ++i) {
    EXPECT_EQ(leader->propose(putReq(i, "k" + std::to_string(i), "v"), 1000).status,
              ClientStatus::kOk);
  }
  c->transport->heal(1);
  test::driveTicks(*c, 100, 10);

  const auto& base = c->nodes[0].log->all();
  for (const auto& n : c->nodes) {
    ASSERT_EQ(n.log->all().size(), base.size()) << "lagging follower must catch up";
  }
}
