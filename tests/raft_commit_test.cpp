// A-group: commit rule (§5.4.2) tests.
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

TEST(RaftCommit, CommitRequiresMajority) {
  auto c = test::makeCluster(3);
  test::driveTicks(*c, 200, 10);
  RaftNode* leader = test::findLeader(*c);
  ASSERT_NE(leader, nullptr);
  const int leaderId = leader->leaderId();
  ASSERT_GT(leaderId, 0);

  // Isolate BOTH followers: leader alone (1 of 3) is not a majority.
  for (int id = 1; id <= 3; ++id) {
    if (id != leaderId) c->transport->isolate(id);
  }
  const auto reply = leader->propose(putReq(1, "k", "v"), 1000);
  EXPECT_NE(reply.status, ClientStatus::kOk);

  // Heal and retry: now a majority can be reached and the entry commits.
  for (int id = 1; id <= 3; ++id) {
    if (id != leaderId) c->transport->heal(id);
  }
  test::driveTicks(*c, 100, 10);
  EXPECT_EQ(leader->propose(putReq(2, "k2", "v2"), 1000).status,
            ClientStatus::kOk);
  EXPECT_GT(leader->commitIndex(), kNoIndex);
}

TEST(RaftCommit, OldTermEntryNotCommittedByCount) {
  // §5.4.2: an entry from a previous term must NOT be committed merely
  // because it is replicated to a majority. Deterministic 5-node construction:
  //   1. old leader appends E1(term1) to only itself + one follower -> uncommitted
  //   2. old leader steps down; the follower with E1 becomes leader in term2
  //   3. heartbeats copy E1 to a majority (5/5), but E1 must stay uncommitted
  //   4. a new current-term entry E2(term2) commits and drags E1 with it
  auto c = test::makeCluster(5, /*appendNoop=*/false);

  // 1. Natural leader election (deterministic: node 5 wins).
  test::driveTicks(*c, 50, 10);
  RaftNode* leader = test::findLeader(*c);
  ASSERT_NE(leader, nullptr);
  const int leaderId = leader->leaderId();

  // 2. Isolate everyone except one follower, then append an uncommittable entry.
  int reachable = 0;
  for (int id = 1; id <= 5; ++id) {
    if (id != leaderId) { reachable = id; break; }
  }
  for (int id = 1; id <= 5; ++id) {
    if (id != leaderId && id != reachable) c->transport->isolate(id);
  }
  const auto r1 = leader->propose(putReq(1, "old", "v1"), 50);
  EXPECT_NE(r1.status, ClientStatus::kOk);  // leader + 1 follower < majority(3)

  // 3. Step the old leader down and make it vote for the entry's carrier.
  RequestVoteArgs stepDown;
  stepDown.term = leader->currentTerm() + 1;
  stepDown.candidateId = reachable;
  stepDown.lastLogIndex = 1;  // the carrier has E1 at index 1
  stepDown.lastLogTerm = 1;
  leader->onRequestVote(stepDown);

  // 4. Heal everyone so the next leader can reach a majority.
  for (int id = 1; id <= 5; ++id) c->transport->heal(id);

  // 5. Force the carrier to become leader in the new term.
  RaftNode* newLeader = c->nodes[reachable - 1].node.get();
  c->clock->advance(5000);
  newLeader->tick();
  ASSERT_EQ(newLeader->role(), Role::kLeader);

  // 6. Heartbeats replicate E1 to a majority, but §5.4.2 forbids committing it.
  test::driveTicks(*c, 200, 10);
  EXPECT_EQ(newLeader->commitIndex(), kNoIndex);
  EXPECT_EQ(newLeader->lastApplied(), kNoIndex);

  // 7. A current-term entry commits and drags the old entry with it.
  const auto r2 = newLeader->propose(putReq(2, "new", "v2"), 1000);
  EXPECT_EQ(r2.status, ClientStatus::kOk);
  EXPECT_EQ(newLeader->commitIndex(), 2);
  EXPECT_EQ(newLeader->lastApplied(), 2);

  // 8. Both entries are visible, applied in log order.
  ClientRequest g;
  g.op = OpCode::kGet;
  g.key = "old";
  g.clientId = 1;
  g.requestId = 100;
  const auto gr = newLeader->propose(g, 100);
  EXPECT_EQ(gr.status, ClientStatus::kOk);
  EXPECT_EQ(gr.value, "v1");
}
