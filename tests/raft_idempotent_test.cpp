// A-group: propose step-down + KV idempotency.
#include <gtest/gtest.h>

#include <string>

#include "kv/kv_state_machine.h"
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

}  // namespace

TEST(ProposeStepDown, ReturnsNotLeaderAfterStepDown) {
  auto c = test::makeCluster(3);
  test::driveTicks(*c, 200, 10);
  RaftNode* leader = test::findLeader(*c);
  ASSERT_NE(leader, nullptr);

  // A higher-term RPC forces the leader to step down.
  RequestVoteArgs higher;
  higher.term = leader->currentTerm() + 5;
  // M5.2：非成员候选者不再被采纳任期（J4 纵深防御），改用配置内成员触发降级
  higher.candidateId = 2;
  higher.lastLogIndex = kNoIndex;
  higher.lastLogTerm = kNoTerm;
  leader->onRequestVote(higher);

  const auto reply = leader->propose(putReq(1, "k", "v"), 100);
  EXPECT_EQ(reply.status, ClientStatus::kNotLeader);
}

TEST(KvIdempotent, ApplyIsIdempotentOnRetry) {
  KvStateMachine sm;
  raft::LogEntry e;
  e.index = 1;
  e.term = 1;
  e.op = OpCode::kPut;
  e.key = "k";
  e.value = "v";
  e.clientId = 7;
  e.requestId = 42;

  sm.apply(e);
  std::string out;
  EXPECT_TRUE(sm.get("k", out));
  EXPECT_EQ(out, "v");

  // Retry with the same (clientId, requestId): must be deduplicated.
  e.value = "MUST-NOT-APPLY";
  sm.apply(e);
  EXPECT_TRUE(sm.get("k", out));
  EXPECT_EQ(out, "v");
  EXPECT_EQ(sm.lastApplied(), 1);
}
