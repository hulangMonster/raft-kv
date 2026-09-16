// A-group: deterministic election tests (FakeClock + MemoryTransport).
#include <gtest/gtest.h>

#include <memory>

#include "raft_test_harness.h"

using namespace raftkv::raft;
using raftkv::OpCode;

TEST(RaftElection, SingleNodeBecomesLeader) {
  auto c = test::makeCluster(1);
  c->clock->advance(1000);  // well past the election timeout
  c->nodes[0].node->tick();
  EXPECT_EQ(c->nodes[0].node->role(), Role::kLeader);
}

TEST(RaftElection, OnlyOneLeaderPerTerm) {
  auto c = test::makeCluster(3);
  test::driveTicks(*c, 200, 10);

  int leaders = 0;
  for (const auto& n : c->nodes) {
    if (n.node->role() == Role::kLeader) ++leaders;
  }
  ASSERT_GE(leaders, 1) << "a leader must be elected";

  // No two leaders may hold the same term.
  for (const auto& a : c->nodes) {
    for (const auto& b : c->nodes) {
      if (&a == &b) continue;
      if (a.node->role() == Role::kLeader && b.node->role() == Role::kLeader) {
        EXPECT_NE(a.node->currentTerm(), b.node->currentTerm());
      }
    }
  }
}

TEST(RaftElection, SplitVoteResolvedByNextTimeout) {
  auto c = test::makeCluster(3);
  test::driveTicks(*c, 100, 40);

  int leaders = 0;
  for (const auto& n : c->nodes) {
    if (n.node->role() == Role::kLeader) ++leaders;
  }
  EXPECT_EQ(leaders, 1);
}

TEST(RaftElection, VoteGrantedToUpToDateCandidate) {
  // M4 J4：只给当前配置里的投票成员投票 -> 候选人必须是配置内成员（这里 2 号）。
  auto c = test::makeCluster(2);
  RequestVoteArgs args;
  args.term = 1;
  args.candidateId = 2;
  args.lastLogIndex = kNoIndex;
  args.lastLogTerm = kNoTerm;
  const auto reply = c->nodes[0].node->onRequestVote(args);
  EXPECT_EQ(reply.term, 1);
  EXPECT_TRUE(reply.voteGranted);
}

TEST(RaftElection, TermAndVotePersistedBeforeReply) {
  auto clock = std::make_shared<FakeClock>();
  auto transport = std::make_shared<MemoryTransport>();
  auto spy = std::make_unique<test::SpyMemoryLogStore>();
  auto* spyRaw = spy.get();
  raftkv::KvStateMachine sm;
  RaftConfig cfg;
  cfg.selfId = 1;
  cfg.peerIds = {2};  // M4 J4：候选人 2 必须是配置内成员才会被投票
  RaftNode node(cfg, *spy, sm, *transport, *clock);
  transport->addNode(1, &node);

  RequestVoteArgs args;
  args.term = 1;
  args.candidateId = 2;
  args.lastLogIndex = kNoIndex;
  args.lastLogTerm = kNoTerm;
  const auto reply = node.onRequestVote(args);

  ASSERT_TRUE(reply.voteGranted) << "up-to-date candidate must be granted";
  EXPECT_GE(spyRaw->persistMetaCallCount(), 1)
      << "term/votedFor must be persisted before the vote is granted";
}
