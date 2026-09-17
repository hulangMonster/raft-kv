#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "common.h"  // raftkv::OpCode

namespace raftkv::raft {

using Term = uint64_t;
using Index = uint64_t;

constexpr Index kNoIndex = 0;  // log indices are 1-based; 0 means "none"
constexpr Term kNoTerm = 0;

enum class Role : uint8_t { kFollower = 0, kCandidate = 1, kLeader = 2 };

struct LogEntry {
  Index index = kNoIndex;                 // assigned by the Leader
  Term term = kNoTerm;
  raftkv::OpCode op = raftkv::OpCode::kGet;
  std::string key;
  std::string value;                      // meaningful for PUT only
  uint64_t clientId = 0;                  // idempotency: client session id
  uint64_t requestId = 0;                 // idempotency: per-session sequence
};

struct RequestVoteArgs {
  Term term = kNoTerm;
  int candidateId = 0;
  Index lastLogIndex = kNoIndex;
  Term lastLogTerm = kNoTerm;
};
struct RequestVoteReply {
  Term term = kNoTerm;
  bool voteGranted = false;
};

struct AppendEntriesArgs {
  Term term = kNoTerm;
  int leaderId = 0;
  Index prevLogIndex = kNoIndex;
  Term prevLogTerm = kNoTerm;
  std::vector<LogEntry> entries;          // empty for heartbeats
  Index leaderCommit = kNoIndex;
};
struct AppendEntriesReply {
  Term term = kNoTerm;
  bool success = false;
  Index conflictIndex = kNoIndex;         // fast backup (M2.5)
  Term conflictTerm = kNoTerm;
};

// ---- M3: InstallSnapshot (msgType 5/6) ----
struct InstallSnapshotArgs {
  Term term = kNoTerm;
  int leaderId = 0;
  Index lastIncludedIndex = kNoIndex;
  Term lastIncludedTerm = kNoTerm;
  uint64_t offset = 0;   // this chunk's offset in the snapshot file
  bool done = false;
  Bytes data;            // raw snapshot bytes [offset, offset+len)
};
struct InstallSnapshotReply {
  Term term = kNoTerm;
  bool success = false;
  uint64_t nextOffset = 0;  // reserved for M5 resume; always 0 in M3
};

struct ClientRequest {
  raftkv::OpCode op = raftkv::OpCode::kGet;
  std::string key;
  std::string value;
  uint64_t clientId = 0;
  uint64_t requestId = 0;
};
enum class ClientStatus : uint8_t {
  kOk = 0,
  kNotFound = 1,
  kErr = 2,
  kNotLeader = 3,
};
struct ClientReply {
  ClientStatus status = ClientStatus::kErr;
  std::string value;
  int leaderHint = -1;                    // node id when status == kNotLeader
};

struct RaftConfig {
  int selfId = 1;
  std::vector<int> peerIds;               // does NOT include self
  uint64_t electionTimeoutMinMs = 150;
  uint64_t electionTimeoutMaxMs = 300;
  uint64_t heartbeatMs = 50;
  uint64_t rpcTimeoutMs = 100;
  // M5.6（设计 §8.2 滑动窗口）：异步引擎下每 peer 允许**同时在途**的 AppendEntries 批数。
  // 1 = M5.3 的保守行为（单批在途）；>1 时 leader 不必等上一批的 ack 才发下一批，
  // 复制往返得以流水线化（实测这是本机 p=64 的真正瓶颈：每批要串行等两个 follower 的
  // fsync+RTT，约 51ms/批，而 fsync 只占 14%）。
  // 安全前提：① 应答上下文由**闭包**携带（M5.3 已落地），乱序/重复 ack 幂等；
  //          ② `matchIndex_` 只取 max（单调）；③ 每个槽位有 TTL 兜底（见 inflightMuteGapMs），
  //          因此 reactor 丢帧（不回调）也不会永久占位；④ TTL < 最小选举超时（I15）。
  size_t maxInflightPerPeer = 1;
  // M5.4（决策④「批处理调优」）：组提交"蓄批"窗口。>0 时，被选为 flusher 的
  // propose 会先短睡这么久再 fsync，让并发写汇入同一批 —— 每批的**往返成本**
  // （leader fsync + follower fsync + RTT）被更多条目摊薄，吞吐近似线性提升。
  // 仅当同时在等的写者数 ≥ lingerMinWaiters 时才蓄批，pipeline=1/2 不受影响。
  uint64_t groupCommitLingerUs = 0;  // 0 = 关闭（默认；由 --group-linger-us 打开）
  size_t lingerMinWaiters = 4;       // 并发放等者达到此数才蓄批
  size_t maxEntriesPerAppend = 128;
  size_t maxBytesPerAppend = 1u << 20;
  bool appendNoop = true;  // leader appends+commits a no-op entry on election
  // M3 snapshot knobs
  size_t snapshotThresholdEntries = 10000;  // trigger compaction threshold
  size_t snapshotChunkBytes = 1u << 20;     // InstallSnapshot chunk size
  // M4 membership knobs (m4-design.md v1.1)
  uint64_t catchUpTimeoutMs = 30000;  // add: 追平预算
  uint64_t readIndexTimeoutMs = 500;  // linearizableGet: quorum 探针预算
};

// ---- M4: 线性一致读探针（msgType 9/14）----
struct ReadProbeArgs {
  Term term = kNoTerm;
  int leaderId = 0;
  uint64_t seq = 0;
};
struct ReadProbeReply {
  Term term = kNoTerm;
  bool ok = false;
  uint64_t seq = 0;
};

}  // namespace raftkv::raft
