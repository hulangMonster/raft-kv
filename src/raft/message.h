#pragma once

#include "cluster_config.h"
#include "common.h"
#include "types.h"

namespace raftkv::raft {

// Frame: [len:4][type:1][payload...]  where len = 1 + payload.size().
// Types 1-4 are node-to-node RPC; 10/11 are the raft cluster client protocol.
enum class MsgType : uint8_t {
  kRequestVote = 1,
  kRequestVoteReply = 2,
  kAppendEntries = 3,
  kAppendEntriesReply = 4,
  kInstallSnapshot = 5,
  kInstallSnapshotReply = 6,
  // M4 (m4-design.md v1.1 §4.5)
  kConfigRequest = 7,       // 客户端 -> 节点：get / add / remove
  kConfigReply = 8,         // 节点 -> 客户端
  kReadProbe = 9,           // Leader -> 节点：ReadIndex quorum 探针
  kClientRequest = 10,
  kClientReply = 11,
  kStatusRequest = 12,
  kSnapshotTrigger = 13,
  kReadProbeReply = 14,     // 节点 -> Leader：探针回包
  // M5.1（只增不改）：指标文本端点（可选），应答复用 kClientReply
  kMetricsRequest = 15,
};

// ---- M4: 配置查询/变更消息体（m4-design.md v1.1 §4.5）----
struct ConfigRequestArgs {
  uint8_t action = 0;  // 0=get, 1=add, 2=remove
  int targetId = 0;
  std::string addr;
};
struct ConfigReplyArgs {
  Term term = kNoTerm;
  bool ok = false;
  int leaderHint = -1;
  ClusterConfig config;  // action==0 时返回当前配置；写操作时 version/members 可留空
};

Bytes encodeFrame(MsgType type, const Bytes& payload);
bool decodeFrame(const Byte* data, size_t n, MsgType& type, Bytes& payload);

Bytes encodeRequestVote(const RequestVoteArgs& args);
bool decodeRequestVote(const Byte* data, size_t n, RequestVoteArgs& out);

Bytes encodeRequestVoteReply(const RequestVoteReply& reply);
bool decodeRequestVoteReply(const Byte* data, size_t n, RequestVoteReply& out);

Bytes encodeAppendEntries(const AppendEntriesArgs& args);
bool decodeAppendEntries(const Byte* data, size_t n, AppendEntriesArgs& out);

Bytes encodeAppendEntriesReply(const AppendEntriesReply& reply);
bool decodeAppendEntriesReply(const Byte* data, size_t n, AppendEntriesReply& out);

Bytes encodeInstallSnapshot(const InstallSnapshotArgs& args);
bool decodeInstallSnapshot(const Byte* data, size_t n, InstallSnapshotArgs& out);

Bytes encodeInstallSnapshotReply(const InstallSnapshotReply& reply);
bool decodeInstallSnapshotReply(const Byte* data, size_t n,
                                InstallSnapshotReply& out);

Bytes encodeClientRequest(const ClientRequest& req);
bool decodeClientRequest(const Byte* data, size_t n, ClientRequest& out);

Bytes encodeClientReply(const ClientReply& reply);
bool decodeClientReply(const Byte* data, size_t n, ClientReply& out);

// M4: 7/8 配置消息 + 9/14 读探针（复用 M2 长度前缀帧）
Bytes encodeConfigRequest(const ConfigRequestArgs& args);
bool decodeConfigRequest(const Byte* data, size_t n, ConfigRequestArgs& out);
Bytes encodeConfigReply(const ConfigReplyArgs& reply);
bool decodeConfigReply(const Byte* data, size_t n, ConfigReplyArgs& out);
Bytes encodeReadProbe(const ReadProbeArgs& args);
bool decodeReadProbe(const Byte* data, size_t n, ReadProbeArgs& out);
Bytes encodeReadProbeReply(const ReadProbeReply& reply);
bool decodeReadProbeReply(const Byte* data, size_t n, ReadProbeReply& out);

}  // namespace raftkv::raft
