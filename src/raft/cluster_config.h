#pragma once

#include <string>
#include <vector>

#include "types.h"

namespace raftkv::raft {

// M4 (m4-design.md v1.1 §4.1): 集群配置与成员。
// 配置是日志的一部分（D1）：version == 产生它的配置条目 index；seed 配置为 0。
struct Member {
  int id = 0;
  std::string addr;   // "host:port"：用于 transport 地址簿与客户端路由
  bool voting = true; // false = CatchUp 中的非投票成员（J4：不计多数派）

  bool operator==(const Member& o) const {
    return id == o.id && addr == o.addr && voting == o.voting;
  }
};

struct ClusterConfig {
  uint64_t version = 0;
  std::vector<Member> members;  // 含 self；按 id 升序

  const Member* find(int id) const;
  bool isVoting(int id) const;
  bool contains(int id) const;
  size_t votingCount() const;
  size_t majority() const;            // votingCount()/2 + 1；votingCount()==0 时为 0
  std::vector<int> votingIds() const;
  bool operator==(const ClusterConfig& o) const;
};

// 配置 payload 编解码（配置条目 value 与快照 config 段共用同一格式）：
//   [cfgVer:1]=1 | [version:8] | [count:4] | count x { [id:4] [voting:1] [addrLen:2] [addr] }
Bytes encodeClusterConfig(const ClusterConfig& c);
bool decodeClusterConfig(const Byte* p, size_t n, ClusterConfig& out);

// M4: 成员变更操作
enum class MembershipOp : uint8_t { kAdd = 1, kRemove = 2 };

}  // namespace raftkv::raft
