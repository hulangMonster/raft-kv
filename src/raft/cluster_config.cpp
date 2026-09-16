// M4 cluster config: 数据结构与（M4.1 待实现的）编解码。
// 本文件在 #2（TDD 脚手架）阶段只提供：真实的数据访问器 + 未实现的 serde 桩；
// 真正的编解码在 M4.1 落地（见 docs/m4-prerequisites.md §5.1）。
#include "raft/cluster_config.h"

#include <algorithm>

namespace raftkv::raft {

const Member* ClusterConfig::find(int id) const {
  for (const Member& m : members) {
    if (m.id == id) return &m;
  }
  return nullptr;
}

bool ClusterConfig::isVoting(int id) const {
  const Member* m = find(id);
  return m != nullptr && m->voting;
}

bool ClusterConfig::contains(int id) const { return find(id) != nullptr; }

size_t ClusterConfig::votingCount() const {
  size_t n = 0;
  for (const Member& m : members) {
    if (m.voting) ++n;
  }
  return n;
}

size_t ClusterConfig::majority() const {
  const size_t v = votingCount();
  return (v == 0) ? 0 : (v / 2 + 1);  // 空配置没有多数派（未定义行为清单 §6.4）
}

std::vector<int> ClusterConfig::votingIds() const {
  std::vector<int> out;
  for (const Member& m : members) {
    if (m.voting) out.push_back(m.id);
  }
  std::sort(out.begin(), out.end());
  return out;
}

bool ClusterConfig::operator==(const ClusterConfig& o) const {
  return version == o.version && members == o.members;
}

// ---- 以下为 M4.1 待实现（#2 阶段故意返回空/false，用于跑出 RED）----
Bytes encodeClusterConfig(const ClusterConfig& c) {
  (void)c;
  return Bytes{};
}

bool decodeClusterConfig(const Byte* p, size_t n, ClusterConfig& out) {
  (void)p;
  (void)n;
  (void)out;
  return false;
}

}  // namespace raftkv::raft
