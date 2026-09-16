// M4 cluster config：数据结构 + 编解码（m4-design.md v1.2 §4.1/§4.2）。
//
// 配置 payload（配置条目 value 与快照 config 段共用同一格式，BE）：
//   [cfgVer:1]=1 | [version:8] | [count:4] | count x { [id:4] [voting:1] [addrLen:2] [addr] }
#include "raft/cluster_config.h"

#include <algorithm>
#include <cstdint>

namespace raftkv::raft {

namespace {

constexpr uint8_t kCfgVer = 1;
constexpr size_t kFixedLen = 1 + 8 + 4;  // cfgVer + version + count
constexpr size_t kMemberMinLen = 4 + 1 + 2;

void putU64(Bytes& out, uint64_t v) {
  for (int i = 7; i >= 0; --i) {
    out.push_back(static_cast<Byte>((v >> (i * 8)) & 0xff));
  }
}
uint64_t getU64(const Byte* p) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) v = (v << 8) | p[i];
  return v;
}
void putU16(Bytes& out, uint16_t v) {
  out.push_back(static_cast<Byte>((v >> 8) & 0xff));
  out.push_back(static_cast<Byte>(v & 0xff));
}
uint16_t getU16(const Byte* p) {
  return static_cast<uint16_t>((static_cast<uint16_t>(p[0]) << 8) | p[1]);
}

}  // namespace

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
  return (v == 0) ? 0 : (v / 2 + 1);  // 空配置没有多数派（m4-prerequisites §6.4）
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

Bytes encodeClusterConfig(const ClusterConfig& c) {
  Bytes out;
  out.reserve(kFixedLen + c.members.size() * 16);
  out.push_back(kCfgVer);
  putU64(out, c.version);
  putU32(out, static_cast<uint32_t>(c.members.size()));
  for (const Member& m : c.members) {
    putU32(out, static_cast<uint32_t>(m.id));
    out.push_back(m.voting ? 1 : 0);
    putU16(out, static_cast<uint16_t>(m.addr.size()));
    out.insert(out.end(), m.addr.begin(), m.addr.end());
  }
  return out;
}

bool decodeClusterConfig(const Byte* p, size_t n, ClusterConfig& out) {
  if (p == nullptr || n < kFixedLen) return false;
  if (p[0] != kCfgVer) return false;
  size_t off = 1;
  const uint64_t version = getU64(p + off);
  off += 8;
  const uint32_t count = getU32(p + off);
  off += 4;
  if (count > (n - off) / kMemberMinLen) return false;  // 上界校验，防超大分配

  std::vector<Member> ms;
  ms.reserve(count);
  for (uint32_t i = 0; i < count; ++i) {
    if (off + kMemberMinLen > n) return false;
    Member m;
    m.id = static_cast<int>(getU32(p + off));
    m.voting = (p[off + 4] != 0);
    const uint16_t alen = getU16(p + off + 5);
    off += kMemberMinLen;
    if (off + alen > n) return false;
    m.addr.assign(reinterpret_cast<const char*>(p + off), alen);
    off += alen;
    ms.push_back(std::move(m));
  }
  if (off != n) return false;  // 尾部垃圾

  out.version = version;
  out.members = std::move(ms);
  return true;
}

}  // namespace raftkv::raft
