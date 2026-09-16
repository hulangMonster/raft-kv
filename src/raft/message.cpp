#include "raft/message.h"

#include <cstring>

namespace raftkv::raft {

namespace {

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

Bytes encodeLogEntry(const LogEntry& e) {
  Bytes p;
  p.reserve(41 + e.key.size() + e.value.size());
  putU64(p, e.index);
  putU64(p, e.term);
  p.push_back(static_cast<Byte>(e.op));
  putU32(p, static_cast<uint32_t>(e.key.size()));
  putU32(p, static_cast<uint32_t>(e.value.size()));
  putU64(p, e.clientId);
  putU64(p, e.requestId);
  p.insert(p.end(), e.key.begin(), e.key.end());
  p.insert(p.end(), e.value.begin(), e.value.end());
  return p;
}

bool decodeLogEntry(const Byte* p, size_t n, LogEntry& e) {
  if (n < 41) return false;
  const uint8_t op = p[16];
  if (op != static_cast<uint8_t>(OpCode::kPut) &&
      op != static_cast<uint8_t>(OpCode::kGet) &&
      op != static_cast<uint8_t>(OpCode::kDel) &&
      op != static_cast<uint8_t>(OpCode::kConfig)) {  // M4: 配置条目（m4-prerequisites §5.1-11）
    return false;
  }
  const size_t keyLen = getU32(p + 17);
  const size_t valLen = getU32(p + 21);
  if (n != 41 + keyLen + valLen) return false;

  e.index = getU64(p);
  e.term = getU64(p + 8);
  e.op = static_cast<OpCode>(op);
  e.clientId = getU64(p + 25);
  e.requestId = getU64(p + 33);
  e.key.assign(reinterpret_cast<const char*>(p + 41), keyLen);
  e.value.assign(reinterpret_cast<const char*>(p + 41 + keyLen), valLen);
  return true;
}

}  // namespace

Bytes encodeFrame(MsgType type, const Bytes& payload) {
  Bytes out;
  out.reserve(5 + payload.size());
  putU32(out, static_cast<uint32_t>(1 + payload.size()));
  out.push_back(static_cast<Byte>(type));
  out.insert(out.end(), payload.begin(), payload.end());
  return out;
}

bool decodeFrame(const Byte* data, size_t n, MsgType& type, Bytes& payload) {
  if (n < 5) return false;
  const uint32_t len = getU32(data);
  if (len < 1 || 4u + len != n) return false;
  type = static_cast<MsgType>(data[4]);
  payload.assign(data + 5, data + n);
  return true;
}

Bytes encodeRequestVote(const RequestVoteArgs& a) {
  Bytes p;
  p.reserve(28);
  putU64(p, a.term);
  putU32(p, static_cast<uint32_t>(a.candidateId));
  putU64(p, a.lastLogIndex);
  putU64(p, a.lastLogTerm);
  return p;
}
bool decodeRequestVote(const Byte* d, size_t n, RequestVoteArgs& out) {
  if (n != 28) return false;
  out.term = getU64(d);
  out.candidateId = static_cast<int>(getU32(d + 8));
  out.lastLogIndex = getU64(d + 12);
  out.lastLogTerm = getU64(d + 20);
  return true;
}

Bytes encodeRequestVoteReply(const RequestVoteReply& r) {
  Bytes p;
  p.reserve(9);
  putU64(p, r.term);
  p.push_back(r.voteGranted ? 1 : 0);
  return p;
}
bool decodeRequestVoteReply(const Byte* d, size_t n, RequestVoteReply& out) {
  if (n != 9) return false;
  out.term = getU64(d);
  out.voteGranted = (d[8] != 0);
  return true;
}

Bytes encodeAppendEntries(const AppendEntriesArgs& a) {
  Bytes p;
  p.reserve(36);
  putU64(p, a.term);
  putU32(p, static_cast<uint32_t>(a.leaderId));
  putU64(p, a.prevLogIndex);
  putU64(p, a.prevLogTerm);
  putU64(p, a.leaderCommit);
  putU32(p, static_cast<uint32_t>(a.entries.size()));
  for (const LogEntry& e : a.entries) {
    const Bytes eb = encodeLogEntry(e);
    p.insert(p.end(), eb.begin(), eb.end());
  }
  return p;
}
bool decodeAppendEntries(const Byte* d, size_t n, AppendEntriesArgs& out) {
  if (n < 40) return false;
  out.term = getU64(d);
  out.leaderId = static_cast<int>(getU32(d + 8));
  out.prevLogIndex = getU64(d + 12);
  out.prevLogTerm = getU64(d + 20);
  out.leaderCommit = getU64(d + 28);
  const uint32_t count = getU32(d + 36);

  size_t off = 40;
  out.entries.clear();
  out.entries.reserve(count);
  for (uint32_t i = 0; i < count; ++i) {
    if (off >= n) return false;
    LogEntry e;
    // Need the entry length; parse from its keyLen/valLen fields.
    if (n - off < 41) return false;
    const size_t keyLen = getU32(d + off + 17);
    const size_t valLen = getU32(d + off + 21);
    const size_t total = 41 + keyLen + valLen;
    if (off + total > n) return false;
    if (!decodeLogEntry(d + off, total, e)) return false;
    out.entries.push_back(std::move(e));
    off += total;
  }
  return off == n;
}

Bytes encodeAppendEntriesReply(const AppendEntriesReply& r) {
  Bytes p;
  p.reserve(25);
  putU64(p, r.term);
  p.push_back(r.success ? 1 : 0);
  putU64(p, r.conflictIndex);
  putU64(p, r.conflictTerm);
  return p;
}
bool decodeAppendEntriesReply(const Byte* d, size_t n, AppendEntriesReply& out) {
  if (n != 25) return false;
  out.term = getU64(d);
  out.success = (d[8] != 0);
  out.conflictIndex = getU64(d + 9);
  out.conflictTerm = getU64(d + 17);
  return true;
}

// InstallSnapshot: term(8) leaderId(4) index(8) term(8) offset(8) dataLen(4)
//                  done(1) data[dataLen]   -> 41 + dataLen
Bytes encodeInstallSnapshot(const InstallSnapshotArgs& a) {
  Bytes p;
  p.reserve(41 + a.data.size());
  putU64(p, a.term);
  putU32(p, static_cast<uint32_t>(a.leaderId));
  putU64(p, a.lastIncludedIndex);
  putU64(p, a.lastIncludedTerm);
  putU64(p, a.offset);
  putU32(p, static_cast<uint32_t>(a.data.size()));
  p.push_back(a.done ? 1 : 0);
  p.insert(p.end(), a.data.begin(), a.data.end());
  return p;
}
bool decodeInstallSnapshot(const Byte* d, size_t n, InstallSnapshotArgs& out) {
  if (n < 41) return false;
  out.term = getU64(d);
  out.leaderId = static_cast<int>(getU32(d + 8));
  out.lastIncludedIndex = getU64(d + 12);
  out.lastIncludedTerm = getU64(d + 20);
  out.offset = getU64(d + 28);
  const uint32_t dataLen = getU32(d + 36);
  out.done = (d[40] != 0);
  if (41 + dataLen != n) return false;
  out.data.assign(d + 41, d + n);
  return true;
}

// InstallSnapshotReply: term(8) success(1) nextOffset(8) -> 17
Bytes encodeInstallSnapshotReply(const InstallSnapshotReply& r) {
  Bytes p;
  p.reserve(17);
  putU64(p, r.term);
  p.push_back(r.success ? 1 : 0);
  putU64(p, r.nextOffset);
  return p;
}
bool decodeInstallSnapshotReply(const Byte* d, size_t n,
                                InstallSnapshotReply& out) {
  if (n != 17) return false;
  out.term = getU64(d);
  out.success = (d[8] != 0);
  out.nextOffset = getU64(d + 9);
  return true;
}

Bytes encodeClientRequest(const ClientRequest& r) {
  Bytes p;
  p.reserve(25 + r.key.size() + r.value.size());
  p.push_back(static_cast<Byte>(r.op));
  putU64(p, r.clientId);
  putU64(p, r.requestId);
  putU32(p, static_cast<uint32_t>(r.key.size()));
  putU32(p, static_cast<uint32_t>(r.value.size()));
  p.insert(p.end(), r.key.begin(), r.key.end());
  p.insert(p.end(), r.value.begin(), r.value.end());
  return p;
}
bool decodeClientRequest(const Byte* d, size_t n, ClientRequest& out) {
  // layout: op(1) clientId(8) requestId(8) keyLen(4) valLen(4) key value
  if (n < 25) return false;
  const uint8_t op = d[0];
  if (op != static_cast<uint8_t>(OpCode::kPut) &&
      op != static_cast<uint8_t>(OpCode::kGet) &&
      op != static_cast<uint8_t>(OpCode::kDel)) {
    return false;
  }
  const size_t keyLen = getU32(d + 17);
  const size_t valLen = getU32(d + 21);
  if (n != 25 + keyLen + valLen) return false;
  out.op = static_cast<OpCode>(op);
  out.clientId = getU64(d + 1);
  out.requestId = getU64(d + 9);
  out.key.assign(reinterpret_cast<const char*>(d + 25), keyLen);
  out.value.assign(reinterpret_cast<const char*>(d + 25 + keyLen), valLen);
  return true;
}

Bytes encodeClientReply(const ClientReply& r) {
  Bytes p;
  p.reserve(9 + r.value.size());
  p.push_back(static_cast<Byte>(r.status));
  putU32(p, static_cast<uint32_t>(r.leaderHint));
  putU32(p, static_cast<uint32_t>(r.value.size()));
  p.insert(p.end(), r.value.begin(), r.value.end());
  return p;
}
bool decodeClientReply(const Byte* d, size_t n, ClientReply& out) {
  if (n < 9) return false;
  const uint8_t s = d[0];
  if (s > static_cast<uint8_t>(ClientStatus::kNotLeader)) return false;
  const uint32_t valLen = getU32(d + 5);
  if (n != 9 + valLen) return false;
  out.status = static_cast<ClientStatus>(s);
  out.leaderHint = static_cast<int>(getU32(d + 1));
  out.value.assign(reinterpret_cast<const char*>(d + 9), valLen);
  return true;
}

// ---- M4 scaffolding: 7/8 配置消息与 9/14 读探针（实现见 M4.1/M4.4）----
// msgType 7: [action:1][targetId:4][addrLen:2][addr]
Bytes encodeConfigRequest(const ConfigRequestArgs& args) {
  Bytes p;
  p.reserve(7 + args.addr.size());
  p.push_back(args.action);
  putU32(p, static_cast<uint32_t>(args.targetId));
  const uint16_t alen = static_cast<uint16_t>(args.addr.size());
  p.push_back(static_cast<Byte>((alen >> 8) & 0xff));
  p.push_back(static_cast<Byte>(alen & 0xff));
  p.insert(p.end(), args.addr.begin(), args.addr.end());
  return p;
}
bool decodeConfigRequest(const Byte* data, size_t n, ConfigRequestArgs& out) {
  if (data == nullptr || n < 7) return false;
  const uint16_t alen =
      static_cast<uint16_t>((static_cast<uint16_t>(data[5]) << 8) | data[6]);
  if (n != static_cast<size_t>(7) + alen) return false;
  out.action = data[0];
  out.targetId = static_cast<int>(getU32(data + 1));
  out.addr.assign(reinterpret_cast<const char*>(data + 7), alen);
  return true;
}
// msgType 8: [term:8][ok:1][leaderHint:4][configVersion:8][count:4] members...
Bytes encodeConfigReply(const ConfigReplyArgs& reply) {
  Bytes p;
  p.reserve(25 + reply.config.members.size() * 16);
  putU64(p, reply.term);
  p.push_back(reply.ok ? 1 : 0);
  putU32(p, static_cast<uint32_t>(reply.leaderHint));
  putU64(p, reply.config.version);
  putU32(p, static_cast<uint32_t>(reply.config.members.size()));
  for (const Member& m : reply.config.members) {
    putU32(p, static_cast<uint32_t>(m.id));
    p.push_back(m.voting ? 1 : 0);
    const uint16_t alen = static_cast<uint16_t>(m.addr.size());
    p.push_back(static_cast<Byte>((alen >> 8) & 0xff));
    p.push_back(static_cast<Byte>(alen & 0xff));
    p.insert(p.end(), m.addr.begin(), m.addr.end());
  }
  return p;
}
bool decodeConfigReply(const Byte* data, size_t n, ConfigReplyArgs& out) {
  if (data == nullptr || n < 25) return false;
  out.term = getU64(data);
  out.ok = (data[8] != 0);
  out.leaderHint = static_cast<int>(getU32(data + 9));
  out.config.version = getU64(data + 13);
  const uint32_t count = getU32(data + 21);
  size_t off = 25;
  if (count > (n - off) / 7) return false;
  std::vector<Member> ms;
  ms.reserve(count);
  for (uint32_t i = 0; i < count; ++i) {
    if (off + 7 > n) return false;
    Member m;
    m.id = static_cast<int>(getU32(data + off));
    m.voting = (data[off + 4] != 0);
    const uint16_t alen = static_cast<uint16_t>(
        (static_cast<uint16_t>(data[off + 5]) << 8) | data[off + 6]);
    off += 7;
    if (off + alen > n) return false;
    m.addr.assign(reinterpret_cast<const char*>(data + off), alen);
    off += alen;
    ms.push_back(std::move(m));
  }
  if (off != n) return false;
  out.config.members = std::move(ms);
  return true;
}
Bytes encodeReadProbe(const ReadProbeArgs& args) {
  (void)args;
  return Bytes{};
}
bool decodeReadProbe(const Byte* data, size_t n, ReadProbeArgs& out) {
  (void)data;
  (void)n;
  (void)out;
  return false;
}
Bytes encodeReadProbeReply(const ReadProbeReply& reply) {
  (void)reply;
  return Bytes{};
}
bool decodeReadProbeReply(const Byte* data, size_t n, ReadProbeReply& out) {
  (void)data;
  (void)n;
  (void)out;
  return false;
}

}  // namespace raftkv::raft
