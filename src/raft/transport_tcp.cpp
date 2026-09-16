#include "raft/transport_tcp.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <utility>

#include "raft/message.h"

namespace raftkv::raft {

namespace {

constexpr uint32_t kMaxFrameLen = 64u * 1024u * 1024u;

bool writeFull(int fd, const Byte* data, size_t len) {
  size_t sent = 0;
  while (sent < len) {
    ssize_t r = ::send(fd, data + sent, len - sent, MSG_NOSIGNAL);
    if (r < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    if (r == 0) return false;
    sent += static_cast<size_t>(r);
  }
  return true;
}

bool readFull(int fd, Byte* data, size_t len) {
  size_t got = 0;
  while (got < len) {
    ssize_t r = ::recv(fd, data + got, len - got, 0);
    if (r < 0) {
      if (errno == EINTR) continue;
      return false;  // includes SO_RCVTIMEO expiry (EAGAIN/EWOULDBLOCK)
    }
    if (r == 0) return false;  // peer closed
    got += static_cast<size_t>(r);
  }
  return true;
}

int connectPeer(const std::string& hostport, uint64_t timeoutMs) {
  const size_t sep = hostport.rfind(':');
  if (sep == std::string::npos) return -1;
  const std::string host = hostport.substr(0, sep);
  const std::string port = hostport.substr(sep + 1);

  struct addrinfo hints {};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  struct addrinfo* res = nullptr;
  if (::getaddrinfo(host.c_str(), port.c_str(), &hints, &res) != 0) return -1;

  int fd = -1;
  for (struct addrinfo* p = res; p != nullptr; p = p->ai_next) {
    fd = ::socket(p->ai_family, p->ai_socktype, p->ai_protocol);
    if (fd < 0) continue;
    if (::connect(fd, p->ai_addr, p->ai_addrlen) == 0) break;
    ::close(fd);
    fd = -1;
  }
  ::freeaddrinfo(res);
  if (fd < 0) return -1;

  timeval tv{};
  tv.tv_sec = static_cast<time_t>(timeoutMs / 1000);
  tv.tv_usec = static_cast<suseconds_t>((timeoutMs % 1000) * 1000);
  ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
  return fd;
}

}  // namespace

TcpTransport::TcpTransport(std::unordered_map<int, std::string> peers,
                           uint64_t rpcTimeoutMs)
    : peers_(std::move(peers)),
      rpcTimeoutMs_(rpcTimeoutMs),
      installTimeoutMs_(2000) {}

TcpTransport::~TcpTransport() {
  for (auto& [id, fd] : conns_) {
    if (fd >= 0) ::close(fd);
  }
}

int TcpTransport::getConnection(int peerId) {
  auto it = conns_.find(peerId);
  if (it != conns_.end() && it->second >= 0) return it->second;

  auto p = peers_.find(peerId);
  if (p == peers_.end()) return -1;
  const int fd = connectPeer(p->second, rpcTimeoutMs_);
  if (fd < 0) return -1;
  conns_[peerId] = fd;
  return fd;
}

void TcpTransport::dropConnection(int peerId) {
  auto it = conns_.find(peerId);
  if (it != conns_.end()) {
    ::close(it->second);
    conns_.erase(it);
  }
}

bool TcpTransport::roundTrip(int peerId, MsgType reqType,
                             const Bytes& reqPayload, MsgType& replyType,
                             Bytes& replyPayload, uint64_t timeoutMs) {
  std::lock_guard<std::mutex> lock(mu_);
  const int fd = getConnection(peerId);
  if (fd < 0) return false;

  // Per-call timeout: InstallSnapshot makes the peer do disk I/O (write +
  // fsync + compact), which takes far longer than a heartbeat.
  timeval tv{};
  tv.tv_sec = static_cast<time_t>(timeoutMs / 1000);
  tv.tv_usec = static_cast<suseconds_t>((timeoutMs % 1000) * 1000);
  ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

  const Bytes frame = encodeFrame(reqType, reqPayload);
  if (!writeFull(fd, frame.data(), frame.size())) {
    dropConnection(peerId);
    return false;
  }

  Byte lenBuf[4];
  if (!readFull(fd, lenBuf, sizeof(lenBuf))) {
    dropConnection(peerId);
    return false;
  }
  const uint32_t len = getU32(lenBuf);
  if (len < 1 || len > kMaxFrameLen) {
    dropConnection(peerId);
    return false;
  }

  Bytes body(len);
  if (!readFull(fd, body.data(), body.size())) {
    dropConnection(peerId);
    return false;
  }

  Bytes whole;
  whole.reserve(4 + len);
  whole.insert(whole.end(), lenBuf, lenBuf + 4);
  whole.insert(whole.end(), body.begin(), body.end());
  if (!decodeFrame(whole.data(), whole.size(), replyType, replyPayload)) {
    dropConnection(peerId);
    return false;
  }
  return true;
}

void TcpTransport::sendRequestVote(int peerId, const RequestVoteArgs& args,
                                   VoteCb cb) {
  MsgType replyType = MsgType::kRequestVoteReply;
  Bytes replyPayload;
  if (!roundTrip(peerId, MsgType::kRequestVote, encodeRequestVote(args),
                 replyType, replyPayload, rpcTimeoutMs_)) {
    return;  // dropped / timed out: no callback (sender treats as timeout)
  }
  RequestVoteReply reply;
  if (replyType == MsgType::kRequestVoteReply &&
      decodeRequestVoteReply(replyPayload.data(), replyPayload.size(), reply)) {
    cb(reply);
  }
}

void TcpTransport::sendAppendEntries(int peerId, const AppendEntriesArgs& args,
                                     AppendCb cb) {
  MsgType replyType = MsgType::kAppendEntriesReply;
  Bytes replyPayload;
  if (!roundTrip(peerId, MsgType::kAppendEntries, encodeAppendEntries(args),
                 replyType, replyPayload, rpcTimeoutMs_)) {
    return;  // dropped / timed out
  }
  AppendEntriesReply reply;
  if (replyType == MsgType::kAppendEntriesReply &&
      decodeAppendEntriesReply(replyPayload.data(), replyPayload.size(), reply)) {
    cb(reply);
  }
}

void TcpTransport::addPeer(int id, const std::string& addr) {
  std::lock_guard<std::mutex> lock(mu_);
  const auto it = peers_.find(id);
  if (it != peers_.end() && it->second == addr) return;
  peers_[id] = addr;
  dropConnection(id);  // 地址变化：旧连接作废，下次重连新地址
}

void TcpTransport::removePeer(int id) {
  std::lock_guard<std::mutex> lock(mu_);
  peers_.erase(id);
  dropConnection(id);
}

void TcpTransport::sendReadProbe(int peerId, const ReadProbeArgs& args,
                                 ReadProbeCb cb) {
  MsgType replyType = MsgType::kReadProbeReply;
  Bytes replyPayload;
  if (!roundTrip(peerId, MsgType::kReadProbe, encodeReadProbe(args), replyType,
                 replyPayload, rpcTimeoutMs_)) {
    return;  // 丢包/超时：Leader 会因凑不齐多数派而拒绝读（不失线性一致）
  }
  ReadProbeReply reply;
  if (replyType == MsgType::kReadProbeReply &&
      decodeReadProbeReply(replyPayload.data(), replyPayload.size(), reply)) {
    cb(reply);
  }
}

void TcpTransport::sendInstallSnapshot(int peerId,
                                       const InstallSnapshotArgs& args,
                                       InstallCb cb) {
  MsgType replyType = MsgType::kInstallSnapshotReply;
  Bytes replyPayload;
  if (!roundTrip(peerId, MsgType::kInstallSnapshot,
                 encodeInstallSnapshot(args), replyType, replyPayload,
                 installTimeoutMs_)) {
    return;  // dropped / timed out
  }
  InstallSnapshotReply reply;
  if (replyType == MsgType::kInstallSnapshotReply &&
      decodeInstallSnapshotReply(replyPayload.data(), replyPayload.size(),
                                 reply)) {
    cb(reply);
  }
}

}  // namespace raftkv::raft
