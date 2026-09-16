// raftkv raft cluster client: put/get/del/status + interactive REPL.
// Talks the raft client protocol (MsgType::kClientRequest / kStatusRequest),
// with NOT_LEADER redirect + retry (same requestId across retries).
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <iostream>
#include <mutex>
#include <algorithm>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "raft/message.h"

using namespace raftkv;
using namespace raftkv::raft;

namespace {

struct Options {
  std::string host = "127.0.0.1";
  int port = 19601;
  std::unordered_map<int, std::string> peers;  // --peers 种子：id -> "host:port"
  // M4（决策⑦）：运行期拓扑缓存。客户端按最新配置路由，不再被 --peers 静态列表限制。
  uint64_t configVersion = 0;                       // 已缓存的配置版本（0 = 未知）
  std::unordered_map<int, std::string> topology;     // 最新 id -> "host:port"
  uint64_t clientId =
      static_cast<uint64_t>(::getpid()) * 1000003u +
      static_cast<uint64_t>(
          std::chrono::steady_clock::now().time_since_epoch().count());
  uint64_t requestId = 0;
  size_t pipeline = 1;  // fill: in-flight requests per window
};

bool readFull(int fd, void* buf, size_t len) {
  auto* p = static_cast<Byte*>(buf);
  size_t got = 0;
  while (got < len) {
    ssize_t r = ::recv(fd, p + got, len - got, 0);
    if (r < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    if (r == 0) return false;
    got += static_cast<size_t>(r);
  }
  return true;
}

bool writeFull(int fd, const void* buf, size_t len) {
  auto* p = static_cast<const Byte*>(buf);
  size_t sent = 0;
  while (sent < len) {
    ssize_t r = ::send(fd, p + sent, len - sent, MSG_NOSIGNAL);
    if (r < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    if (r == 0) return false;
    sent += static_cast<size_t>(r);
  }
  return true;
}

void setSocketTimeout(int fd, uint64_t ms) {
  timeval tv{};
  tv.tv_sec = static_cast<time_t>(ms / 1000);
  tv.tv_usec = static_cast<suseconds_t>((ms % 1000) * 1000);
  ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
}

int connectTo(const std::string& host, int port) {
  struct addrinfo hints {};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  struct addrinfo* res = nullptr;
  const std::string portStr = std::to_string(port);
  if (::getaddrinfo(host.c_str(), portStr.c_str(), &hints, &res) != 0) return -1;
  int fd = -1;
  for (struct addrinfo* p = res; p != nullptr; p = p->ai_next) {
    fd = ::socket(p->ai_family, p->ai_socktype, p->ai_protocol);
    if (fd < 0) continue;
    if (::connect(fd, p->ai_addr, p->ai_addrlen) == 0) break;
    ::close(fd);
    fd = -1;
  }
  ::freeaddrinfo(res);
  if (fd >= 0) {
    // A hung node must not block the client forever (status to a stopped
    // peer should fail fast instead of hanging).
    timeval tv{};
    tv.tv_sec = 0;
    tv.tv_usec = 500000;
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
  }
  return fd;
}

bool parseHostPort(const std::string& s, std::string& host, int& port) {
  const size_t sep = s.rfind(':');
  if (sep == std::string::npos) return false;
  host = s.substr(0, sep);
  port = std::stoi(s.substr(sep + 1));
  return true;
}

// One request over a fresh connection; returns reply or nullptr via `ok`.
// timeoutMs = 0 表示沿用默认的 500ms 连接超时（普通请求）；
// 成员变更这类"节点侧要跑 CatchUp + 提交"的请求需要显式放宽。
bool requestWithTimeout(const std::string& host, int port, MsgType type,
                        const Bytes& payload, MsgType& replyType,
                        Bytes& replyPayload, uint64_t timeoutMs);

bool request(const std::string& host, int port, MsgType type,
             const Bytes& payload, MsgType& replyType, Bytes& replyPayload) {
  return requestWithTimeout(host, port, type, payload, replyType, replyPayload, 0);
}

bool requestWithTimeout(const std::string& host, int port, MsgType type,
                        const Bytes& payload, MsgType& replyType,
                        Bytes& replyPayload, uint64_t timeoutMs) {
  const int fd = connectTo(host, port);
  if (fd < 0) return false;
  if (timeoutMs > 0) setSocketTimeout(fd, timeoutMs);
  const Bytes frame = encodeFrame(type, payload);
  const bool w = writeFull(fd, frame.data(), frame.size());
  if (!w) {
    ::close(fd);
    return false;
  }
  Byte lenBuf[4];
  if (!readFull(fd, lenBuf, sizeof(lenBuf))) {
    ::close(fd);
    return false;
  }
  const uint32_t len = getU32(lenBuf);
  Bytes body(len);
  if (len == 0 || !readFull(fd, body.data(), body.size())) {
    ::close(fd);
    return false;
  }
  Bytes whole;
  whole.reserve(4 + len);
  whole.insert(whole.end(), lenBuf, lenBuf + 4);
  whole.insert(whole.end(), body.begin(), body.end());
  ::close(fd);
  return decodeFrame(whole.data(), whole.size(), replyType, replyPayload);
}

// ---- M4（决策⑦）：客户端拓扑缓存 ----

// 单个 id -> "host:port"：最新拓扑缓存优先，回退到 --peers 种子。
std::string peerAddrOf(const Options& o, int id) {
  const auto it = o.topology.find(id);
  if (it != o.topology.end() && !it->second.empty()) return it->second;
  const auto seed = o.peers.find(id);
  return seed == o.peers.end() ? std::string() : seed->second;
}

bool resolvePeer(const Options& o, int id, std::string& host, int& port) {
  const std::string addr = peerAddrOf(o, id);
  if (addr.empty()) return false;
  return parseHostPort(addr, host, port);
}

// 已知节点（拓扑缓存优先，去重；跳过 avoid）。
std::vector<std::pair<std::string, int>> knownNodes(const Options& o,
                                                    const std::string& avoidHost,
                                                    int avoidPort) {
  std::vector<std::pair<std::string, int>> out;
  auto push = [&](const std::unordered_map<int, std::string>& m) {
    for (const auto& kv : m) {
      std::string h;
      int p = 0;
      if (kv.second.empty() || !parseHostPort(kv.second, h, p)) continue;
      if (h == avoidHost && p == avoidPort) continue;
      const auto dup = std::find(out.begin(), out.end(),
                                 std::make_pair(h, p));
      if (dup == out.end()) out.emplace_back(h, p);
    }
  };
  push(o.topology);
  push(o.peers);
  return out;
}

bool pickOtherNode(const Options& o, const std::string& avoidHost, int avoidPort,
                   std::string& host, int& port) {
  const auto nodes = knownNodes(o, avoidHost, avoidPort);
  if (nodes.empty()) return false;
  host = nodes.front().first;
  port = nodes.front().second;
  return true;
}

// 向任意可达节点索取最新配置视图并更新缓存。
// version 单调：只有 >= 已缓存版本才覆盖（陈旧节点的回答不得让缓存回退）。
// out != nullptr 时回带原始应答（config 命令复用同一次请求）。
bool refreshTopology(Options& o, const std::string& host, int port,
                     ConfigReplyArgs* out = nullptr) {
  std::vector<std::pair<std::string, int>> targets;
  targets.emplace_back(host, port);
  for (const auto& n : knownNodes(o, "", 0)) targets.push_back(n);

  for (const auto& t : targets) {
    MsgType rt = MsgType::kClientReply;
    Bytes rp;
    ConfigRequestArgs args;  // action = 0 (get)：任何节点都能回答自己的配置视图
    if (!request(t.first, t.second, MsgType::kConfigRequest,
                 encodeConfigRequest(args), rt, rp)) {
      continue;
    }
    ConfigReplyArgs reply;
    if (rt != MsgType::kConfigReply ||
        !decodeConfigReply(rp.data(), rp.size(), reply)) {
      continue;
    }
    if (out != nullptr) *out = reply;
    if (reply.config.version >= o.configVersion) {
      std::unordered_map<int, std::string> addrs;
      for (const Member& m : reply.config.members) {
        if (!m.addr.empty()) addrs[m.id] = m.addr;
      }
      if (!addrs.empty()) {
        o.topology = std::move(addrs);
        o.configVersion = reply.config.version;
      }
    }
    return true;
  }
  return false;
}

// Same as request() but over an already-open connection (used by fill).
bool requestOnFd(int fd, MsgType type, const Bytes& payload, MsgType& replyType,
                 Bytes& replyPayload) {
  const Bytes frame = encodeFrame(type, payload);
  if (!writeFull(fd, frame.data(), frame.size())) return false;
  Byte lenBuf[4];
  if (!readFull(fd, lenBuf, sizeof(lenBuf))) return false;
  const uint32_t len = getU32(lenBuf);
  if (len == 0) return false;
  Bytes body(len);
  if (!readFull(fd, body.data(), body.size())) return false;
  Bytes whole;
  whole.reserve(4 + len);
  whole.insert(whole.end(), lenBuf, lenBuf + 4);
  whole.insert(whole.end(), body.begin(), body.end());
  return decodeFrame(whole.data(), whole.size(), replyType, replyPayload);
}

int doOp(Options& o, OpCode op, const std::string& key,
         const std::string& value) {
  const uint64_t requestId = ++o.requestId;  // reused across redirect retries
  std::string host = o.host;
  int port = o.port;
  bool refreshed = false;  // 本次调用是否已经"失效重取"过（避免打转）

  for (int attempt = 0; attempt <= 3; ++attempt) {
    ClientRequest req;
    req.op = op;
    req.key = key;
    req.value = value;
    req.clientId = o.clientId;
    req.requestId = requestId;

    MsgType replyType = MsgType::kClientReply;
    Bytes replyPayload;
    if (!request(host, port, MsgType::kClientRequest, encodeClientRequest(req),
                 replyType, replyPayload)) {
      // M4（决策⑦）：连不上当前节点 -> 失效重取拓扑，换一个已知节点重试一次
      if (!refreshed) {
        refreshed = true;
        std::string altHost = host;
        int altPort = port;
        if (refreshTopology(o, host, port) &&
            pickOtherNode(o, host, port, altHost, altPort)) {
          host = altHost;
          port = altPort;
          continue;
        }
      }
      std::cerr << "cannot reach " << host << ":" << port << "\n";
      return -1;
    }
    ClientReply reply;
    if (replyType != MsgType::kClientReply ||
        !decodeClientReply(replyPayload.data(), replyPayload.size(), reply)) {
      std::cerr << "bad reply from " << host << ":" << port << "\n";
      return -1;
    }

    if (reply.status == ClientStatus::kNotLeader) {
      if (reply.leaderHint > 0) {
        std::string h = host;
        int p = port;
        if (resolvePeer(o, reply.leaderHint, h, p)) {
          host = h;
          port = p;
          continue;  // redirect to the leader and retry
        }
        // 缓存里没有这个 Leader（例如刚加入的节点当选）-> 失效重取一次
        if (!refreshed && refreshTopology(o, host, port) &&
            resolvePeer(o, reply.leaderHint, h, p)) {
          refreshed = true;
          host = h;
          port = p;
          continue;
        }
      }
      std::cout << "NOT_LEADER\n";
      return 1;
    }

    if (reply.status == ClientStatus::kErr) {
      std::cout << "ERR " << reply.value << "\n";
      return 1;
    }
    if (op == OpCode::kGet) {
      if (reply.status == ClientStatus::kNotFound) {
        std::cout << "NOT_FOUND\n";
      } else {
        std::cout << reply.value << "\n";
      }
    } else {
      std::cout << "OK\n";
    }
    return 0;
  }
  std::cout << "ERR redirect loop\n";
  return 1;
}

int doStatus(Options& o) {
  MsgType replyType = MsgType::kClientReply;
  Bytes replyPayload;
  if (!request(o.host, o.port, MsgType::kStatusRequest, {}, replyType,
               replyPayload)) {
    std::cerr << "cannot reach " << o.host << ":" << o.port << "\n";
    return -1;
  }
  ClientReply reply;
  if (replyType != MsgType::kClientReply ||
      !decodeClientReply(replyPayload.data(), replyPayload.size(), reply)) {
    return -1;
  }
  std::cout << reply.value << "\n";
  return 0;
}

int doSnapshot(Options& o) {
  MsgType replyType = MsgType::kClientReply;
  Bytes replyPayload;
  if (!request(o.host, o.port, MsgType::kSnapshotTrigger, {}, replyType,
               replyPayload)) {
    std::cerr << "cannot reach " << o.host << ":" << o.port << "\n";
    return -1;
  }
  ClientReply reply;
  if (replyType != MsgType::kClientReply ||
      !decodeClientReply(replyPayload.data(), replyPayload.size(), reply)) {
    return -1;
  }
  std::cout << reply.value << "\n";
  return 0;
}

// Reads one reply frame from an open connection.
bool readFrame(int fd, MsgType& type, Bytes& payload) {
  Byte lenBuf[4];
  if (!readFull(fd, lenBuf, sizeof(lenBuf))) return false;
  const uint32_t len = getU32(lenBuf);
  if (len == 0) return false;
  Bytes body(len);
  if (!readFull(fd, body.data(), body.size())) return false;
  Bytes whole;
  whole.reserve(4 + len);
  whole.insert(whole.end(), lenBuf, lenBuf + 4);
  whole.insert(whole.end(), body.begin(), body.end());
  return decodeFrame(whole.data(), whole.size(), type, payload);
}

int doFill(Options& o, uint64_t n, size_t pipeline) {
  if (pipeline < 1) pipeline = 1;
  const uint64_t base = o.requestId;  // stable requestIds => idempotent retries

  std::mutex addrMu;  // guards the (shared) leader address for redirects
  std::string host = o.host;
  int port = o.port;
  std::atomic<uint64_t> next{0};
  std::atomic<bool> failed{false};

  // `pipeline` concurrent connections -> the server sees that many concurrent
  // propose() calls, which is what lets group commit amortise one fsync.
  //
  // Each worker gets its OWN clientId: the idempotency contract (m2-design.md
  // §5.4) is "requestIds of one client are monotonic", and the state machine
  // drops requestId <= last seen. Sharing one clientId across workers (whose
  // requestIds are assigned out of order) silently discards writes.
  auto worker = [&](size_t workerIndex) {
    int fd = -1;
    while (!failed.load()) {
      const uint64_t i = next.fetch_add(1);
      if (i >= n) break;

      bool ok = false;
      for (int attempt = 0; attempt < 200 && !ok && !failed.load(); ++attempt) {
        if (fd < 0) {
          std::lock_guard<std::mutex> lk(addrMu);
          fd = connectTo(host, port);
        }
        if (fd < 0) {
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
          continue;
        }

        ClientRequest req;
        req.op = OpCode::kPut;
        req.key = "f" + std::to_string(i);
        req.value = "v";
        req.clientId = o.clientId + workerIndex;
        req.requestId = base + i + 1;

        MsgType rt = MsgType::kClientReply;
        Bytes rp;
        if (!requestOnFd(fd, MsgType::kClientRequest, encodeClientRequest(req),
                         rt, rp)) {
          ::close(fd);
          fd = -1;
          continue;
        }
        ClientReply reply;
        if (rt != MsgType::kClientReply ||
            !decodeClientReply(rp.data(), rp.size(), reply)) {
          ::close(fd);
          fd = -1;
          continue;
        }
        if (reply.status == ClientStatus::kNotLeader) {
          ::close(fd);
          fd = -1;
          // M4（决策⑦）：先查拓扑缓存，查不到再失效重取一次。
          // 持 addrMu 执行：多个 worker 不会同时打拓扑接口，也不会撕裂 host/port。
          std::lock_guard<std::mutex> lk(addrMu);
          if (reply.leaderHint > 0) {
            std::string h = host;
            int p = port;
            if (resolvePeer(o, reply.leaderHint, h, p) ||
                (refreshTopology(o, host, port) &&
                 resolvePeer(o, reply.leaderHint, h, p))) {
              host = h;
              port = p;
            }
          }
          continue;
        }
        if (reply.status == ClientStatus::kErr) {
          std::cerr << "fill: server error: " << reply.value << "\n";
          failed.store(true);
          break;
        }
        ok = true;
      }
      if (!ok) failed.store(true);
    }
    if (fd >= 0) ::close(fd);
  };

  std::vector<std::thread> workers;
  workers.reserve(pipeline);
  for (size_t k = 0; k < pipeline; ++k) workers.emplace_back(worker, k);
  for (auto& t : workers) t.join();

  o.requestId = base + n;
  if (failed.load()) {
    std::cerr << "fill: failed\n";
    return 1;
  }
  std::cout << "filled " << n << "\n";
  return 0;
}

// Reads back f0..f{n-1} and reports how many are missing. A concurrent `fill`
// must not lose writes: requestIds of one client have to stay monotonic, which
// is why every fill worker uses its own clientId.
// ---- M4: 集群配置查询与成员变更 ----

std::string membersToString(const ClusterConfig& c) {
  std::ostringstream ss;
  for (size_t i = 0; i < c.members.size(); ++i) {
    if (i != 0) ss << ",";
    ss << c.members[i].id << ":" << c.members[i].addr << ":"
       << (c.members[i].voting ? "v" : "n");
  }
  return ss.str();
}

int doConfig(Options& o) {
  // M4（决策⑦）：config 命令顺便刷新客户端拓扑缓存（同一个请求）
  ConfigReplyArgs reply;
  if (!refreshTopology(o, o.host, o.port, &reply)) {
    std::cerr << "cannot reach " << o.host << ":" << o.port << "\n";
    return -1;
  }
  std::cout << "config_version=" << reply.config.version
            << " members=" << membersToString(reply.config) << "\n";
  return 0;
}

// action: 1 = add, 2 = remove。非 Leader 时按 leaderHint + --peers 重定向重试。
int doMembership(Options& o, uint8_t action, int targetId,
                 const std::string& addr) {
  std::string host = o.host;
  int port = o.port;
  bool refreshed = false;
  for (int attempt = 0; attempt < 20; ++attempt) {
    MsgType rt = MsgType::kClientReply;
    Bytes rp;
    ConfigRequestArgs args;
    args.action = action;
    args.targetId = targetId;
    args.addr = addr;
    // 节点侧要跑 CatchUp + 提交，放宽读超时（默认 500ms 不够）
    if (!requestWithTimeout(host, port, MsgType::kConfigRequest,
                            encodeConfigRequest(args), rt, rp, 15000)) {
      // M4（决策⑦）：连不上 -> 失效重取拓扑，换一个已知节点重试一次
      if (!refreshed) {
        refreshed = true;
        std::string altHost = host;
        int altPort = port;
        if (refreshTopology(o, host, port) &&
            pickOtherNode(o, host, port, altHost, altPort)) {
          host = altHost;
          port = altPort;
          continue;
        }
      }
      std::cerr << "cannot reach " << host << ":" << port << "\n";
      return -1;
    }
    ConfigReplyArgs reply;
    if (rt != MsgType::kConfigReply ||
        !decodeConfigReply(rp.data(), rp.size(), reply)) {
      std::cerr << "bad config reply\n";
      return -1;
    }
    if (reply.ok) {
      std::cout << "OK config_version=" << reply.config.version
                << " members=" << membersToString(reply.config) << "\n";
      return 0;
    }
    if (reply.leaderHint > 0) {
      std::string h = host;
      int p = port;
      if (resolvePeer(o, reply.leaderHint, h, p) ||
          (!refreshed && refreshTopology(o, host, port) &&
           resolvePeer(o, reply.leaderHint, h, p))) {
        refreshed = true;
        host = h;  // 重定向到 Leader 再试
        port = p;
        continue;
      }
    }
    break;
  }
  std::cerr << "membership change rejected\n";
  return 1;
}

int doVerify(Options& o, uint64_t n) {
  std::mutex addrMu;  // guards the (shared) leader address for redirects
  std::string host = o.host;
  int port = o.port;
  std::atomic<uint64_t> next{0};
  std::atomic<uint64_t> missing{0};
  std::atomic<bool> failed{false};
  const size_t workers = 8;

  auto worker = [&]() {
    int fd = -1;
    while (!failed.load()) {
      const uint64_t i = next.fetch_add(1);
      if (i >= n) break;

      bool ok = false;
      for (int attempt = 0; attempt < 200 && !ok && !failed.load(); ++attempt) {
        if (fd < 0) {
          std::lock_guard<std::mutex> lk(addrMu);
          fd = connectTo(host, port);
        }
        if (fd < 0) {
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
          continue;
        }

        ClientRequest req;
        req.op = OpCode::kGet;  // reads never enter the log
        req.key = "f" + std::to_string(i);
        MsgType rt = MsgType::kClientReply;
        Bytes rp;
        if (!requestOnFd(fd, MsgType::kClientRequest, encodeClientRequest(req),
                         rt, rp)) {
          ::close(fd);
          fd = -1;
          continue;
        }
        ClientReply reply;
        if (rt != MsgType::kClientReply ||
            !decodeClientReply(rp.data(), rp.size(), reply)) {
          ::close(fd);
          fd = -1;
          continue;
        }
        if (reply.status == ClientStatus::kNotLeader) {
          ::close(fd);
          fd = -1;
          // M4（决策⑦）：同 fill：拓扑缓存优先，miss 才失效重取
          std::lock_guard<std::mutex> lk(addrMu);
          if (reply.leaderHint > 0) {
            std::string h = host;
            int p = port;
            if (resolvePeer(o, reply.leaderHint, h, p) ||
                (refreshTopology(o, host, port) &&
                 resolvePeer(o, reply.leaderHint, h, p))) {
              host = h;
              port = p;
            }
          }
          continue;
        }
        if (reply.status == ClientStatus::kNotFound) missing.fetch_add(1);
        ok = (reply.status == ClientStatus::kOk ||
              reply.status == ClientStatus::kNotFound);
      }
      if (!ok) failed.store(true);
    }
    if (fd >= 0) ::close(fd);
  };

  std::vector<std::thread> pool;
  pool.reserve(workers);
  for (size_t k = 0; k < workers; ++k) pool.emplace_back(worker);
  for (auto& t : pool) t.join();

  if (failed.load()) {
    std::cerr << "verify: failed\n";
    return 1;
  }
  const uint64_t lost = missing.load();
  std::cout << "verified " << n << " missing " << lost << "\n";
  return lost == 0 ? 0 : 1;
}

std::string trimLeft(const std::string& s) {
  const size_t p = s.find_first_not_of(" \t");
  return p == std::string::npos ? std::string() : s.substr(p);
}

int runRepl(Options& o) {
  const bool tty = ::isatty(STDIN_FILENO) == 1;
  if (tty) std::cout << "raftkv> " << std::flush;
  std::string line;
  while (std::getline(std::cin, line)) {
    std::string cmd, key, value;
    {
      std::istringstream ss(line);
      if (!(ss >> cmd)) {
        // blank
      } else if (cmd == "put") {
        if (!(ss >> key)) cmd.clear();
        else { std::getline(ss, value); value = trimLeft(value); }
      } else {
        (void)(ss >> key);
      }
    }
    if (cmd == "quit" || cmd == "exit" || cmd == "q") break;
    if (cmd == "help") {
      std::cout << "put <key> <value> | get <key> | del <key> | status | snapshot | quit\n";
    } else if (cmd == "status") {
      doStatus(o);
    } else if (cmd == "snapshot") {
      doSnapshot(o);
    } else if (cmd == "put") {
      if (key.empty()) std::cout << "usage: put <key> <value>\n";
      else doOp(o, OpCode::kPut, key, value);
    } else if (cmd == "get") {
      if (key.empty()) std::cout << "usage: get <key>\n";
      else doOp(o, OpCode::kGet, key, "");
    } else if (cmd == "del") {
      if (key.empty()) std::cout << "usage: del <key>\n";
      else doOp(o, OpCode::kDel, key, "");
    } else if (!cmd.empty()) {
      std::cout << "unknown command: " << cmd << "\n";
    }
    if (tty) std::cout << "raftkv> " << std::flush;
  }
  return 0;
}

void usage(const char* argv0) {
  std::cerr << "usage:\n"
            << "  " << argv0
            << " --peers \"1=h:p,2=h:p,...\" [--host H] [--port P] put <k> <v>\n"
            << "  " << argv0 << " ... get <k>\n"
            << "  " << argv0 << " ... del <k>\n"
            << "  " << argv0 << " ... status\n"
            << "  " << argv0 << " ... fill <n> [--pipeline K]\n"
            << "  " << argv0 << " ... verify <n>\n"
            << "  " << argv0 << " ... config\n"
            << "  " << argv0 << " ... add <id> <host:port>\n"
            << "  " << argv0 << " ... remove <id>\n"
            << "  (no command -> interactive mode)\n"
            << "客户端会自动向任意可达节点索取最新集群配置（kConfigRequest）并按新"
               "拓扑路由，因此 --peers 只需给出一个可达种子。\n";
}

}  // namespace

int main(int argc, char** argv) {
  std::signal(SIGPIPE, SIG_IGN);
  Options o;

  std::vector<std::string> pos;
  for (int i = 1; i < argc; ++i) {
    const std::string a = argv[i];
    auto next = [&](const char* what) -> std::string {
      if (i + 1 >= argc) {
        std::cerr << "missing value for " << what << "\n";
        std::exit(2);
      }
      return argv[++i];
    };
    if (a == "--host") o.host = next("--host");
    else if (a == "--port") o.port = std::stoi(next("--port"));
    else if (a == "--pipeline") o.pipeline = std::stoul(next("--pipeline"));
    else if (a == "--peers") {
      const std::string s = next("--peers");
      size_t start = 0;
      while (start <= s.size()) {
        const size_t comma = s.find(',', start);
        const std::string item =
            s.substr(start, comma == std::string::npos ? std::string::npos
                                                       : comma - start);
        const size_t eq = item.find('=');
        if (eq != std::string::npos && !item.empty()) {
          o.peers[std::stoi(item.substr(0, eq))] = item.substr(eq + 1);
        }
        if (comma == std::string::npos) break;
        start = comma + 1;
      }
    } else if (!a.empty() && a[0] == '-') {
      std::cerr << "unknown option: " << a << "\n";
      usage(argv[0]);
      return 2;
    } else {
      pos.push_back(a);
    }
  }

  if (pos.empty()) return runRepl(o);

  const std::string cmd = pos[0];
  if (cmd == "status") return doStatus(o);
  if (cmd == "snapshot") return doSnapshot(o);
  if (cmd == "fill" && pos.size() >= 2) {
    return doFill(o, std::stoull(pos[1]), o.pipeline);
  }
  if (cmd == "verify" && pos.size() >= 2) {
    return doVerify(o, std::stoull(pos[1]));
  }
  if (cmd == "config") return doConfig(o);
  if (cmd == "add" && pos.size() >= 3) {
    return doMembership(o, /*action=*/1, std::stoi(pos[1]), pos[2]);
  }
  if (cmd == "remove" && pos.size() >= 2) {
    return doMembership(o, /*action=*/2, std::stoi(pos[1]), "");
  }
  if (cmd == "put" && pos.size() >= 3) return doOp(o, OpCode::kPut, pos[1], pos[2]);
  if (cmd == "get" && pos.size() >= 2) return doOp(o, OpCode::kGet, pos[1], "");
  if (cmd == "del" && pos.size() >= 2) return doOp(o, OpCode::kDel, pos[1], "");
  usage(argv[0]);
  return 2;
}
