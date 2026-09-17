#include "server.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <csignal>
#include <cstring>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_set>

#include "codec.h"
#include "common.h"
#include "store.h"
#include "thread_pool.h"

namespace raftkv {

namespace {

volatile std::sig_atomic_t g_stop = 0;

void onSignal(int /*sig*/) { g_stop = 1; }

bool readFully(int fd, void* buf, size_t len) {
  auto* p = static_cast<Byte*>(buf);
  size_t got = 0;
  while (got < len) {
    ssize_t r = ::read(fd, p + got, len - got);
    if (r < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    if (r == 0) return false;  // EOF before the full frame
    got += static_cast<size_t>(r);
  }
  return true;
}

bool writeFully(int fd, const void* buf, size_t len) {
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

// Tracks live client connections so shutdown can unblock their handlers.
class ConnRegistry {
 public:
  void add(int fd) {
    std::lock_guard<std::mutex> lock(mu_);
    fds_.insert(fd);
  }

  // Remove + close. Safe to call more than once (later calls are no-ops).
  void release(int fd) {
    std::lock_guard<std::mutex> lock(mu_);
    if (fds_.erase(fd) == 1) {
      ::shutdown(fd, SHUT_RDWR);
      ::close(fd);
    }
  }

  // Shutdown() first to unblock handlers stuck in recv(), then close.
  void closeAll() {
    std::lock_guard<std::mutex> lock(mu_);
    for (const int fd : fds_) ::shutdown(fd, SHUT_RDWR);
    for (const int fd : fds_) ::close(fd);
    fds_.clear();
  }

 private:
  std::mutex mu_;
  std::unordered_set<int> fds_;
};

void sendError(int fd, const std::string& msg) {
  Response resp;
  resp.status = StatusCode::kErr;
  resp.payload = msg;
  const Bytes frame = encodeResponse(resp);
  (void)writeFully(fd, frame.data(), frame.size());
}

// Serves one connection until EOF/error, one request-response at a time.
void handleConn(int fd, Store& store, ConnRegistry& registry) {
  Byte hdr[kReqHeaderLen];
  while (!g_stop) {
    if (!readFully(fd, hdr, sizeof(hdr))) break;  // clean close or error

    const size_t keyLen = getU16(hdr + 2);
    const size_t valLen = getU32(hdr + 4);
    const size_t bodyLen = keyLen + valLen;
    if (kReqHeaderLen + bodyLen > kMaxRequestBytes) {
      sendError(fd, "request too large");
      break;
    }

    Bytes frame(hdr, hdr + kReqHeaderLen);
    const size_t before = frame.size();
    frame.resize(before + bodyLen);
    if (bodyLen > 0 && !readFully(fd, frame.data() + before, bodyLen)) break;

    Request req;
    if (!parseRequest(frame.data(), frame.size(), req)) {
      sendError(fd, "bad request");
      break;
    }

    Response resp;
    switch (req.op) {
      case OpCode::kPut: {
        const StoreResult r = store.put(req.key, req.value);
        if (!r.ok) {
          resp.status = StatusCode::kErr;
          resp.payload = r.err;
        }
        break;
      }
      case OpCode::kGet: {
        const StoreResult r = store.get(req.key);
        if (!r.ok) {
          resp.status = StatusCode::kErr;
          resp.payload = r.err;
        } else if (r.found) {
          resp.payload = r.value;
        } else {
          resp.status = StatusCode::kNotFound;
        }
        break;
      }
      case OpCode::kDel: {
        const StoreResult r = store.del(req.key);
        if (!r.ok) {
          resp.status = StatusCode::kErr;
          resp.payload = r.err;
        }
        break;
      }
      case OpCode::kConfig:
        // 不可达：kConfig 不在 M1 codec 白名单内（parseRequest 会先拒绝），
        // 客户端无法伪造配置条目（m4-prerequisites.md §7.1 P3）。
        // 显式列出而非用 default:，既守住 -Wswitch 零告警，
        // 也不会让将来新增的 OpCode 被静默吞掉。
        resp.status = StatusCode::kErr;
        resp.payload = "config entries are not accepted on this protocol";
        break;
    }

    const Bytes out = encodeResponse(resp);
    if (!writeFully(fd, out.data(), out.size())) break;
  }
  registry.release(fd);
}

}  // namespace

int runServer(const ServerOptions& opt) {
  if (opt.workers == 0) {
    throw std::runtime_error("--workers must be >= 1");
  }
  std::signal(SIGPIPE, SIG_IGN);

  struct sigaction sa {};
  sa.sa_handler = onSignal;
  sigemptyset(&sa.sa_mask);
  if (::sigaction(SIGINT, &sa, nullptr) != 0 ||
      ::sigaction(SIGTERM, &sa, nullptr) != 0) {
    throw std::runtime_error("sigaction failed: " +
                             std::string(std::strerror(errno)));
  }
  g_stop = 0;

  Store store(opt.dataDir, opt.sync);
  logInfo("data dir: " + opt.dataDir + " (sync=" + (opt.sync ? "on" : "off") +
          ")");

  const int lsock = ::socket(AF_INET, SOCK_STREAM, 0);
  if (lsock < 0) {
    throw std::runtime_error("socket failed: " +
                             std::string(std::strerror(errno)));
  }
  {
    const int one = 1;
    ::setsockopt(lsock, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  }
  sockaddr_in addr {};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons(static_cast<uint16_t>(opt.port));
  if (::bind(lsock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    const std::string err = std::strerror(errno);
    ::close(lsock);
    throw std::runtime_error("bind 0.0.0.0:" + std::to_string(opt.port) +
                             " failed: " + err);
  }
  if (::listen(lsock, 128) != 0) {
    const std::string err = std::strerror(errno);
    ::close(lsock);
    throw std::runtime_error("listen failed: " + err);
  }
  logInfo("listening on 0.0.0.0:" + std::to_string(opt.port));

  ThreadPool pool(opt.workers);
  ConnRegistry registry;
  while (!g_stop) {
    const int c = ::accept(lsock, nullptr, nullptr);
    if (c < 0) {
      if (errno == EINTR) continue;  // signal handler may have set g_stop
      if (g_stop) break;
      logError("accept: " + std::string(std::strerror(errno)));
      continue;
    }
    registry.add(c);
    pool.submit([c, &store, &registry] { handleConn(c, store, registry); });
  }

  ::close(lsock);
  registry.closeAll();  // unblock handlers, then join the pool
  pool.stop();
  logInfo("shutdown complete");
  return 0;
}

}  // namespace raftkv
