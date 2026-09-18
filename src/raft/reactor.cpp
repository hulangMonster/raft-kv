// M5.3：Reactor 实现（单线程 epoll + 每 peer 连接状态机）。
#include "raft/reactor.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <deque>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "raft/lock_probe.h"

namespace raftkv::raft {

namespace {

constexpr int kMaxEvents = 64;
constexpr int kLoopWaitMs = 20;  // 兼顾超时精度与空转开销

bool parseHostPort(const std::string& s, std::string& host, uint16_t& port) {
  const size_t sep = s.rfind(':');
  if (sep == std::string::npos) return false;
  host = s.substr(0, sep);
  const int p = std::atoi(s.substr(sep + 1).c_str());
  if (p <= 0 || p > 65535) return false;
  port = static_cast<uint16_t>(p);
  return true;
}

int connectNonBlocking(const std::string& host, uint16_t port) {
  struct addrinfo hints {};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  struct addrinfo* res = nullptr;
  const std::string portStr = std::to_string(port);
  if (::getaddrinfo(host.c_str(), portStr.c_str(), &hints, &res) != 0) return -1;
  int fd = -1;
  for (struct addrinfo* p = res; p != nullptr; p = p->ai_next) {
    fd = ::socket(p->ai_family, p->ai_socktype | SOCK_NONBLOCK, p->ai_protocol);
    if (fd < 0) continue;
    const int rc = ::connect(fd, p->ai_addr, p->ai_addrlen);
    if (rc == 0 || errno == EINPROGRESS) break;
    ::close(fd);
    fd = -1;
  }
  ::freeaddrinfo(res);
  return fd;
}

bool writeAllNonBlocking(int fd, const Byte* data, size_t len, size_t& written) {
  written = 0;
  while (written < len) {
    const ssize_t r = ::send(fd, data + written, len - written, MSG_NOSIGNAL);
    if (r > 0) {
      written += static_cast<size_t>(r);
      continue;
    }
    if (r < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
      return true;  // 暂时写不动：等 EPOLLOUT
    }
    return false;  // 真错误
  }
  return true;
}

}  // namespace

struct Reactor::Impl {
  struct Pending {
    Bytes frame;  // 完整请求帧（含长度前缀）
    Completion cb;
    uint64_t deadlineMs = 0;
  };
  struct Peer {
    int id = 0;
    std::string host;
    uint16_t port = 0;
    int fd = -1;
    bool connecting = false;
    Bytes out;                  // 待写字节（可能是半个帧）
    Bytes in;                   // 已读字节
    // M5.6 修复（丢写根因）：队首帧已**完整写出**、正在等应答。没有这个标志时，
    // flushLocked 会在每次事件循环尾部（run() 的扫描）看到 queue 非空就重新把队首
    // 装进 out 再发一遍 —— 实测同一 seq 被完整发送 2.0 次（node3 有 44% 的帧重复），
    // 对端于是回两次；第二次答复会弹掉**下一批**的 Pending，造成 ack 归因错位
    // （实测 1~3% 的记功对应到 peer 从未处理的帧）-> matchIndex 虚高 -> 已 ack 的写丢失。
    bool awaitingReply = false;
    std::deque<Pending> queue;  // FIFO：一次一个请求在途（协议 request->reply 一一对应）
    size_t inflight() const { return queue.size(); }
  };

  int epfd = -1;
  int wakeFd = -1;
  std::atomic<bool> running{false};
  std::thread loop;
  mutable std::mutex mu;
  std::unordered_map<int, std::unique_ptr<Peer>> peers;

  // ---- 以下均在 reactor 线程内调用（peers 仍用 mu 保护，因为 send/removePeer 来自其它线程）

  static uint64_t nowMs() { return lockprobe::nowUs() / 1000ULL; }

  void wake() {
    if (wakeFd >= 0) {
      const uint64_t one = 1;
      ssize_t r = ::write(wakeFd, &one, sizeof(one));
      (void)r;
    }
  }

  void closePeerLocked(Peer& p, bool dropHead) {
    if (p.fd >= 0) {
      ::epoll_ctl(epfd, EPOLL_CTL_DEL, p.fd, nullptr);
      ::close(p.fd);
      p.fd = -1;
    }
    p.connecting = false;
    p.out.clear();
    p.in.clear();
    p.awaitingReply = false;
    if (dropHead && !p.queue.empty()) p.queue.pop_front();  // 超时/错误：回调不触发
  }

  // 尝试为 peer 建立连接（非阻塞）；成功后注册 EPOLLOUT 触发首次 flush。
  void ensureConnectingLocked(Peer& p) {
    if (p.fd >= 0 || p.connecting) return;
    const int fd = connectNonBlocking(p.host, p.port);
    if (fd < 0) return;  // 失败：下次再试（不触发回调，与超时语义一致）
    p.fd = fd;
    p.connecting = true;
    epoll_event ev{};
    ev.events = EPOLLOUT | EPOLLIN | EPOLLERR | EPOLLHUP;
    ev.data.fd = fd;
    ::epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev);
  }

  void updateInterestLocked(Peer& p) {
    if (p.fd < 0) return;
    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLERR | EPOLLHUP;
    if (!p.out.empty()) ev.events |= EPOLLOUT;
    ev.data.fd = p.fd;
    ::epoll_ctl(epfd, EPOLL_CTL_MOD, p.fd, &ev);
  }

  // 把 out 缓冲尽量写出去；写完则发起下一个待发帧。
  void flushLocked(Peer& p) {
    if (p.fd < 0) return;
    if (p.connecting) {
      int err = 0;
      socklen_t len = sizeof(err);
      if (::getsockopt(p.fd, SOL_SOCKET, SO_ERROR, &err, &len) != 0 || err != 0) {
        closePeerLocked(p, /*dropHead=*/true);
        return;
      }
      p.connecting = false;
    }
    if (!p.awaitingReply && p.out.empty() && !p.queue.empty()) {
      p.out = p.queue.front().frame;  // 一次只发队首（请求-应答配对）
    }
    size_t written = 0;
    if (!p.out.empty()) {
      if (!writeAllNonBlocking(p.fd, p.out.data(), p.out.size(), written)) {
        closePeerLocked(p, /*dropHead=*/true);
        return;
      }
      p.out.erase(p.out.begin(), p.out.begin() + static_cast<ptrdiff_t>(written));
      if (p.out.empty()) p.awaitingReply = true;  // 整帧已上线：只等应答，绝不重发
    }
    updateInterestLocked(p);
  }

  // 处理可读：累积字节 -> 尝试解析一整帧 -> 触发队首回调 -> 发起下一个请求。
  void onReadableLocked(Peer& p) {
    if (p.fd < 0) return;
    Byte buf[8192];
    for (;;) {
      const ssize_t r = ::recv(p.fd, buf, sizeof(buf), 0);
      if (r > 0) {
        p.in.insert(p.in.end(), buf, buf + r);
        continue;
      }
      if (r == 0) {  // 对端关闭
        closePeerLocked(p, /*dropHead=*/true);
        return;
      }
      if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) break;
      closePeerLocked(p, /*dropHead=*/true);
      return;
    }
    for (;;) {
      if (p.in.size() < 4) break;
      const uint32_t len = (static_cast<uint32_t>(p.in[0]) << 24) |
                           (static_cast<uint32_t>(p.in[1]) << 16) |
                           (static_cast<uint32_t>(p.in[2]) << 8) |
                           static_cast<uint32_t>(p.in[3]);
      if (len == 0 || len > 64u * 1024u * 1024u) {  // 畸形长度：断开
        closePeerLocked(p, /*dropHead=*/true);
        return;
      }
      const size_t total = 4 + len;
      if (p.in.size() < total) break;  // 半帧：继续等
      Bytes frame(p.in.begin(), p.in.begin() + static_cast<ptrdiff_t>(total));
      p.in.erase(p.in.begin(), p.in.begin() + static_cast<ptrdiff_t>(total));

      if (p.queue.empty()) continue;  // 没有在途请求：丢弃（不应发生）
      Pending pending = std::move(p.queue.front());
      p.queue.pop_front();
      p.awaitingReply = false;  // 队首已应答：下一帧可以发了
      if (pending.cb) pending.cb(frame, true);
      // 收到应答后立刻尝试发下一个。**必须复用 flushLocked**：这里原来自己写一遍
      // 内联发送逻辑，不会置 awaitingReply，于是下一次事件循环扫描又把这个新队首
      // 重发一遍（与上面修掉的是同一个缺陷的另一条出口）。
      if (!p.queue.empty()) flushLocked(p);
      updateInterestLocked(p);
    }
  }

  void run() {
    std::vector<epoll_event> events(kMaxEvents);
    while (running.load(std::memory_order_relaxed)) {
      const int n = ::epoll_wait(epfd, events.data(), kMaxEvents, kLoopWaitMs);
      if (n < 0 && errno != EINTR) break;
      for (int i = 0; i < n; ++i) {
        const int fd = events[i].data.fd;
        if (fd == wakeFd) {
          uint64_t v = 0;
          ssize_t r = ::read(wakeFd, &v, sizeof(v));
          (void)r;
          continue;
        }
        std::lock_guard<std::mutex> lk(mu);
        for (auto& kv : peers) {
          Peer& p = *kv.second;
          if (p.fd != fd) continue;
          const uint32_t ev = events[i].events;
          if (ev & (EPOLLERR | EPOLLHUP)) {
            closePeerLocked(p, /*dropHead=*/true);
            break;
          }
          if (ev & EPOLLOUT) flushLocked(p);
          if (p.fd == fd && (ev & EPOLLIN)) onReadableLocked(p);
          break;
        }
      }
      // 超时扫描（peers 数量很小，直接线性扫）+ 重新发起连接/发送
      std::lock_guard<std::mutex> lk(mu);
      const uint64_t now = nowMs();
      for (auto& kv : peers) {
        Peer& p = *kv.second;
        if (!p.queue.empty() && now >= p.queue.front().deadlineMs) {
          closePeerLocked(p, /*dropHead=*/true);  // 超时：回调不触发
        }
        if (!p.queue.empty() && p.fd < 0) ensureConnectingLocked(p);
        if (p.fd >= 0 && (!p.out.empty() || !p.queue.empty())) flushLocked(p);
      }
    }
  }
};

Reactor::Reactor() : impl_(new Impl()) {}

Reactor::~Reactor() { stop(); }

void Reactor::start() {
  if (impl_->running.exchange(true)) return;
  impl_->epfd = ::epoll_create1(0);
  impl_->wakeFd = ::eventfd(0, EFD_NONBLOCK);
  if (impl_->epfd < 0 || impl_->wakeFd < 0) {
    impl_->running.store(false);
    return;
  }
  epoll_event ev{};
  ev.events = EPOLLIN;
  ev.data.fd = impl_->wakeFd;
  ::epoll_ctl(impl_->epfd, EPOLL_CTL_ADD, impl_->wakeFd, &ev);
  impl_->loop = std::thread([this] { impl_->run(); });
}

void Reactor::stop() {
  if (!impl_->running.exchange(false)) {
    if (impl_->loop.joinable()) impl_->loop.join();
    return;
  }
  impl_->wake();
  if (impl_->loop.joinable()) impl_->loop.join();
  std::lock_guard<std::mutex> lk(impl_->mu);
  for (auto& kv : impl_->peers) {
    Impl::Peer& p = *kv.second;
    if (p.fd >= 0) ::close(p.fd);
    p.fd = -1;
    p.connecting = false;
    p.queue.clear();  // 停止后不再触发任何回调（L15）
  }
  if (impl_->wakeFd >= 0) {
    ::close(impl_->wakeFd);
    impl_->wakeFd = -1;
  }
  if (impl_->epfd >= 0) {
    ::close(impl_->epfd);
    impl_->epfd = -1;
  }
}

void Reactor::addPeer(int id, const std::string& hostPort) {
  std::string host;
  uint16_t port = 0;
  if (!parseHostPort(hostPort, host, port)) return;
  std::lock_guard<std::mutex> lk(impl_->mu);
  auto it = impl_->peers.find(id);
  if (it != impl_->peers.end()) {
    if (it->second->host == host && it->second->port == port) return;
    Impl::Peer& p = *it->second;
    if (p.fd >= 0) {
      ::epoll_ctl(impl_->epfd, EPOLL_CTL_DEL, p.fd, nullptr);
      ::close(p.fd);
      p.fd = -1;
    }
    p.host = host;
    p.port = port;
    p.connecting = false;
    return;
  }
  auto p = std::make_unique<Impl::Peer>();
  p->id = id;
  p->host = host;
  p->port = port;
  impl_->peers[id] = std::move(p);
  impl_->wake();
}

void Reactor::removePeer(int id) {
  std::lock_guard<std::mutex> lk(impl_->mu);
  auto it = impl_->peers.find(id);
  if (it == impl_->peers.end()) return;
  Impl::Peer& p = *it->second;
  if (p.fd >= 0) {
    ::epoll_ctl(impl_->epfd, EPOLL_CTL_DEL, p.fd, nullptr);
    ::close(p.fd);
    p.fd = -1;
  }
  p.queue.clear();  // 在途回调一律不触发（A6）
  p.out.clear();
  p.in.clear();
  impl_->peers.erase(it);
}

bool Reactor::send(int peerId, const Bytes& frame, uint64_t timeoutMs,
                   Completion cb) {
  std::lock_guard<std::mutex> lk(impl_->mu);
  auto it = impl_->peers.find(peerId);
  if (it == impl_->peers.end()) return false;
  Impl::Peer& p = *it->second;
  Impl::Pending pending;
  pending.frame = frame;
  pending.cb = std::move(cb);
  pending.deadlineMs = Impl::nowMs() + (timeoutMs == 0 ? 100 : timeoutMs);
  p.queue.push_back(std::move(pending));
  impl_->wake();
  return true;
}

size_t Reactor::inflight() const {
  std::lock_guard<std::mutex> lk(impl_->mu);
  size_t n = 0;
  for (const auto& kv : impl_->peers) n += kv.second->inflight();
  return n;
}

}  // namespace raftkv::raft
