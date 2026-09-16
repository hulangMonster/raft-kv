// M5.3 契约用例：Reactor 的异步投递语义（对真实 loopback socket，不起 RaftNode）。
//
// 编号对应 docs/m5-design.md §11（M5.A5/A6 的 Reactor 侧实现）：
//   R1 基本收发：send() 入队即返回，应答在 reactor 线程回调
//   R2 超时：对端不回复 -> 回调不触发，且 in-flight 归零（与旧同步实现"超时即失败"一致）
//   R3 removePeer：丢弃该 peer 的在途回调（悬垂回调断言，A6）
//   R4 慢 peer 隔离：一个不回复的 peer 不得阻塞另一个 peer 的应答（决策③ 的核心收益）
//   R5 stop()：停止后不再触发任何回调（L15）

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "raft/reactor.h"

using namespace raftkv;
using namespace raftkv::raft;

namespace {

// 极简 TCP 服务器：读一整帧（4 字节长度前缀 + 负载），echo=false 时只读不回。
class FrameServer {
 public:
  explicit FrameServer(bool echo) : echo_(echo) {}
  ~FrameServer() { stop(); }

  bool start() {
    lfd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (lfd_ < 0) return false;
    int one = 1;
    ::setsockopt(lfd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;  // 让内核选端口
    if (::bind(lfd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) return false;
    socklen_t len = sizeof(addr);
    if (::getsockname(lfd_, reinterpret_cast<sockaddr*>(&addr), &len) != 0) return false;
    port_ = ntohs(addr.sin_port);
    if (::listen(lfd_, 16) != 0) return false;
    run_ = true;
    th_ = std::thread([this] { loop(); });
    return true;
  }

  void stop() {
    if (!run_.exchange(false)) return;
    if (lfd_ >= 0) { ::shutdown(lfd_, SHUT_RDWR); ::close(lfd_); lfd_ = -1; }
    if (th_.joinable()) th_.join();
    std::lock_guard<std::mutex> lk(mu_);
    for (int fd : conns_) ::close(fd);
    conns_.clear();
  }

  uint16_t port() const { return port_; }
  std::string addr() const { return "127.0.0.1:" + std::to_string(port_); }
  int accepted() const { return accepted_.load(); }

 private:
  void loop() {
    while (run_.load()) {
      const int c = ::accept(lfd_, nullptr, nullptr);
      if (c < 0) break;
      accepted_.fetch_add(1);
      {
        std::lock_guard<std::mutex> lk(mu_);
        conns_.push_back(c);
      }
      if (!echo_) continue;  // 收下连接但永不回复
      std::thread([this, c] {
        for (;;) {
          Byte hdr[4];
          if (!readFull(c, hdr, 4)) return;
          const uint32_t len = (static_cast<uint32_t>(hdr[0]) << 24) |
                               (static_cast<uint32_t>(hdr[1]) << 16) |
                               (static_cast<uint32_t>(hdr[2]) << 8) |
                               static_cast<uint32_t>(hdr[3]);
          std::vector<Byte> body(len);
          if (len > 0 && !readFull(c, body.data(), len)) return;
          std::vector<Byte> whole;
          whole.insert(whole.end(), hdr, hdr + 4);
          whole.insert(whole.end(), body.begin(), body.end());
          if (!writeFull(c, whole.data(), whole.size())) return;
        }
      }).detach();
    }
  }

  static bool readFull(int fd, void* buf, size_t len) {
    auto* p = static_cast<Byte*>(buf);
    size_t got = 0;
    while (got < len) {
      const ssize_t r = ::recv(fd, p + got, len - got, 0);
      if (r <= 0) return false;
      got += static_cast<size_t>(r);
    }
    return true;
  }
  static bool writeFull(int fd, const void* buf, size_t len) {
    auto* p = static_cast<const Byte*>(buf);
    size_t sent = 0;
    while (sent < len) {
      const ssize_t r = ::send(fd, p + sent, len - sent, MSG_NOSIGNAL);
      if (r <= 0) return false;
      sent += static_cast<size_t>(r);
    }
    return true;
  }

  int lfd_ = -1;
  uint16_t port_ = 0;
  std::atomic<bool> run_{false};
  std::atomic<int> accepted_{0};
  bool echo_ = true;
  std::thread th_;
  mutable std::mutex mu_;
  std::vector<int> conns_;
};

// 一个最小合法帧：[len:4]=1 [type:1]=0xAA
Bytes tinyFrame() { return Bytes{0, 0, 0, 1, 0xAA}; }

bool waitFor(const std::atomic<int>& flag, int timeoutMs) {
  for (int i = 0; i < timeoutMs / 5; ++i) {
    if (flag.load()) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  return flag.load();
}

}  // namespace

// R1：入队即返回 + 应答经 reactor 线程回调
TEST(RaftReactor, R1_SendReceivesReplyAsynchronously) {
  FrameServer server(/*echo=*/true);
  ASSERT_TRUE(server.start());
  Reactor reactor;
  reactor.start();
  reactor.addPeer(1, server.addr());

  std::atomic<int> got{0};
  Bytes gotFrame;
  const auto t0 = std::chrono::steady_clock::now();
  const bool queued = reactor.send(1, tinyFrame(), 1000,
                                   [&](const Bytes& frame, bool ok) {
                                     if (ok) {
                                       gotFrame = frame;
                                       got.store(1);
                                     }
                                   });
  const auto enqueueUs = std::chrono::duration_cast<std::chrono::microseconds>(
                             std::chrono::steady_clock::now() - t0)
                             .count();
  EXPECT_TRUE(queued);
  EXPECT_LT(enqueueUs, 20000) << "send() 必须是入队即返回（不阻塞一个 RPC 往返）";
  EXPECT_TRUE(waitFor(got, 2000));
  EXPECT_EQ(gotFrame, tinyFrame());
  EXPECT_EQ(reactor.inflight(), 0u);
  reactor.stop();
  server.stop();
}

// R2：对端不回复 -> 回调不触发（超时语义），in-flight 归零
TEST(RaftReactor, R2_TimeoutDropsCallback) {
  FrameServer server(/*echo=*/false);
  ASSERT_TRUE(server.start());
  Reactor reactor;
  reactor.start();
  reactor.addPeer(1, server.addr());

  std::atomic<int> called{0};
  EXPECT_TRUE(reactor.send(1, tinyFrame(), 120, [&](const Bytes&, bool ok) {
    if (ok) called.fetch_add(1);
  }));
  std::this_thread::sleep_for(std::chrono::milliseconds(600));
  EXPECT_EQ(called.load(), 0) << "超时后不得触发回调（与旧同步实现一致）";
  EXPECT_EQ(reactor.inflight(), 0u) << "超时请求必须被清理，不能泄漏";
  reactor.stop();
  server.stop();
}

// R3（=A6）：removePeer 丢弃在途回调，绝不悬垂
TEST(RaftReactor, R3_RemovePeerDropsInflightCallback) {
  FrameServer server(/*echo=*/false);
  ASSERT_TRUE(server.start());
  Reactor reactor;
  reactor.start();
  reactor.addPeer(7, server.addr());

  std::atomic<int> called{0};
  EXPECT_TRUE(reactor.send(7, tinyFrame(), 30000, [&](const Bytes&, bool ok) {
    if (ok) called.fetch_add(1);
  }));
  EXPECT_EQ(reactor.inflight(), 1u);
  reactor.removePeer(7);
  EXPECT_EQ(reactor.inflight(), 0u);
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  EXPECT_EQ(called.load(), 0) << "被移除 peer 的在途回调不得触发";
  reactor.stop();
  server.stop();
}

// R4：一个不回复的 peer 不得阻塞其它 peer（决策③ 的核心收益）
TEST(RaftReactor, R4_SlowPeerDoesNotBlockOthers) {
  FrameServer slow(/*echo=*/false);
  FrameServer fast(/*echo=*/true);
  ASSERT_TRUE(slow.start());
  ASSERT_TRUE(fast.start());
  Reactor reactor;
  reactor.start();
  reactor.addPeer(1, slow.addr());  // 慢：30s 超时
  reactor.addPeer(2, fast.addr());  // 快

  std::atomic<int> fastDone{0};
  std::atomic<int> slowDone{0};
  EXPECT_TRUE(reactor.send(1, tinyFrame(), 30000, [&](const Bytes&, bool) {
    slowDone.fetch_add(1);
  }));
  const auto t0 = std::chrono::steady_clock::now();
  EXPECT_TRUE(reactor.send(2, tinyFrame(), 30000, [&](const Bytes& frame, bool ok) {
    if (ok && frame == tinyFrame()) fastDone.store(1);
  }));
  EXPECT_TRUE(waitFor(fastDone, 2000)) << "慢 peer 阻塞了快 peer";
  const auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::steady_clock::now() - t0)
                             .count();
  EXPECT_LT(elapsedMs, 1000);
  EXPECT_EQ(slowDone.load(), 0);
  reactor.stop();
  slow.stop();
  fast.stop();
}

// R5（L15）：stop() 之后不再触发任何回调
TEST(RaftReactor, R5_StopDropsPendingCallbacks) {
  FrameServer server(/*echo=*/false);
  ASSERT_TRUE(server.start());
  Reactor reactor;
  reactor.start();
  reactor.addPeer(1, server.addr());

  std::atomic<int> called{0};
  EXPECT_TRUE(reactor.send(1, tinyFrame(), 30000, [&](const Bytes&, bool ok) {
    if (ok) called.fetch_add(1);
  }));
  reactor.stop();
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  EXPECT_EQ(called.load(), 0);
  server.stop();
}
