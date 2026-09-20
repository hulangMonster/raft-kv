// raftkv raft node process: RaftNode + FileLogStore + KvStateMachine +
// TcpTransport + SteadyClock, serving both node RPC and the cluster client
// protocol over one TCP port.
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <algorithm>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "kv/kv_state_machine.h"
#include "raft/clock.h"
#include "raft/lock_probe.h"
#include "raft/log_store.h"
#include "raft/metrics.h"
#include "raft/message.h"
#include "raft/raft_node.h"
#include "raft/snapshot_store.h"
#include "raft/transport_reactor.h"
#include "raft/transport_tcp.h"

using namespace raftkv;
using namespace raftkv::raft;

namespace {

std::atomic<bool> g_running{true};
std::atomic<int> g_connClientId{1};
// M5.2 实测：故障窗口内客户端重试洪泛会让 thread-per-connection 无限起线程
// （实测单节点 1021 个线程，几乎全在等锁）-> ticker 被饿死 -> 无法选主。
// 在 Reactor 落地（M5.3）之前先加硬上限：超限直接关连接，绝不让线程数失控。
std::atomic<int> g_activeConns{0};
constexpr int kMaxConns = 256;

void onSignal(int /*sig*/) { g_running = false; }

bool readFull(int fd, void* buf, size_t len) {
  auto* p = static_cast<Byte*>(buf);
  size_t got = 0;
  while (got < len) {
    ssize_t r = ::recv(fd, p + got, len - got, 0);
    if (r < 0) {
      if (errno == EINTR) continue;
      return false;  // includes read timeout
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

bool readFrame(int fd, MsgType& type, Bytes& payload) {
  Byte lenBuf[4];
  if (!readFull(fd, lenBuf, sizeof(lenBuf))) return false;
  const uint32_t len = getU32(lenBuf);
  if (len < 1 || len > 64u * 1024u * 1024u) return false;
  Bytes body(len);
  if (!readFull(fd, body.data(), body.size())) return false;
  Bytes whole;
  whole.reserve(4 + len);
  whole.insert(whole.end(), lenBuf, lenBuf + 4);
  whole.insert(whole.end(), body.begin(), body.end());
  return decodeFrame(whole.data(), whole.size(), type, payload);
}

std::string roleName(Role r) {
  switch (r) {
    case Role::kLeader: return "leader";
    case Role::kCandidate: return "candidate";
    case Role::kFollower: return "follower";
  }
  return "unknown";
}

// 评审 B7：单条连接的请求处理循环抽出来，异常只允许影响这条连接。
void serveConnection(int fd, RaftNode& node, Metrics& metrics,
                     int connClientId, uint64_t& connRequestId) {
  MsgType type;
  Bytes payload;
  while (g_running && readFrame(fd, type, payload)) {
    if (type == MsgType::kRequestVote) {
      RequestVoteArgs args;
      if (decodeRequestVote(payload.data(), payload.size(), args)) {
        const RequestVoteReply reply = node.onRequestVote(args);
        const Bytes f = encodeFrame(MsgType::kRequestVoteReply,
                                    encodeRequestVoteReply(reply));
        if (!writeFull(fd, f.data(), f.size())) break;
      }
    } else if (type == MsgType::kAppendEntries) {
      AppendEntriesArgs args;
      if (decodeAppendEntries(payload.data(), payload.size(), args)) {
        const AppendEntriesReply reply = node.onAppendEntries(args);
        const Bytes f = encodeFrame(MsgType::kAppendEntriesReply,
                                    encodeAppendEntriesReply(reply));
        if (!writeFull(fd, f.data(), f.size())) break;
      }
    } else if (type == MsgType::kInstallSnapshot) {
      InstallSnapshotArgs args;
      if (decodeInstallSnapshot(payload.data(), payload.size(), args)) {
        const InstallSnapshotReply reply = node.onInstallSnapshot(args);
        const Bytes f = encodeFrame(MsgType::kInstallSnapshotReply,
                                    encodeInstallSnapshotReply(reply));
        if (!writeFull(fd, f.data(), f.size())) break;
      }
    } else if (type == MsgType::kClientRequest) {
      ClientRequest req;
      if (decodeClientRequest(payload.data(), payload.size(), req)) {
        if (req.clientId == 0) {  // synthesize per-connection ids if absent
          req.clientId = static_cast<uint64_t>(connClientId);
          req.requestId = ++connRequestId;
        }
        const ClientReply reply = node.propose(req, 1000);
        const Bytes f = encodeFrame(MsgType::kClientReply,
                                    encodeClientReply(reply));
        if (!writeFull(fd, f.data(), f.size())) break;
      }
    } else if (type == MsgType::kStatusRequest) {
      std::ostringstream ss;
      ss << "role=" << roleName(node.role()) << " term=" << node.currentTerm()
         << " leader_id=" << node.leaderId()
         << " commit_index=" << node.commitIndex()
         << " last_applied=" << node.lastApplied()
         << " snapshot_index=" << node.lastIncludedIndex()
         << " snapshot_term=" << node.lastIncludedTerm()
         // M4.4: 拓扑与读路径观测
         << " config_version=" << node.configVersion()
         << " retired=" << (node.retired() ? "true" : "false")
         << " read_index=" << node.commitIndex();
      const ClusterConfig cfgNow = node.clusterConfig();
      std::ostringstream members;
      for (size_t i = 0; i < cfgNow.members.size(); ++i) {
        if (i != 0) members << ",";
        members << cfgNow.members[i].id << ":" << cfgNow.members[i].addr << ":"
                << (cfgNow.members[i].voting ? "v" : "n");
      }
      ss << " members=" << members.str();
      ss << " " << metrics.statusFragment();  // M5.1: 指标（只读）
      ClientReply r;
      r.status = ClientStatus::kOk;
      r.value = ss.str();
      r.leaderHint = node.leaderId();
      const Bytes f = encodeFrame(MsgType::kClientReply, encodeClientReply(r));
      if (!writeFull(fd, f.data(), f.size())) break;
    } else if (type == MsgType::kSnapshotTrigger) {
      node.triggerSnapshot();
      ClientReply r;
      r.status = ClientStatus::kOk;
      r.value = "snapshot triggered";
      r.leaderHint = -1;
      const Bytes f = encodeFrame(MsgType::kClientReply, encodeClientReply(r));
      if (!writeFull(fd, f.data(), f.size())) break;
    } else if (type == MsgType::kConfigRequest) {
      ConfigRequestArgs args;
      if (decodeConfigRequest(payload.data(), payload.size(), args)) {
        ConfigReplyArgs reply;
        reply.term = node.currentTerm();
        reply.leaderHint = node.leaderId();
        reply.config = node.clusterConfig();
        if (args.action == 0) {
          reply.ok = true;  // get：任何节点都可回答自己的配置视图
        } else if (args.action == 1 || args.action == 2) {
          // 评审 O8：未知 action 绝不能落进 remove 分支（decode 已拒绝，双保险）
          const MembershipOp op = (args.action == 1) ? MembershipOp::kAdd
                                                     : MembershipOp::kRemove;
          const ClientReply cr =
              node.changeMembership(op, args.targetId, args.addr, 2000);
          reply.ok = (cr.status == ClientStatus::kOk);
          reply.leaderHint = cr.leaderHint;
          reply.config = node.clusterConfig();
        } else {
          reply.ok = false;  // 未知 action
        }
        const Bytes f =
            encodeFrame(MsgType::kConfigReply, encodeConfigReply(reply));
        if (!writeFull(fd, f.data(), f.size())) break;
      }
    } else if (type == MsgType::kMetricsRequest) {
      // M5.1：可选指标文本端点（Prometheus 风格）
      ClientReply r;
      r.status = ClientStatus::kOk;
      r.value = metrics.prometheusText();
      r.leaderHint = node.leaderId();
      const Bytes f = encodeFrame(MsgType::kClientReply, encodeClientReply(r));
      if (!writeFull(fd, f.data(), f.size())) break;
    } else if (type == MsgType::kReadProbe) {
      ReadProbeArgs args;
      if (decodeReadProbe(payload.data(), payload.size(), args)) {
        const ReadProbeReply reply = node.onReadProbe(args);
        const Bytes f = encodeFrame(MsgType::kReadProbeReply,
                                    encodeReadProbeReply(reply));
        if (!writeFull(fd, f.data(), f.size())) break;
      }
    } else {
      break;  // unknown frame type
    }
  }
}

void handleConnection(int fd, RaftNode& node, Metrics& metrics) {
  const int connClientId = g_connClientId.fetch_add(1);
  uint64_t connRequestId = 0;
  // 评审 B7：解码/处理抛出的异常（如恶意帧触发的 bad_alloc）绝不能 terminate
  // 整个节点进程——只关闭这一条连接。
  try {
    serveConnection(fd, node, metrics, connClientId, connRequestId);
  } catch (const std::exception& e) {
    std::cerr << "[raftkv-node] conn dropped after error: " << e.what() << "\n";
  } catch (...) {
    std::cerr << "[raftkv-node] conn dropped after unknown error\n";
  }
  ::close(fd);
}

std::unordered_map<int, std::string> parsePeers(const std::string& s) {
  std::unordered_map<int, std::string> peers;
  size_t start = 0;
  while (start <= s.size()) {
    const size_t comma = s.find(',', start);
    const std::string item =
        s.substr(start, comma == std::string::npos ? std::string::npos
                                                   : comma - start);
    const size_t eq = item.find('=');
    if (eq != std::string::npos && !item.empty()) {
      peers[std::stoi(item.substr(0, eq))] = item.substr(eq + 1);
    }
    if (comma == std::string::npos) break;
    start = comma + 1;
  }
  return peers;
}

void usage(const char* argv0) {
  std::cerr << "usage: " << argv0
           << " [--group-linger-us N] [--inflight-per-peer N]"
            << " --id N --port P --peers \"1=host:port,...\""
            << " [--data-dir DIR]\n";
}

}  // namespace

int main(int argc, char** argv) {
  std::signal(SIGPIPE, SIG_IGN);

  int id = 1;
  int port = 19601;
  size_t snapshotThreshold = 10000;
  uint64_t groupLingerUs = 0;  // M5.4: --group-linger-us（0 = 不蓄批）
  // M5.6 收尾：**默认 1（= M5.3 的单批在途保守行为）**。窗口=4 能显著改善 p=1 延迟
  // （22.5 -> 13.4 ms/写），但 M5.6 的深度复测证明：窗口放大了异步引擎的 ack 归因风险，
  // 使"同一 index 出现两个都被提交的条目"这类分歧更容易出现（详见 docs/m5-design.md v2.2 的证据链）。
  // reactor 引擎目前是**可选**且不推荐开窗，故默认 1；需要复现该性能实验时显式传
  // `--inflight-per-peer 4`。
  size_t inflightPerPeer = 1;  // M5.6: --inflight-per-peer
  std::string peersArg;
  std::string dataDir;
  bool lockWaitMetrics = false;  // M5.1: 打开锁等待计时（默认关，零开销）
  // 引擎默认 = **sync**。理由随版本演过三轮，**当前的真实理由是性能**（P2a 后同轮交替实测，
  // 见 docs/m5-bench.md §3.11）：
  //   * p=64：sync 2317 qps vs reactor 1676（sync 快 1.38×）
  //   * p=8 ：打平（514 vs 511 qps）
  //   * p=1 ：reactor 更快（1.17×）——只要低并发延迟时显式选它
  // 历史上两条"阻断项"都已关闭，不再构成理由：
  //   * M5.3：reactor 下节点静默消失/SIGSEGV（`snapshotOpMu_` 串行化 compact + 回调异常重抛，已修）
  //   * M5.6：reactor `win=1` 偶发丢写（应答归因错配 + awaitCommit 把"已提交"误判为"本次已提交"，
  //           已修，并有 R6 / M5.A12 守门）
  // reactor 作为可选引擎保留：--transport=reactor / RAFTKV_TRANSPORT=reactor
  bool useReactor = false;

  for (int i = 1; i < argc; ++i) {
    const std::string a = argv[i];
    auto next = [&](const char* what) -> std::string {
      if (i + 1 >= argc) {
        std::cerr << "missing value for " << what << "\n";
        std::exit(2);
      }
      return argv[++i];
    };
    if (a == "--id") {
      id = std::stoi(next("--id"));
    } else if (a == "--port") {
      port = std::stoi(next("--port"));
    } else if (a == "--peers") {
      peersArg = next("--peers");
    } else if (a == "--data-dir") {
      dataDir = next("--data-dir");
    } else if (a.rfind("--transport=", 0) == 0) {
      const std::string t = a.substr(std::string("--transport=").size());
      if (t == "reactor") {
        useReactor = true;
      } else if (t == "sync") {
        useReactor = false;
      } else {
        std::cerr << "unknown --transport: " << t << std::endl;
        return 2;
      }
    } else if (a == "--transport") {
      const std::string t = next("--transport");
      if (t == "reactor") {
        useReactor = true;
      } else if (t == "sync") {
        useReactor = false;
      } else {
        std::cerr << "unknown --transport: " << t << std::endl;
        return 2;
      }
    } else if (a == "--lock-wait-metrics") {
      lockWaitMetrics = true;
    } else if (a == "--snapshot-threshold") {
      snapshotThreshold = std::stoul(next("--snapshot-threshold"));
    } else if (a == "--inflight-per-peer") {
      // M5.6（§8.2 滑动窗口）：异步引擎下每 peer 允许同时在途的 AppendEntries 批数。
      inflightPerPeer = std::stoul(next("--inflight-per-peer"));
    } else if (a == "--group-linger-us") {
      // M5.4（决策④ 批处理调优）：组提交蓄批窗口，用于 A/B 实测取值。
      groupLingerUs = std::stoull(next("--group-linger-us"));
    } else {
      usage(argv[0]);
      return 2;
    }
  }
  if (dataDir.empty()) dataDir = "./raft-data-" + std::to_string(id);
  if (peersArg.empty()) {
    std::cerr << "--peers is required\n";
    usage(argv[0]);
    return 2;
  }

  auto peers = parsePeers(peersArg);
  std::vector<int> peerIds;
  std::unordered_map<int, std::string> transportPeers;
  for (const auto& [pid, addr] : peers) {
    if (pid != id) {
      peerIds.push_back(pid);
      transportPeers[pid] = addr;
    }
  }

  try {
    RaftConfig cfg;
    cfg.selfId = id;
    cfg.peerIds = std::move(peerIds);
    cfg.snapshotThresholdEntries = snapshotThreshold;
    cfg.groupCommitLingerUs = groupLingerUs;  // M5.4：0 = 不蓄批
    cfg.maxInflightPerPeer = inflightPerPeer;  // M5.6：滑动窗口宽度（同步引擎不使用）

    Metrics metrics;  // M5.1: 进程内指标（只读；不参与任何判定）
    if (lockWaitMetrics) lockprobe::setTimingEnabled(true);
    FileLogStore log(dataDir);
    FileSnapshotStore snapshots(dataDir);  // M3.4: durable snapshots
    KvStateMachine sm;
    SteadyClock clock;
    // M5.3：transport 引擎可切换（--transport=reactor|sync 或 RAFKV_TRANSPORT 环境变量）。
    //   sync     = 旧的同步阻塞实现（每 RPC 建连 + 全局锁），作为对照与回退路径
    //   reactor  = epoll 事件循环 + 每 peer 长连接；sendX 入队即返回，回调在 reactor 线程
    if (const char* envT = ::getenv("RAFTKV_TRANSPORT")) {
      if (std::strcmp(envT, "reactor") == 0) useReactor = true;
      else if (std::strcmp(envT, "sync") == 0) useReactor = false;
    }
    std::unique_ptr<TransportReactor> reactorTransport;
    std::unique_ptr<TcpTransport> syncTransport;
    if (useReactor) {
      reactorTransport = std::make_unique<TransportReactor>(transportPeers, 30);
    } else {
      syncTransport = std::make_unique<TcpTransport>(transportPeers, 30);
    }
    Transport& transport = useReactor ? static_cast<Transport&>(*reactorTransport)
                                      : static_cast<Transport&>(*syncTransport);
    std::cerr << "[raftkv-node] transport=" << (useReactor ? "reactor" : "sync") << std::endl;
    // M4: --peers 是启动种子配置（version=0，含地址）；运行期由日志/快照推进
    ClusterConfig seed;
    seed.version = 0;
    for (const auto& [pid, addr] : peers) {
      Member m;
      m.id = pid;
      m.addr = addr;
      m.voting = true;
      seed.members.push_back(m);
    }
    // 注意：self 不在 --peers 中 = 这是一个“动态加入”的节点（seed 里没有自己）：
    // 它必须以非投票/退役态启动，只接收复制，等 Leader 用 add 把它写入配置
    // （m4-design v1.2 §5.1-5 / §5.4）。绝不能自动把自己塞进 seed，否则它会
    // 以为自己已是成员而不断竞选，把现有 Leader 逼下台。
    std::sort(seed.members.begin(), seed.members.end(),
              [](const Member& a, const Member& b) { return a.id < b.id; });
    RaftNode node(cfg, log, sm, transport, clock, &snapshots, seed, &metrics);
    // M5.1：把 Reactor 的在途请求数接进指标（status/metrics 渲染时读取）
    node.setInflightMetricsProvider([&reactorTransport]() -> size_t {
      return reactorTransport ? reactorTransport->inflight() : 0;
    });

    struct sigaction sa {};
    sa.sa_handler = onSignal;
    sigemptyset(&sa.sa_mask);
    ::sigaction(SIGINT, &sa, nullptr);
    ::sigaction(SIGTERM, &sa, nullptr);

    std::thread ticker([&] {
      while (g_running) {
        node.tick();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
    });
    // M5.6 修复（实测）：ticker 是 joinable 的，而下面 socket/bind/listen 失败都会
    // 直接 `return 1`——那会在析构 ticker 时触发 std::terminate（日志里表现为
    // "terminate called without an active exception"，节点无诊断地消失）。
    // 用 RAII 守卫保证**所有**退出路径（含异常展开）都先停 ticker 再 join。
    bool tickerJoined = false;
    struct TickerStop {
      std::thread& t;
      bool& done;
      ~TickerStop() {
        if (done) return;
        g_running = false;
        if (t.joinable()) t.join();
        done = true;
      }
    } tickerStop{ticker, tickerJoined};

    const int lsock = ::socket(AF_INET, SOCK_STREAM, 0);
    if (lsock < 0) {
      std::cerr << "socket failed: " << std::strerror(errno) << "\n";
      return 1;
    }
    const int one = 1;
    ::setsockopt(lsock, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (::bind(lsock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
      std::cerr << "bind :" << port << " failed: " << std::strerror(errno) << "\n";
      ::close(lsock);
      return 1;
    }
    if (::listen(lsock, 128) != 0) {
      std::cerr << "listen failed: " << std::strerror(errno) << "\n";
      ::close(lsock);
      return 1;
    }
    std::cerr << "[raftkv-node] id=" << id << " listening on 0.0.0.0:" << port
              << " data-dir=" << dataDir << "\n";

    while (g_running) {
      const int c = ::accept(lsock, nullptr, nullptr);
      if (c < 0) {
        if (errno == EINTR) continue;
        if (!g_running) break;
        continue;
      }
      timeval tv{};
      tv.tv_sec = 0;
      tv.tv_usec = 500000;  // idle connections wake every 0.5s to check stop
      ::setsockopt(c, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
      if (g_activeConns.load() >= kMaxConns) {
        ::close(c);  // 保护：拒绝而不是无限起线程
        continue;
      }
      g_activeConns.fetch_add(1);
      std::thread([c, &node, &metrics] {
        handleConnection(c, node, metrics);
        g_activeConns.fetch_sub(1);
      }).detach();
    }

    ::close(lsock);
    g_running = false;
    if (ticker.joinable()) ticker.join();
    tickerJoined = true;  // 已显式 join，守卫不再重复
    // M5.3（L15）：显式停止 reactor（停事件循环 + join + 丢弃在途回调），避免
    // detached 连接线程/回调触及即将失效的对象。
    if (reactorTransport) reactorTransport->stop();
    std::cerr << "[raftkv-node] id=" << id << " shutdown\n";
  } catch (const std::exception& e) {
    std::cerr << "fatal: " << e.what() << "\n";
    return 1;
  }

  // Avoid running stack-object destructors while detached connection threads
  // may still reference `node`; the OS reclaims everything on exit.
  std::_Exit(0);
}
