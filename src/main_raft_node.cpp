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
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "kv/kv_state_machine.h"
#include "raft/clock.h"
#include "raft/log_store.h"
#include "raft/message.h"
#include "raft/raft_node.h"
#include "raft/transport_tcp.h"

using namespace raftkv;
using namespace raftkv::raft;

namespace {

std::atomic<bool> g_running{true};
std::atomic<int> g_connClientId{1};

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

void handleConnection(int fd, RaftNode& node) {
  const int connClientId = g_connClientId.fetch_add(1);
  uint64_t connRequestId = 0;

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
         << " last_applied=" << node.lastApplied();
      ClientReply r;
      r.status = ClientStatus::kOk;
      r.value = ss.str();
      r.leaderHint = node.leaderId();
      const Bytes f = encodeFrame(MsgType::kClientReply, encodeClientReply(r));
      if (!writeFull(fd, f.data(), f.size())) break;
    } else {
      break;  // unknown frame type
    }
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
            << " --id N --port P --peers \"1=host:port,...\""
            << " [--data-dir DIR]\n";
}

}  // namespace

int main(int argc, char** argv) {
  std::signal(SIGPIPE, SIG_IGN);

  int id = 1;
  int port = 19601;
  std::string peersArg;
  std::string dataDir;

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

    FileLogStore log(dataDir);
    KvStateMachine sm;
    SteadyClock clock;
    // Short RPC timeout: a hung peer (e.g. SIGSTOP'd) must not block the
    // ticker long enough to starve live peers of heartbeats. Proper async
    // sends are a listed M5 improvement.
    TcpTransport transport(transportPeers, /*rpcTimeoutMs=*/30);
    RaftNode node(cfg, log, sm, transport, clock);

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
      std::thread([c, &node] { handleConnection(c, node); }).detach();
    }

    ::close(lsock);
    g_running = false;
    ticker.join();
    std::cerr << "[raftkv-node] id=" << id << " shutdown\n";
  } catch (const std::exception& e) {
    std::cerr << "fatal: " << e.what() << "\n";
    return 1;
  }

  // Avoid running stack-object destructors while detached connection threads
  // may still reference `node`; the OS reclaims everything on exit.
  std::_Exit(0);
}
