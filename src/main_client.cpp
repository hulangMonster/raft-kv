// raftkv_cli: command-line client + sequential micro-benchmark.
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <iostream>
#include <string>
#include <vector>

#include "codec.h"
#include "common.h"

namespace {

using namespace raftkv;
using Clock = std::chrono::steady_clock;

struct CliOptions {
  std::string host = "127.0.0.1";
  int port = 9527;
  size_t valueSize = 32;
};

void printUsage(const char* argv0) {
  std::cerr << "usage:\n"
            << "  " << argv0 << " [--host H] [--port P] put <key> <value>\n"
            << "  " << argv0 << " [--host H] [--port P] get <key>\n"
            << "  " << argv0 << " [--host H] [--port P] del <key>\n"
            << "  " << argv0
            << " [--host H] [--port P] bench <N> [--value-size B]\n"
            << "options: --host H         server address (default 127.0.0.1)\n"
            << "         --port P         server port    (default 9527)\n"
            << "         --value-size B   bench value size (default 32)\n";
}

bool readFully(int fd, void* buf, size_t len) {
  auto* p = static_cast<Byte*>(buf);
  size_t got = 0;
  while (got < len) {
    ssize_t r = ::read(fd, p + got, len - got);
    if (r < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    if (r == 0) return false;
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

int openConnection(const CliOptions& o) {
  struct addrinfo hints {};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  struct addrinfo* res = nullptr;
  const std::string portStr = std::to_string(o.port);
  if (::getaddrinfo(o.host.c_str(), portStr.c_str(), &hints, &res) != 0) {
    return -1;
  }
  int fd = -1;
  for (struct addrinfo* p = res; p != nullptr; p = p->ai_next) {
    fd = ::socket(p->ai_family, p->ai_socktype, p->ai_protocol);
    if (fd < 0) continue;
    if (::connect(fd, p->ai_addr, p->ai_addrlen) == 0) break;
    ::close(fd);
    fd = -1;
  }
  ::freeaddrinfo(res);
  return fd;
}

// One request-response round trip over an open connection.
// Returns 0 on success, -1 on transport/protocol error.
int roundTrip(int fd, const Request& req, Response& out) {
  const Bytes frame = encodeRequest(req);
  if (frame.empty()) return -1;
  if (!writeFully(fd, frame.data(), frame.size())) return -1;

  Byte hdr[kRespHeaderLen];
  if (!readFully(fd, hdr, sizeof(hdr))) return -1;
  const size_t payloadLen = getU32(hdr + 1);
  if (payloadLen > kMaxRequestBytes) return -1;  // reuse as generic 64MiB cap

  Bytes body(payloadLen);
  if (!body.empty() && !readFully(fd, body.data(), body.size())) return -1;

  Bytes respBytes;
  respBytes.reserve(kRespHeaderLen + payloadLen);
  respBytes.insert(respBytes.end(), hdr, hdr + kRespHeaderLen);
  respBytes.insert(respBytes.end(), body.begin(), body.end());
  return parseResponse(respBytes.data(), respBytes.size(), out) ? 0 : -1;
}

// Returns 0 ok, 1 logical error (server replied ERR), -1 transport, -2 usage.
int runPutGetDel(const CliOptions& o, const std::string& cmd,
                 const std::vector<std::string>& pos) {
  Request req;
  if (cmd == "put" && pos.size() >= 2) {
    req.op = OpCode::kPut;
    req.key = pos[0];
    req.value = pos[1];
  } else if (cmd == "get" && pos.size() >= 1) {
    req.op = OpCode::kGet;
    req.key = pos[0];
  } else if (cmd == "del" && pos.size() >= 1) {
    req.op = OpCode::kDel;
    req.key = pos[0];
  } else {
    return -2;
  }

  const int fd = openConnection(o);
  if (fd < 0) {
    std::cerr << "cannot connect to " << o.host << ":" << o.port << "\n";
    return -1;
  }
  Response resp;
  const int rc = roundTrip(fd, req, resp);
  ::close(fd);
  if (rc != 0) {
    std::cerr << "request failed (connection or protocol error)\n";
    return -1;
  }
  if (resp.status == StatusCode::kErr) {
    std::cout << "ERR " << resp.payload << "\n";
    return 1;
  }
  if (cmd == "get") {
    if (resp.status == StatusCode::kNotFound) {
      std::cout << "NOT_FOUND\n";
    } else {
      std::cout << resp.payload << "\n";
    }
  } else {
    std::cout << "OK\n";
  }
  return 0;
}

int runBench(const CliOptions& o, const std::vector<std::string>& pos) {
  if (pos.empty()) return -2;
  const uint64_t n = std::stoull(pos[0]);
  if (n == 0) return 0;

  const int fd = openConnection(o);
  if (fd < 0) {
    std::cerr << "cannot connect to " << o.host << ":" << o.port << "\n";
    return -1;
  }

  std::vector<uint64_t> latUs;
  latUs.reserve(static_cast<size_t>(n));
  const std::string value(o.valueSize, 'x');
  const auto start = Clock::now();
  for (uint64_t i = 0; i < n; ++i) {
    Request req;
    req.op = OpCode::kPut;
    req.key = "bench_key_" + std::to_string(i);
    req.value = value;

    const auto t0 = Clock::now();
    Response resp;
    if (roundTrip(fd, req, resp) != 0 || resp.status != StatusCode::kOk) {
      std::cerr << "bench failed at op " << i << "\n";
      ::close(fd);
      return -1;
    }
    const auto t1 = Clock::now();
    latUs.push_back(static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0)
            .count()));
  }
  const auto end = Clock::now();
  ::close(fd);

  const double totalMs =
      std::chrono::duration<double, std::milli>(end - start).count();
  const double qps =
      totalMs > 0 ? static_cast<double>(n) / (totalMs / 1000.0) : 0.0;

  std::sort(latUs.begin(), latUs.end());
  const auto percentile = [&](double p) -> double {
    if (latUs.empty()) return 0.0;
    const size_t idx =
        static_cast<size_t>(p * static_cast<double>(latUs.size() - 1));
    return static_cast<double>(latUs[idx]);
  };
  double sum = 0;
  for (const double us : latUs) sum += us;
  const double avgUs = latUs.empty() ? 0.0 : sum / latUs.size();

  std::cout << "bench: n=" << n << " value_size=" << o.valueSize
            << " total_ms=" << totalMs << " qps="
            << static_cast<uint64_t>(qps + 0.5) << " avg_us=" << avgUs
            << " p50_us=" << percentile(0.50)
            << " p99_us=" << percentile(0.99) << "\n";
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  std::signal(SIGPIPE, SIG_IGN);

  CliOptions o;
  std::vector<std::string> args(argv + 1, argv + argc);
  std::vector<std::string> pos;  // non-option arguments, in order

  for (size_t i = 0; i < args.size(); ++i) {
    const std::string& a = args[i];
    auto next = [&](const char* what) -> std::string {
      if (i + 1 >= args.size()) {
        std::cerr << "missing value for " << what << "\n";
        std::exit(2);
      }
      return args[++i];
    };
    if (a == "--host") {
      const std::string host = next("--host");
      struct in_addr addr4;
      struct in6_addr addr6;
      if (inet_pton(AF_INET, host.c_str(), &addr4) == 0 &&
          inet_pton(AF_INET6, host.c_str(), &addr6) == 0) {
        std::cerr << "invalid IP address for --host: " << host << "\n";
        printUsage(argv[0]);
        return 2;
      }
      o.host = host;
    } else if (a == "--port") {
      o.port = std::stoi(next("--port"));
    } else if (a == "--value-size") {
      o.valueSize = std::stoul(next("--value-size"));
    } else if (!a.empty() && a[0] == '-') {
      std::cerr << "unknown option: " << a << "\n";
      printUsage(argv[0]);
      return 2;
    } else {
      pos.push_back(a);
    }
  }

  if (pos.empty()) {
    printUsage(argv[0]);
    return 2;
  }
  const std::string cmd = pos.front();
  const std::vector<std::string> rest(pos.begin() + 1, pos.end());

  int rc = -2;
  try {
    if (cmd == "put" || cmd == "get" || cmd == "del") {
      rc = runPutGetDel(o, cmd, rest);
    } else if (cmd == "bench") {
      rc = runBench(o, rest);
    } else {
      std::cerr << "unknown command: " << cmd << "\n";
      printUsage(argv[0]);
      return 2;
    }
  } catch (const std::exception& e) {
    std::cerr << "error: " << e.what() << "\n";
    return 1;
  }
  if (rc == -2) {
    printUsage(argv[0]);
    return 2;
  }
  return rc;
}
