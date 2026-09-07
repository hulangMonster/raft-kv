// raftkv_server: single-node key-value store daemon (milestone M1).
#include <cstdlib>
#include <exception>
#include <iostream>
#include <string>

#include "server.h"

namespace {

void printUsage(const char* argv0) {
  std::cerr << "usage: " << argv0 << " [--port N] [--workers N]"
            << " [--data-dir DIR] [--no-sync] [--help]\n"
            << "  --port      listening port     (default 9527)\n"
            << "  --workers   pool size          (default 4)\n"
            << "  --data-dir  WAL + data dir     (default ./data)\n"
            << "  --no-sync   skip fsync per op (faster, less durable)\n";
}

}  // namespace

int main(int argc, char** argv) {
  raftkv::ServerOptions opt;
  try {
    for (int i = 1; i < argc; ++i) {
      const std::string a = argv[i];
      auto next = [&](const char* what) -> std::string {
        if (i + 1 >= argc) {
          std::cerr << "missing value for " << what << "\n";
          std::exit(2);
        }
        return argv[++i];
      };
      if (a == "--port") {
        opt.port = std::stoi(next("--port"));
      } else if (a == "--workers") {
        opt.workers = std::stoul(next("--workers"));
      } else if (a == "--data-dir") {
        opt.dataDir = next("--data-dir");
      } else if (a == "--no-sync") {
        opt.sync = false;
      } else if (a == "--help") {
        printUsage(argv[0]);
        return 0;
      } else {
        std::cerr << "unknown option: " << a << "\n";
        printUsage(argv[0]);
        return 2;
      }
    }
  } catch (const std::exception& e) {
    std::cerr << "bad arguments: " << e.what() << "\n";
    printUsage(argv[0]);
    return 2;
  }

  try {
    return raftkv::runServer(opt);
  } catch (const std::exception& e) {
    std::cerr << "fatal: " << e.what() << "\n";
    return 1;
  }
}
