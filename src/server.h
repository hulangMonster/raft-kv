#pragma once

#include <cstddef>
#include <string>

namespace raftkv {

struct ServerOptions {
  int port = 9527;
  size_t workers = 4;
  std::string dataDir = "data";
  bool sync = true;  // fsync every WAL append (disable for benchmarks)
};

// Blocks until SIGINT/SIGTERM, then shuts down cleanly and returns 0.
// Throws std::exception on fatal setup errors.
int runServer(const ServerOptions& opt);

}  // namespace raftkv
