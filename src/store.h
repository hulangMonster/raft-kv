#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace raftkv {

class Wal;

struct StoreResult {
  bool ok = true;      // false == persistent I/O failure
  bool found = false;  // meaningful for GET
  std::string value;   // GET payload
  std::string err;     // human-readable message when !ok
};

// Thread-safe in-memory KV guarded by a single mutex (stage 1).
//
// Ordering guarantee: every mutation is appended to the WAL *before* it is
// applied to memory, and the two steps run under the same lock so that the
// WAL order always matches the in-memory order. Replay on startup therefore
// reproduces exactly the last acknowledged state.
class Store {
 public:
  // Opens/creates the data directory and replays the WAL.
  Store(std::string dataDir, bool sync);
  ~Store();  // defined out-of-line so Wal is complete at destruction point

  StoreResult put(const std::string& key, const std::string& value);
  StoreResult get(const std::string& key) const;
  StoreResult del(const std::string& key);
  size_t size() const;

 private:
  std::string dataDir_;
  std::unique_ptr<Wal> wal_;
  std::unordered_map<std::string, std::string> data_;
  mutable std::mutex mu_;
};

}  // namespace raftkv
