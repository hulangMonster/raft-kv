#pragma once

#include <cstdint>
#include <functional>
#include <mutex>
#include <string>

namespace raftkv {

// Append-only Write-Ahead Log.
//
// On-disk layout (all integers big-endian):
//   [crc32(payload):4][payloadLen:4][payload...]
//   payload = [op:1][keyLen:4][valLen:4][key...][value...]
//
// A partially written / corrupted tail (e.g. after kill -9) is detected by
// CRC/length checks during open() and truncated, keeping a valid prefix.
class Wal {
 public:
  // Opens/creates `<dataDir>/raftkv.wal`; throws std::runtime_error on failure.
  Wal(std::string dataDir, bool sync);
  ~Wal();

  Wal(const Wal&) = delete;
  Wal& operator=(const Wal&) = delete;

  // Replays the log, invoking `apply(op, key, value)` for each valid record.
  // Returns the number of applied records. Truncates any torn tail.
  uint64_t open(const std::function<void(uint8_t, const std::string&,
                                         const std::string&)>& apply);

  // Appends one record. When sync=true, fsync() is issued before returning.
  bool append(uint8_t op, const std::string& key, const std::string& value);

  uint64_t applied() const { return applied_; }

 private:
  std::string path_;
  int fd_ = -1;
  bool sync_ = false;
  uint64_t applied_ = 0;
  std::mutex mu_;
};

}  // namespace raftkv
