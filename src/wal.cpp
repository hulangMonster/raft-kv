#include "wal.h"

#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>

#include "common.h"

namespace raftkv {

namespace {

// IEEE CRC-32, used to detect torn/corrupted WAL records.
uint32_t crc32(const Byte* data, size_t len) {
  static uint32_t table[256];
  static const bool tableReady = [] {
    for (uint32_t i = 0; i < 256; ++i) {
      uint32_t c = i;
      for (int k = 0; k < 8; ++k) {
        c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
      }
      table[i] = c;
    }
    return true;
  }();
  (void)tableReady;

  uint32_t crc = 0xFFFFFFFFu;
  for (size_t i = 0; i < len; ++i) {
    crc = table[(crc ^ data[i]) & 0xFFu] ^ (crc >> 8);
  }
  return crc ^ 0xFFFFFFFFu;
}

constexpr size_t kWALHeaderLen = 8;  // crc(4) + payloadLen(4)
constexpr uint32_t kMaxRecordPayload =
    1 + 4 + 4 + 64u * 1024u * 1024u + 64u * 1024u;  // op + lens + generous body cap

inline void throwIo(const std::string& what) {
  throw std::runtime_error(what + ": " + std::strerror(errno));
}

// Reads up to `len` bytes; returns bytes actually read (0 == EOF).
// Retries on EINTR.
size_t readSome(int fd, void* buf, size_t len) {
  auto* p = static_cast<Byte*>(buf);
  size_t got = 0;
  while (got < len) {
    ssize_t r = ::read(fd, p + got, len - got);
    if (r < 0) {
      if (errno == EINTR) continue;
      throwIo("wal read");
    }
    if (r == 0) break;
    got += static_cast<size_t>(r);
  }
  return got;
}

bool writeAll(int fd, const Byte* data, size_t len) {
  size_t written = 0;
  while (written < len) {
    ssize_t r = ::write(fd, data + written, len - written);
    if (r < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    written += static_cast<size_t>(r);
  }
  return true;
}

}  // namespace

Wal::Wal(std::string dataDir, bool sync) : sync_(sync) {
  path_ = dataDir + "/raftkv.wal";
  fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
  if (fd_ < 0) {
    throw std::runtime_error("cannot open wal " + path_ + ": " +
                             std::strerror(errno));
  }
}

Wal::~Wal() {
  if (fd_ >= 0) ::close(fd_);
}

uint64_t Wal::open(const std::function<void(uint8_t, const std::string&,
                                            const std::string&)>& apply) {
  std::lock_guard<std::mutex> lock(mu_);

  uint64_t applied = 0;
  off_t validEnd = 0;
  Byte hdr[kWALHeaderLen];

  for (;;) {
    const size_t gotHdr = readSome(fd_, hdr, kWALHeaderLen);
    if (gotHdr == 0) break;                 // clean EOF at a boundary
    if (gotHdr < kWALHeaderLen) break;      // torn header -> tail
    const uint32_t crc = getU32(hdr);
    const uint32_t len = getU32(hdr + 4);
    if (len == 0 || len > kMaxRecordPayload) break;  // corrupt header

    Bytes payload(len);
    const size_t gotPayload = readSome(fd_, payload.data(), len);
    if (gotPayload < len) break;  // torn record -> tail

    if (crc32(payload.data(), len) != crc) break;  // corrupted record -> tail

    // payload = [op:1][keyLen:4][valLen:4][key...][value...]
    if (payload.size() < 9) break;
    const uint8_t op = payload[0];
    const size_t keyLen = getU32(payload.data() + 1);
    const size_t valLen = getU32(payload.data() + 5);
    if (9 + keyLen + valLen != payload.size()) break;  // malformed record

    apply(op,
          std::string(reinterpret_cast<const char*>(payload.data() + 9),
                      keyLen),
          std::string(reinterpret_cast<const char*>(payload.data() + 9 + keyLen),
                      valLen));
    ++applied;

    // Successful record consumed entirely: mark its end as the valid prefix.
    validEnd = ::lseek(fd_, 0, SEEK_CUR);
  }

  // Drop any torn tail so the log is a clean prefix for the next boot.
  if (::ftruncate(fd_, validEnd) != 0) {
    throwIo("wal truncate");
  }

  applied_ = applied;
  return applied;
}

bool Wal::append(uint8_t op, const std::string& key, const std::string& value) {
  if (key.size() > UINT32_MAX || value.size() > UINT32_MAX) return false;

  Bytes payload;
  payload.reserve(9 + key.size() + value.size());
  payload.push_back(op);
  putU32(payload, static_cast<uint32_t>(key.size()));
  putU32(payload, static_cast<uint32_t>(value.size()));
  payload.insert(payload.end(), key.begin(), key.end());
  payload.insert(payload.end(), value.begin(), value.end());

  Bytes hdr;
  hdr.reserve(kWALHeaderLen);
  putU32(hdr, crc32(payload.data(), payload.size()));
  putU32(hdr, static_cast<uint32_t>(payload.size()));

  std::lock_guard<std::mutex> lock(mu_);
  if (!writeAll(fd_, hdr.data(), hdr.size())) return false;
  if (!writeAll(fd_, payload.data(), payload.size())) return false;
  if (sync_ && ::fsync(fd_) != 0) return false;

  ++applied_;
  return true;
}

}  // namespace raftkv
