// FileLogStore: durable raft log + meta (M2.4).
//
// On-disk layout (all integers big-endian):
//   meta.dat : [crc32:4][len:4][term:8][votedFor:4]   (written tmp + rename)
//   raft.log : repeated [crc32:4][len:4][payload]
//     payload = [index:8][term:8][op:1][keyLen:4][valLen:4]
//               [clientId:8][requestId:8][key...][value...]
//
// A partially written / corrupted tail (e.g. after kill -9) is detected by
// CRC/length checks during load() and truncated to the last valid record.
#include "raft/log_store.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <stdexcept>
#include <utility>

#include "common.h"

namespace raftkv::raft {

namespace {

constexpr size_t kEntryFixedLen = 41;   // 8+8+1+4+4+8+8
constexpr size_t kMetaPayloadLen = 12;  // term(8) + votedFor(4)
constexpr size_t kFrameHeaderLen = 8;   // crc(4) + len(4)
constexpr uint32_t kMaxRecordPayload =
    kEntryFixedLen + 64u * 1024u * 1024u + 64u * 1024u;

// IEEE CRC-32 (same polynomial as M1's WAL).
uint32_t crc32(const Byte* data, size_t len) {
  static uint32_t table[256];
  static const bool ready = [] {
    for (uint32_t i = 0; i < 256; ++i) {
      uint32_t c = i;
      for (int k = 0; k < 8; ++k) {
        c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
      }
      table[i] = c;
    }
    return true;
  }();
  (void)ready;

  uint32_t crc = 0xFFFFFFFFu;
  for (size_t i = 0; i < len; ++i) {
    crc = table[(crc ^ data[i]) & 0xFFu] ^ (crc >> 8);
  }
  return crc ^ 0xFFFFFFFFu;
}

void putU64(Bytes& out, uint64_t v) {
  for (int i = 7; i >= 0; --i) {
    out.push_back(static_cast<Byte>((v >> (i * 8)) & 0xff));
  }
}
uint64_t getU64(const Byte* p) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) v = (v << 8) | p[i];
  return v;
}

size_t readSome(int fd, void* buf, size_t len) {
  auto* p = static_cast<Byte*>(buf);
  size_t got = 0;
  while (got < len) {
    ssize_t r = ::read(fd, p + got, len - got);
    if (r < 0) {
      if (errno == EINTR) continue;
      return got;  // I/O error: caller treats short read as torn tail
    }
    if (r == 0) break;
    got += static_cast<size_t>(r);
  }
  return got;
}

bool writeAll(int fd, const Byte* data, size_t len) {
  size_t done = 0;
  while (done < len) {
    ssize_t r = ::write(fd, data + done, len - done);
    if (r < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    done += static_cast<size_t>(r);
  }
  return true;
}

Bytes encodeEntry(const LogEntry& e) {
  Bytes p;
  p.reserve(kEntryFixedLen + e.key.size() + e.value.size());
  putU64(p, e.index);
  putU64(p, e.term);
  p.push_back(static_cast<Byte>(e.op));
  putU32(p, static_cast<uint32_t>(e.key.size()));
  putU32(p, static_cast<uint32_t>(e.value.size()));
  putU64(p, e.clientId);
  putU64(p, e.requestId);
  p.insert(p.end(), e.key.begin(), e.key.end());
  p.insert(p.end(), e.value.begin(), e.value.end());
  return p;
}

bool decodeEntry(const Byte* p, size_t n, LogEntry& e) {
  if (n < kEntryFixedLen) return false;
  const uint8_t op = p[16];
  if (op != static_cast<uint8_t>(OpCode::kPut) &&
      op != static_cast<uint8_t>(OpCode::kGet) &&
      op != static_cast<uint8_t>(OpCode::kDel)) {
    return false;
  }
  const size_t keyLen = getU32(p + 17);
  const size_t valLen = getU32(p + 21);
  if (n != kEntryFixedLen + keyLen + valLen) return false;

  e.index = getU64(p);
  e.term = getU64(p + 8);
  e.op = static_cast<OpCode>(op);
  e.clientId = getU64(p + 25);
  e.requestId = getU64(p + 33);
  e.key.assign(reinterpret_cast<const char*>(p + kEntryFixedLen), keyLen);
  e.value.assign(
      reinterpret_cast<const char*>(p + kEntryFixedLen + keyLen), valLen);
  return true;
}

}  // namespace

FileLogStore::FileLogStore(std::string dir) : dir_(std::move(dir)) {
  std::error_code ec;
  std::filesystem::create_directories(dir_ + "/raft", ec);
  if (ec) {
    throw std::runtime_error("cannot create raft dir " + dir_ + "/raft: " +
                             ec.message());
  }
  metaPath_ = dir_ + "/raft/meta.dat";
  logPath_ = dir_ + "/raft/raft.log";
  logFd_ = ::open(logPath_.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
  if (logFd_ < 0) {
    throw std::runtime_error("cannot open " + logPath_ + ": " +
                             std::strerror(errno));
  }
}

FileLogStore::~FileLogStore() {
  if (logFd_ >= 0) ::close(logFd_);
}

bool FileLogStore::load(Term& term, int& votedFor, Index& lastIndex) {
  // 1. Metadata.
  term_ = kNoTerm;
  votedFor_ = -1;
  const int mfd = ::open(metaPath_.c_str(), O_RDONLY);
  if (mfd >= 0) {
    Byte hdr[kFrameHeaderLen];
    if (readSome(mfd, hdr, sizeof(hdr)) == sizeof(hdr)) {
      const uint32_t crc = getU32(hdr);
      const uint32_t len = getU32(hdr + 4);
      if (len == kMetaPayloadLen) {
        Bytes payload(len);
        if (readSome(mfd, payload.data(), len) == len &&
            crc32(payload.data(), len) == crc) {
          term_ = getU64(payload.data());
          votedFor_ = static_cast<int>(getU32(payload.data() + 8));
        }
      }
    }
    ::close(mfd);
  }

  // 2. Log records.
  entries_.clear();
  offsetOf_.clear();
  ::lseek(logFd_, 0, SEEK_SET);

  off_t validEnd = 0;
  for (;;) {
    const off_t recStart = ::lseek(logFd_, 0, SEEK_CUR);
    Byte hdr[kFrameHeaderLen];
    const size_t gotHdr = readSome(logFd_, hdr, sizeof(hdr));
    if (gotHdr == 0) break;                        // clean EOF at a boundary
    if (gotHdr < sizeof(hdr)) break;               // torn header

    const uint32_t crc = getU32(hdr);
    const uint32_t len = getU32(hdr + 4);
    if (len == 0 || len > kMaxRecordPayload) break;

    Bytes payload(len);
    if (readSome(logFd_, payload.data(), len) < len) break;  // torn body
    if (crc32(payload.data(), len) != crc) break;            // corrupt record

    LogEntry e;
    if (!decodeEntry(payload.data(), len, e)) break;
    if (e.index != entries_.size() + 1) break;  // must be contiguous

    entries_.push_back(e);
    offsetOf_[e.index] = static_cast<int64_t>(recStart);
    validEnd = ::lseek(logFd_, 0, SEEK_CUR);
  }

  // Drop any torn tail so the log is a clean prefix for the next boot.
  if (::ftruncate(logFd_, validEnd) != 0) return false;

  term = term_;
  votedFor = votedFor_;
  lastIndex = this->lastIndex();
  return true;
}

bool FileLogStore::persistMeta(Term term, int votedFor) {
  Bytes payload;
  payload.reserve(kMetaPayloadLen);
  putU64(payload, term);
  putU32(payload, static_cast<uint32_t>(votedFor));

  Bytes frame;
  frame.reserve(kFrameHeaderLen + payload.size());
  putU32(frame, crc32(payload.data(), payload.size()));
  putU32(frame, static_cast<uint32_t>(payload.size()));
  frame.insert(frame.end(), payload.begin(), payload.end());

  const std::string tmp = metaPath_ + ".tmp";
  const int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) return false;
  const bool ok = writeAll(fd, frame.data(), frame.size()) &&
                  (::fsync(fd) == 0);
  ::close(fd);
  if (!ok) return false;
  if (::rename(tmp.c_str(), metaPath_.c_str()) != 0) return false;

  term_ = term;
  votedFor_ = votedFor;
  return true;
}

bool FileLogStore::append(const std::vector<LogEntry>& entries) {
  for (const LogEntry& e : entries) {
    if (e.index <= lastIndex()) {
      if (termAt(e.index) == e.term) continue;  // already present
      if (!truncateSuffix(e.index)) return false;
    }
    const int64_t off = static_cast<int64_t>(::lseek(logFd_, 0, SEEK_END));

    const Bytes payload = encodeEntry(e);
    Bytes hdr;
    hdr.reserve(kFrameHeaderLen);
    putU32(hdr, crc32(payload.data(), payload.size()));
    putU32(hdr, static_cast<uint32_t>(payload.size()));

    if (!writeAll(logFd_, hdr.data(), hdr.size())) return false;
    if (!writeAll(logFd_, payload.data(), payload.size())) return false;

    entries_.push_back(e);
    offsetOf_[e.index] = off;
  }
  if (::fsync(logFd_) != 0) return false;
  return true;
}

bool FileLogStore::truncateSuffix(Index fromIndex) {
  if (fromIndex == kNoIndex) return true;
  if (fromIndex > lastIndex() + 1) return false;

  const int64_t off = (fromIndex <= lastIndex())
                          ? offsetOf_[fromIndex]
                          : static_cast<int64_t>(::lseek(logFd_, 0, SEEK_END));
  if (::ftruncate(logFd_, static_cast<off_t>(off)) != 0) return false;

  entries_.resize(static_cast<size_t>(fromIndex - 1));
  for (auto it = offsetOf_.begin(); it != offsetOf_.end();) {
    if (it->first >= fromIndex) {
      it = offsetOf_.erase(it);
    } else {
      ++it;
    }
  }
  return true;
}

std::vector<LogEntry> FileLogStore::slice(Index from, size_t maxEntries,
                                          size_t maxBytes) const {
  std::vector<LogEntry> out;
  size_t bytes = 0;
  for (const LogEntry& e : entries_) {
    if (e.index < from) continue;
    if (out.size() >= maxEntries) break;
    const size_t sz = e.key.size() + e.value.size();
    if (!out.empty() && bytes + sz > maxBytes) break;
    out.push_back(e);
    bytes += sz;
  }
  return out;
}

Index FileLogStore::lastIndex() const {
  return static_cast<Index>(entries_.size());
}

Term FileLogStore::lastTerm() const {
  return entries_.empty() ? kNoTerm : entries_.back().term;
}

Term FileLogStore::termAt(Index index) const {
  if (index == kNoIndex || index > lastIndex()) return kNoTerm;
  return entries_[static_cast<size_t>(index - 1)].term;
}

}  // namespace raftkv::raft
