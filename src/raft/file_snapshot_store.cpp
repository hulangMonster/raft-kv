// Snapshot file codec + FileSnapshotStore (M3.4).
//
// File format (m3-design.md v1.1 §5.1):
//   magic(4)="RKS1" | version(1)=1 | lastIncludedIndex(8) | lastIncludedTerm(8)
//   | payloadLen(4) | crc32(4, over payload) | payload[payloadLen]
// Writes are atomic (tmp -> fsync -> rename -> fsync(dir)); a torn/corrupt
// file is discarded by load() so the caller falls back to a full log replay.
#include "raft/snapshot_store.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <string>
#include <utility>

#include "common.h"

namespace raftkv::raft {

namespace {

constexpr size_t kFileHeaderLen = 29;

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

bool readWhole(const std::string& path, Bytes& out) {
  const int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) return false;
  out.clear();
  Byte buf[8192];
  for (;;) {
    ssize_t r = ::read(fd, buf, sizeof(buf));
    if (r < 0) {
      if (errno == EINTR) continue;
      ::close(fd);
      return false;
    }
    if (r == 0) break;
    out.insert(out.end(), buf, buf + r);
  }
  ::close(fd);
  return true;
}

void fsyncDir(const std::string& dir) {
  const int fd = ::open(dir.c_str(), O_RDONLY);
  if (fd < 0) return;
  (void)::fsync(fd);
  ::close(fd);
}

}  // namespace

Bytes encodeSnapshotFile(const SnapshotData& d) {
  Bytes out;
  out.reserve(kFileHeaderLen + d.payload.size());
  out.push_back('R');
  out.push_back('K');
  out.push_back('S');
  out.push_back('1');
  out.push_back(0x01);
  putU64(out, d.lastIncludedIndex);
  putU64(out, d.lastIncludedTerm);
  putU32(out, static_cast<uint32_t>(d.payload.size()));
  putU32(out, crc32(d.payload.data(), d.payload.size()));
  out.insert(out.end(), d.payload.begin(), d.payload.end());
  return out;
}

bool decodeSnapshotFile(const Byte* bytes, size_t n, SnapshotData& out) {
  if (n < kFileHeaderLen) return false;
  if (std::memcmp(bytes, "RKS1", 4) != 0) return false;
  if (bytes[4] != 0x01) return false;

  out.lastIncludedIndex = getU64(bytes + 5);
  out.lastIncludedTerm = getU64(bytes + 13);
  const uint32_t payloadLen = getU32(bytes + 21);
  const uint32_t crc = getU32(bytes + 25);
  if (kFileHeaderLen + payloadLen != n) return false;
  if (crc32(bytes + kFileHeaderLen, payloadLen) != crc) return false;

  out.payload.assign(bytes + kFileHeaderLen, bytes + n);
  return true;
}

// ---- FileSnapshotStore ------------------------------------------------------

FileSnapshotStore::FileSnapshotStore(std::string dir) : dir_(std::move(dir)) {
  std::error_code ec;
  std::filesystem::create_directories(dir_ + "/raft", ec);
  path_ = dir_ + "/raft/snapshot.dat";
  recvPath_ = dir_ + "/raft/snapshot.dat.recv";
  // Remember the boundary already on disk so save() can reject stale snapshots
  // without re-reading the file on every call.
  Bytes existing;
  SnapshotData tmp;
  if (readWhole(path_, existing) &&
      decodeSnapshotFile(existing.data(), existing.size(), tmp)) {
    boundary_ = tmp.lastIncludedIndex;
  }
}

void FileSnapshotStore::resetRecv() {  // caller holds mu_
  recvIndex_ = kNoIndex;
  recvTerm_ = kNoTerm;
  recvEnd_ = 0;
}

bool FileSnapshotStore::load(SnapshotData& out) {
  std::lock_guard<std::mutex> lock(mu_);
  Bytes bytes;
  if (!readWhole(path_, bytes)) return false;
  return decodeSnapshotFile(bytes.data(), bytes.size(), out);
}

bool FileSnapshotStore::save(const SnapshotData& data) {
  std::lock_guard<std::mutex> lock(mu_);
  // B5: never overwrite a snapshot at or above the current boundary (it may
  // have been installed concurrently and is newer than this one).
  if (boundary_ != kNoIndex && data.lastIncludedIndex <= boundary_) {
    return false;
  }
  const Bytes bytes = encodeSnapshotFile(data);
  const std::string tmp = path_ + ".tmp";

  const int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) return false;
  const bool ok = writeAll(fd, bytes.data(), bytes.size()) && (::fsync(fd) == 0);
  ::close(fd);
  if (!ok) {
    ::unlink(tmp.c_str());
    return false;
  }
  if (::rename(tmp.c_str(), path_.c_str()) != 0) {
    ::unlink(tmp.c_str());
    return false;
  }
  fsyncDir(dir_ + "/raft");
  boundary_ = data.lastIncludedIndex;
  resetRecv();
  return true;
}

bool FileSnapshotStore::receiveChunk(Index lastIncludedIndex,
                                     Term lastIncludedTerm, uint64_t offset,
                                     const Bytes& data, bool done) {
  std::lock_guard<std::mutex> lock(mu_);

  // A new (index, term) - or a restart at offset 0 - begins a fresh transfer.
  if (lastIncludedIndex != recvIndex_ || lastIncludedTerm != recvTerm_ ||
      offset == 0) {
    recvIndex_ = lastIncludedIndex;
    recvTerm_ = lastIncludedTerm;
    recvEnd_ = 0;
    if (offset != 0) return false;  // ask the leader to restart at 0
  }
  // B7: a retransmitted chunk is an idempotent success, never a second append.
  if (offset + data.size() <= recvEnd_) return true;
  if (offset != recvEnd_) {  // gap / overlap: restart the transfer
    resetRecv();
    return false;
  }

  int flags = O_RDWR | O_CREAT;
  if (offset == 0) flags |= O_TRUNC;  // (re)start the transfer
  const int fd = ::open(recvPath_.c_str(), flags, 0644);
  if (fd < 0) {
    resetRecv();
    return false;
  }
  if (static_cast<uint64_t>(::lseek(fd, 0, SEEK_END)) != offset) {
    ::close(fd);
    resetRecv();
    return false;  // stale .recv left over from an earlier run
  }
  if (!writeAll(fd, data.data(), data.size())) {
    ::close(fd);
    resetRecv();
    return false;
  }
  if (done && ::fsync(fd) != 0) {
    ::close(fd);
    resetRecv();
    return false;
  }
  ::close(fd);
  recvEnd_ += data.size();
  if (!done) return true;

  Bytes whole;
  if (!readWhole(recvPath_, whole)) {
    resetRecv();
    return false;
  }
  SnapshotData parsed;
  if (!decodeSnapshotFile(whole.data(), whole.size(), parsed) ||
      parsed.lastIncludedIndex != lastIncludedIndex ||
      parsed.lastIncludedTerm != lastIncludedTerm) {
    ::unlink(recvPath_.c_str());  // torn / mismatched -> discard
    resetRecv();
    return false;
  }
  if (boundary_ != kNoIndex && parsed.lastIncludedIndex <= boundary_) {
    // A newer snapshot is already installed: drop the stale transfer.
    ::unlink(recvPath_.c_str());
    resetRecv();
    return true;
  }
  if (::rename(recvPath_.c_str(), path_.c_str()) != 0) {
    resetRecv();
    return false;
  }
  fsyncDir(dir_ + "/raft");
  boundary_ = parsed.lastIncludedIndex;
  resetRecv();
  return true;
}

}  // namespace raftkv::raft

