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

// rename()/ftruncate() only become durable with the directory entry flushed.
void fsyncDir(const std::string& dir) {
  const int fd = ::open(dir.c_str(), O_RDONLY);
  if (fd < 0) return;
  (void)::fsync(fd);
  ::close(fd);
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
      op != static_cast<uint8_t>(OpCode::kDel) &&
      op != static_cast<uint8_t>(OpCode::kConfig)) {  // M4: 配置条目（m4-prerequisites §5.1-14）
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

FileLogStore::FileLogStore(std::string dir, FlushFn flush)
    : dir_(std::move(dir)), flush_(std::move(flush)) {
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
  std::lock_guard<std::recursive_mutex> lock(mu_);
  if (logFd_ < 0) return false;
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

  // D3: after compaction the log may start above index 1, and a failed/partial
  // compact may leave records below the boundary (those are skipped).
  const Index first = firstIndex();
  off_t validEnd = 0;
  bool sawFirst = false;
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

    if (e.index < first) {  // leftover below the boundary: skip it
      validEnd = ::lseek(logFd_, 0, SEEK_CUR);
      continue;
    }
    if (!sawFirst) {
      // A record above the boundary without its predecessors means the compacted
      // prefix is gone: this log can no longer be replayed. Report it instead of
      // silently truncating the whole file away.
      if (e.index != first) return false;
      sawFirst = true;
    } else if (e.index != entries_.back().index + 1) {
      break;  // gap in the middle -> torn tail
    }

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
  std::lock_guard<std::recursive_mutex> lock(mu_);
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
  fsyncDir(dir_ + "/raft");  // the rename itself must survive a crash

  term_ = term;
  votedFor_ = votedFor;
  return true;
}

bool FileLogStore::append(const std::vector<LogEntry>& entries) {
  {
    std::lock_guard<std::recursive_mutex> lock(mu_);
    if (!appendNoSyncLocked(entries)) return false;
  }
  // 语义不变（返回前必须 durable），但 flush 走 sync()：不再持 mu_。
  return sync();
}

// M5.6（D-2 修复）：fsync 期间**不持 mu_**。
//
// M5.2 把 fsync 移出 RaftNode::mu_ 之后，FileLogStore 里 sync() 与 appendNoSync()
// 仍然共用同一把 mu_，而 sync() 是"持锁 fsync"。于是 leader 的锁外 fsync（本机 ~3-8ms）
// 会把所有 append 挡在门外：fsync 窗口内日志**不再增长**，组提交的 batch 只能是
// fsync 开始前已经 append 的那几条 —— 实测 p=8/3 节点：leader 1650 次 fsync / 2200 条写
// （1.33 条/次）、batch_avg=1，吞吐掉到 M4 基线的 0.47×。
//
// 现在 fsync 只与"会换 logFd_ / 改文件结构"的操作互斥（flushMu_，叶子锁；锁序
// flushMu_ -> mu_），append 可以继续写 page cache，批才能成形。
// durability 语义不变：调用方只声称 flushTarget（fsync 开始前就已写下的范围）已 durable。
bool FileLogStore::sync() {
  std::lock_guard<std::mutex> flushLock(flushMu_);
  int fd = -1;
  {
    std::lock_guard<std::recursive_mutex> lock(mu_);
    fd = logFd_;
  }
  if (fd < 0) return false;
  return flushFd(fd);
}

// flush 原语：默认 ::fsync；测试可注入"慢 fsync"来验证 append 不被它阻塞。
bool FileLogStore::flushFd(int fd) {
  if (flush_) return flush_(fd) == 0;
  return ::fsync(fd) == 0;
}

bool FileLogStore::appendNoSync(const std::vector<LogEntry>& entries) {
  std::lock_guard<std::recursive_mutex> lock(mu_);
  return appendNoSyncLocked(entries);
}

bool FileLogStore::appendNoSyncLocked(const std::vector<LogEntry>& entries) {
  if (logFd_ < 0) return false;  // store is broken (failed compact reopen)
  for (const LogEntry& e : entries) {
    if (e.index <= lastIndex()) {
      if (termAt(e.index) == e.term) continue;  // already present
      // M5.6（D-2）：这里不能 fsync —— 本函数在 mu_ 下被调用，而 fsync 要拿 flushMu_。
      // 契约上 appendNoSync() 之后调用方必须 sync()（组提交），那次 fsync 会一并
      // 持久化 ftruncate 造成的长度变化；append()（durable 变体）末尾也调 sync()。
      if (!truncateSuffixLocked(e.index, /*flush=*/false)) return false;
    }
    if (e.index != lastIndex() + 1) return false;  // must be contiguous (D3)
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
  return true;  // NOT durable yet: caller batches with sync() (group commit)
}

bool FileLogStore::truncateSuffix(Index fromIndex) {
  std::lock_guard<std::mutex> flushLock(flushMu_);  // 锁序 flushMu_ -> mu_
  std::lock_guard<std::recursive_mutex> lock(mu_);
  return truncateSuffixLocked(fromIndex, /*flush=*/true);
}

// M5.2（I9）：不做 fsync 的截断（供持 RaftNode::mu_ 的冲突回滚路径使用）。
// 调用方必须在同一批里随后 log_.sync()，否则变短的日志在崩溃后可能复活。
bool FileLogStore::truncateSuffixNoSync(Index fromIndex) {
  std::lock_guard<std::recursive_mutex> lock(mu_);
  return truncateSuffixLocked(fromIndex, /*flush=*/false);
}

// flush=true 时要求调用方已持有 flushMu_（与在飞的 fsync 互斥）。
bool FileLogStore::truncateSuffixLocked(Index fromIndex, bool flush) {
  if (fromIndex == kNoIndex) return true;
  if (fromIndex <= lastIncluded_) return false;  // cannot cut below boundary
  if (fromIndex > lastIndex() + 1) return false;
  if (logFd_ < 0) return false;

  int64_t off = 0;
  if (fromIndex <= lastIndex()) {
    // A missing offset must never be treated as 0: that would ftruncate the
    // entire log away.
    const auto it = offsetOf_.find(fromIndex);
    if (it == offsetOf_.end()) return false;
    off = it->second;
  } else {
    off = static_cast<int64_t>(::lseek(logFd_, 0, SEEK_END));
    if (off < 0) return false;
  }
  if (::ftruncate(logFd_, static_cast<off_t>(off)) != 0) return false;
  // Make the shorter log durable: otherwise a crash could resurrect the entries
  // we just dropped. M5.2（I9）：持 mu_ 的路径传 sync=false，由同批的锁外
  // log_.sync() 负责持久化（fsync 会一并覆盖 size 变更）。
  if (flush && !flushFd(logFd_)) return false;

  entries_.resize(static_cast<size_t>(fromIndex - firstIndex()));
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
  std::lock_guard<std::recursive_mutex> lock(mu_);
  std::vector<LogEntry> out;
  size_t bytes = 0;
  Index start = from;
  if (start < firstIndex()) start = firstIndex();  // clamp (D3)
  for (const LogEntry& e : entries_) {
    if (e.index < start) continue;
    if (out.size() >= maxEntries) break;
    const size_t sz = e.key.size() + e.value.size();
    if (!out.empty() && bytes + sz > maxBytes) break;
    out.push_back(e);
    bytes += sz;
  }
  return out;
}

Index FileLogStore::lastIndex() const {
  std::lock_guard<std::recursive_mutex> lock(mu_);
  return entries_.empty() ? lastIncluded_
                          : static_cast<Index>(entries_.back().index);
}

Term FileLogStore::lastTerm() const {
  std::lock_guard<std::recursive_mutex> lock(mu_);
  if (!entries_.empty()) return entries_.back().term;
  return (lastIncluded_ == kNoIndex) ? kNoTerm : lastIncludedTerm_;
}

Term FileLogStore::termAt(Index index) const {
  std::lock_guard<std::recursive_mutex> lock(mu_);
  if (index == kNoIndex) return kNoTerm;
  if (index == lastIncluded_ && lastIncluded_ != kNoIndex) {
    return lastIncludedTerm_;  // boundary entry (D3)
  }
  if (index < firstIndex() || index > lastIndex()) return kNoTerm;
  return entries_[static_cast<size_t>(index - firstIndex())].term;
}

// ---- M3 compaction (D3) -----------------------------------------------------

void FileLogStore::setBoundary(Index lastIncludedIndex,
                               Term lastIncludedTerm) {
  std::lock_guard<std::recursive_mutex> lock(mu_);
  lastIncluded_ = lastIncludedIndex;
  lastIncludedTerm_ = lastIncludedTerm;
  size_t drop = 0;
  while (drop < entries_.size() && entries_[drop].index <= lastIncludedIndex) {
    offsetOf_.erase(entries_[drop].index);
    ++drop;
  }
  if (drop > 0) {
    entries_.erase(entries_.begin(),
                   entries_.begin() + static_cast<ptrdiff_t>(drop));
  }
}

bool FileLogStore::compact(Index upTo, Term termAtUpTo) {
  // M5.6（D-2）：本函数会 rename + 重开 logFd_，必须与在飞的 fsync 互斥（否则 fsync
  // 可能落在已经被换掉的 fd 上）。锁序 flushMu_ -> mu_。
  std::lock_guard<std::mutex> flushLock(flushMu_);
  std::lock_guard<std::recursive_mutex> lock(mu_);
  if (upTo == kNoIndex) return true;
  if (upTo <= lastIncluded_) return true;  // already compacted: no-op
  // upTo may exceed lastIndex() (InstallSnapshot): the retained suffix is empty.

  std::vector<LogEntry> keep;
  for (const LogEntry& e : entries_) {
    if (e.index > upTo) keep.push_back(e);
  }

  const std::string tmp = logPath_ + ".tmp";
  const int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) return false;

  std::unordered_map<Index, int64_t> newOffsets;
  bool ok = true;
  for (const LogEntry& e : keep) {
    const int64_t off = static_cast<int64_t>(::lseek(fd, 0, SEEK_END));
    const Bytes payload = encodeEntry(e);
    Bytes hdr;
    hdr.reserve(kFrameHeaderLen);
    putU32(hdr, crc32(payload.data(), payload.size()));
    putU32(hdr, static_cast<uint32_t>(payload.size()));
    if (!writeAll(fd, hdr.data(), hdr.size()) ||
        !writeAll(fd, payload.data(), payload.size())) {
      ok = false;
      break;
    }
    newOffsets[e.index] = off;
  }
  if (ok) ok = (::fsync(fd) == 0);
  ::close(fd);
  if (!ok) {
    ::unlink(tmp.c_str());
    return false;
  }
  if (::rename(tmp.c_str(), logPath_.c_str()) != 0) {
    ::unlink(tmp.c_str());
    return false;
  }
  fsyncDir(dir_ + "/raft");  // make the rename durable before reopening

  if (logFd_ >= 0) ::close(logFd_);
  logFd_ = -1;
  logFd_ = ::open(logPath_.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
  if (logFd_ < 0) {
    // The file was already replaced but we cannot use it any more: the caller
    // must treat this as a fatal store failure, not as "compaction skipped".
    return false;
  }

  entries_ = std::move(keep);
  offsetOf_ = std::move(newOffsets);
  lastIncluded_ = upTo;
  lastIncludedTerm_ = termAtUpTo;
  return true;
}

Index FileLogStore::firstIndex() const {
  std::lock_guard<std::recursive_mutex> lock(mu_); return lastIncluded_ + 1; }
Index FileLogStore::lastIncludedIndex() const {
  std::lock_guard<std::recursive_mutex> lock(mu_); return lastIncluded_; }
Term FileLogStore::lastIncludedTerm() const {
  std::lock_guard<std::recursive_mutex> lock(mu_); return lastIncludedTerm_; }

}  // namespace raftkv::raft
