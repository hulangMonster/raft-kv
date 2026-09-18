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

// M5.4（§8.3/§8.4）：CRC32 的**增量**版本 —— 流式写盘与 .recv 头部都要"边收边算"。
// crc32Update 接受/返回**未做最终异或**的中间值；与一次性 crc32() 逐位一致。
uint32_t crc32Update(uint32_t crc, const Byte* data, size_t len) {
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
  for (size_t i = 0; i < len; ++i) {
    crc = table[(crc ^ data[i]) & 0xFFu] ^ (crc >> 8);
  }
  return crc;
}

uint32_t crc32(const Byte* data, size_t len) {
  return crc32Update(0xFFFFFFFFu, data, len) ^ 0xFFFFFFFFu;
}

// M5.4（§8.3）：流式落盘的块大小（1 MiB；InstallSnapshot 的 snapshotChunkBytes 同量级）
constexpr size_t kSaveChunkBytes = 1u << 20;

// M5.4（§8.4）：.recv 的续传元数据放在**尾部**（而不是头部）——这样
// 载荷从偏移 0 开始连续存放，安装时可以 ftruncate 掉尾部后直接 rename，
// 不需要为了去掉头部再复制一份整块 payload。
// 布局：[payload: receivedLen] | magic(4)="RKR1" | idx(8) | term(8) | receivedLen(8) | crcSoFar(4)
constexpr size_t kRecvTrailerLen = 32;
constexpr char kRecvMagic[4] = {'R', 'K', 'R', '1'};

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

// pwrite 版：把尾部字节写在载荷之后（不移动文件偏移）
bool writeAllAt(int fd, const Byte* data, size_t len, off_t off) {
  size_t done = 0;
  while (done < len) {
    ssize_t r = ::pwrite(fd, data + done, len - done, off + static_cast<off_t>(done));
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
  // v1 = 无配置段；v2 = payload 之后追加 [configLen:4][configCrc:4][config]（M4.3）
  const bool withConfig = !d.config.empty();
  Bytes out;
  out.reserve(kFileHeaderLen + d.payload.size() + 8 + d.config.size());
  out.push_back('R');
  out.push_back('K');
  out.push_back('S');
  out.push_back('1');
  out.push_back(withConfig ? 0x02 : 0x01);
  putU64(out, d.lastIncludedIndex);
  putU64(out, d.lastIncludedTerm);
  putU32(out, static_cast<uint32_t>(d.payload.size()));
  putU32(out, crc32(d.payload.data(), d.payload.size()));
  out.insert(out.end(), d.payload.begin(), d.payload.end());
  if (withConfig) {
    putU32(out, static_cast<uint32_t>(d.config.size()));
    putU32(out, crc32(d.config.data(), d.config.size()));
    out.insert(out.end(), d.config.begin(), d.config.end());
  }
  return out;
}

// M5.4（§8.3）默认实现：整块序列化后交给 save()（内存适配器沿用；File 覆写为分块写盘）。
bool SnapshotStore::saveStreaming(const SnapshotView& view, Index lastIncludedIndex,
                                  Term lastIncludedTerm, const Bytes& config) {
  SnapshotData data;
  data.lastIncludedIndex = lastIncludedIndex;
  data.lastIncludedTerm = lastIncludedTerm;
  data.config = config;
  std::unique_ptr<SnapshotStream> st = view.stream();
  Bytes buf;
  while (st->next(buf, kSaveChunkBytes)) {
    data.payload.insert(data.payload.end(), buf.begin(), buf.end());
  }
  return save(data);
}

bool decodeSnapshotFile(const Byte* bytes, size_t n, SnapshotData& out) {
  if (bytes == nullptr || n < kFileHeaderLen) return false;
  if (std::memcmp(bytes, "RKS1", 4) != 0) return false;
  const uint8_t ver = bytes[4];
  if (ver != 0x01 && ver != 0x02) return false;  // v1（M3）必须继续可解

  out.lastIncludedIndex = getU64(bytes + 5);
  out.lastIncludedTerm = getU64(bytes + 13);
  const uint32_t payloadLen = getU32(bytes + 21);
  const uint32_t crc = getU32(bytes + 25);
  const size_t payloadEnd = kFileHeaderLen + payloadLen;
  if (ver == 0x01) {
    if (n != payloadEnd) return false;
    out.config.clear();  // v1：未携带配置
  } else {
    if (n < payloadEnd + 8) return false;
    const uint32_t configLen = getU32(bytes + payloadEnd);
    const uint32_t configCrc = getU32(bytes + payloadEnd + 4);
    if (n != payloadEnd + 8 + configLen) return false;
    const Byte* cp = bytes + payloadEnd + 8;
    if (crc32(cp, configLen) != configCrc) return false;
    out.config.assign(cp, cp + configLen);
  }
  if (crc32(bytes + kFileHeaderLen, payloadLen) != crc) return false;
  out.payload.assign(bytes + kFileHeaderLen, bytes + kFileHeaderLen + payloadLen);
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
    installedSize_ = existing.size();  // M5.4（§8.3）：切片发送靠这个长度
  }
  // M5.4（§8.4）：若上次进程留下未完成的 .recv，用它的头部恢复续传状态。
  Index ri = kNoIndex; Term rt = kNoTerm; uint64_t rlen = 0; uint32_t rcrc = 0;
  if (readRecvTrailerLocked(ri, rt, rlen, rcrc)) {
    recvIndex_ = ri;
    recvTerm_ = rt;
    recvEnd_ = rlen;
    recvCrc_ = rcrc;
  }
}

void FileSnapshotStore::resetRecv() {  // caller holds mu_
  recvIndex_ = kNoIndex;
  recvTerm_ = kNoTerm;
  recvEnd_ = 0;
  recvCrc_ = 0;
}

// ---- M5.4（§8.4）：.recv 尾部元数据（跨进程重启的断点续传） ----
Bytes encodeRecvTrailer(Index idx, Term term, uint64_t receivedLen,
                        uint32_t crcSoFar) {
  Bytes t;
  t.reserve(kRecvTrailerLen);
  for (char c : kRecvMagic) t.push_back(static_cast<Byte>(c));
  putU64(t, idx);
  putU64(t, term);
  putU64(t, receivedLen);
  putU32(t, crcSoFar);
  return t;
}

// 把尾部写在载荷之后（offset = receivedLen）。每次收到一块后更新。
bool FileSnapshotStore::writeRecvTrailerLocked(uint64_t receivedLen,
                                              uint32_t crcSoFar) {
  if (recvIndex_ == kNoIndex) return false;
  const Bytes t = encodeRecvTrailer(recvIndex_, recvTerm_, receivedLen, crcSoFar);
  const int fd = ::open(recvPath_.c_str(), O_RDWR | O_CREAT, 0644);
  if (fd < 0) return false;
  const bool ok = writeAllAt(fd, t.data(), t.size(),
                            static_cast<off_t>(receivedLen));
  ::close(fd);
  return ok;
}

// 从 .recv 的**末尾**恢复续传状态（跨进程重启）：文件大小必须恰好 = len + 32。
bool FileSnapshotStore::readRecvTrailerLocked(Index& idx, Term& term,
                                             uint64_t& receivedLen,
                                             uint32_t& crcSoFar) {
  const int fd = ::open(recvPath_.c_str(), O_RDONLY);
  if (fd < 0) return false;
  struct stat st {};
  if (::fstat(fd, &st) != 0 || st.st_size < static_cast<off_t>(kRecvTrailerLen)) {
    ::close(fd);
    return false;
  }
  Byte t[kRecvTrailerLen];
  size_t got = 0;
  const off_t off = st.st_size - static_cast<off_t>(kRecvTrailerLen);
  while (got < sizeof(t)) {
    const ssize_t r = ::pread(fd, t + got, sizeof(t) - got,
                              off + static_cast<off_t>(got));
    if (r <= 0) { ::close(fd); return false; }
    got += static_cast<size_t>(r);
  }
  ::close(fd);
  if (std::memcmp(t, kRecvMagic, 4) != 0) return false;
  idx = getU64(t + 4);
  term = getU64(t + 12);
  receivedLen = getU64(t + 20);
  crcSoFar = getU32(t + 28);
  if (static_cast<uint64_t>(off) != receivedLen) return false;  // 尾部与长度不一致
  return true;
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
  installedSize_ = bytes.size();
  resetRecv();
  return true;
}

// M5.4（§8.3）：分块写盘 —— 两趟走 view.stream()：第一趟只算 payloadLen+crc32（不缓冲），
// 第二趟逐块写出。内存峰值 O(块)，而不是"整块 payload + 整块编码"。
// 落盘格式与 encodeSnapshotFile 逐字节一致（B 组用例比对），I14 不破。
bool FileSnapshotStore::saveStreaming(const SnapshotView& view,
                                      Index lastIncludedIndex,
                                      Term lastIncludedTerm,
                                      const Bytes& config) {
  std::lock_guard<std::mutex> lock(mu_);
  if (boundary_ != kNoIndex && lastIncludedIndex <= boundary_) return false;  // B5

  uint64_t payloadLen = 0;
  uint32_t crc = 0xFFFFFFFFu;
  Bytes buf;
  {
    std::unique_ptr<SnapshotStream> st = view.stream();
    while (st->next(buf, kSaveChunkBytes)) {
      payloadLen += buf.size();
      crc = crc32Update(crc, buf.data(), buf.size());
    }
  }
  crc ^= 0xFFFFFFFFu;

  const bool withConfig = !config.empty();
  Bytes header;
  header.reserve(kFileHeaderLen);
  header.push_back('R');
  header.push_back('K');
  header.push_back('S');
  header.push_back('1');
  header.push_back(withConfig ? 0x02 : 0x01);
  putU64(header, lastIncludedIndex);
  putU64(header, lastIncludedTerm);
  putU32(header, static_cast<uint32_t>(payloadLen));
  putU32(header, crc);

  const std::string tmp = path_ + ".tmp";
  const int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) return false;
  bool ok = writeAll(fd, header.data(), header.size());
  if (ok) {
    std::unique_ptr<SnapshotStream> st = view.stream();
    while (ok && st->next(buf, kSaveChunkBytes)) {
      ok = writeAll(fd, buf.data(), buf.size());
    }
  }
  if (ok && withConfig) {
    Bytes tail;
    putU32(tail, static_cast<uint32_t>(config.size()));
    putU32(tail, crc32(config.data(), config.size()));
    tail.insert(tail.end(), config.begin(), config.end());
    ok = writeAll(fd, tail.data(), tail.size());
  }
  ok = ok && (::fsync(fd) == 0);
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
  boundary_ = lastIncludedIndex;
  installedSize_ = header.size() + payloadLen + (withConfig ? 8 + config.size() : 0);
  resetRecv();
  return true;
}

uint64_t FileSnapshotStore::installedBytes() const {
  std::lock_guard<std::mutex> lock(mu_);
  return installedSize_;
}

// M5.4（§8.3）：按需读取已安装快照的片段（InstallSnapshot 分块发送用；峰值 O(len)）
bool FileSnapshotStore::readInstalled(uint64_t offset, uint64_t len,
                                      Bytes& out) const {
  std::lock_guard<std::mutex> lock(mu_);
  if (len == 0 || offset + len > installedSize_) return false;
  const int fd = ::open(path_.c_str(), O_RDONLY);
  if (fd < 0) return false;
  out.resize(static_cast<size_t>(len));
  size_t got = 0;
  while (got < out.size()) {
    const ssize_t r = ::pread(fd, out.data() + got, out.size() - got,
                              static_cast<off_t>(offset + got));
    if (r <= 0) { ::close(fd); return false; }
    got += static_cast<size_t>(r);
  }
  ::close(fd);
  return true;
}

uint64_t FileSnapshotStore::recvProgress() const {
  std::lock_guard<std::mutex> lock(mu_);
  return recvEnd_;
}

bool FileSnapshotStore::receiveChunk(Index lastIncludedIndex,
                                     Term lastIncludedTerm, uint64_t offset,
                                     const Bytes& data, bool done) {
  std::lock_guard<std::mutex> lock(mu_);

  // M5.4（§8.4）跨进程重启的续传：内存里没有进行中的传输时，先看 .recv 尾部的元数据。
  if (recvIndex_ == kNoIndex) {
    Index ri = kNoIndex;
    Term rt = kNoTerm;
    uint64_t rlen = 0;
    uint32_t rcrc = 0;
    if (readRecvTrailerLocked(ri, rt, rlen, rcrc)) {
      recvIndex_ = ri;
      recvTerm_ = rt;
      recvEnd_ = rlen;
      recvCrc_ = rcrc;
    }
  }

  // 新的 (index, term)（或显式重启 offset==0）-> 重开一次传输
  if (lastIncludedIndex != recvIndex_ || lastIncludedTerm != recvTerm_ ||
      offset == 0) {
    recvIndex_ = lastIncludedIndex;
    recvTerm_ = lastIncludedTerm;
    recvEnd_ = 0;
    recvCrc_ = 0;
    ::unlink(recvPath_.c_str());  // 丢掉上一份未完成的 .recv
    if (offset != 0) return false;  // ask the leader to restart at 0
  }
  // B7: a retransmitted chunk is an idempotent success, never a second append.
  if (offset + data.size() <= recvEnd_) return true;
  if (offset != recvEnd_) {  // gap / overlap: restart the transfer
    ::unlink(recvPath_.c_str());
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
  // 载荷从偏移 0 连续写；文件末尾（= 载荷末尾 + 可能的尾部元数据）必须与进度一致
  const off_t end = static_cast<off_t>(recvEnd_);
  const off_t size = ::lseek(fd, 0, SEEK_END);
  const bool freshOrExact =
      (offset == 0) || (size == end) ||
      (size == end + static_cast<off_t>(kRecvTrailerLen));  // 尾部已写过
  if (!freshOrExact) {
    ::close(fd);
    ::unlink(recvPath_.c_str());
    resetRecv();
    return false;
  }
  if (::lseek(fd, end, SEEK_SET) != end) {
    ::close(fd);
    resetRecv();
    return false;
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
  recvCrc_ = crc32Update(recvCrc_, data.data(), data.size());
  // 每块之后更新尾部元数据：下一个进程据此续传（收到哪、CRC 到哪）
  if (!writeRecvTrailerLocked(recvEnd_, recvCrc_ ^ 0xFFFFFFFFu)) {
    resetRecv();
    return false;
  }
  if (!done) return true;

  Bytes whole;
  if (!readWhole(recvPath_, whole) ||
      whole.size() != recvEnd_ + kRecvTrailerLen) {
    ::unlink(recvPath_.c_str());
    resetRecv();
    return false;
  }
  SnapshotData parsed;
  if (!decodeSnapshotFile(whole.data(), recvEnd_, parsed) ||
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
  // 安装：把尾部元数据截掉（载荷保持从 0 连续），再原子 rename —— 不需要复制整块 payload
  {
    const int tfd = ::open(recvPath_.c_str(), O_RDWR);
    if (tfd < 0 || ::ftruncate(tfd, static_cast<off_t>(recvEnd_)) != 0) {
      if (tfd >= 0) ::close(tfd);
      resetRecv();
      return false;
    }
    if (::fsync(tfd) != 0) {
      ::close(tfd);
      resetRecv();
      return false;
    }
    ::close(tfd);
  }
  if (::rename(recvPath_.c_str(), path_.c_str()) != 0) {
    resetRecv();
    return false;
  }
  fsyncDir(dir_ + "/raft");
  boundary_ = parsed.lastIncludedIndex;
  installedSize_ = recvEnd_;
  resetRecv();
  return true;
}
}  // namespace raftkv::raft

