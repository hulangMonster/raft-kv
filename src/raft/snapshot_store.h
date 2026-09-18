#pragma once

#include <cstdint>
#include <mutex>
#include <string>

#include "raft/state_machine.h"
#include "types.h"

namespace raftkv::raft {

// Snapshot payload + boundary (see m3-design.md v1.1 §4.2/§5.1).
struct SnapshotData {
  Index lastIncludedIndex = kNoIndex;
  Term lastIncludedTerm = kNoTerm;
  Bytes payload;  // StateMachine-serialized state
  // M4: 生成该快照时的集群配置（encodeClusterConfig 输出）。
  // RKS1 v1 快照没有此段 -> 空（视为"未携带配置"，回退 seed/日志，见 §5.1）
  Bytes config;
};

// Snapshot persistence seam. Implementations serialize internal state.
// save()/receiveChunk() do disk I/O and must NOT be called while holding
// RaftNode::mu_ (lock discipline L8).
class SnapshotStore {
 public:
  virtual ~SnapshotStore() = default;

  // No snapshot / corrupt (magic/version/CRC) -> false (caller falls back to
  // a full log replay).
  virtual bool load(SnapshotData& out) = 0;

  // Atomic persist: tmp -> fsync -> rename -> fsync(dir). true == durable.
  // MUST refuse (return false) to overwrite a snapshot at or above
  // data.lastIncludedIndex: a save() racing an InstallSnapshot could otherwise
  // move the durable boundary backwards.
  virtual bool save(const SnapshotData& data) = 0;

  // InstallSnapshot receive path (Follower): append `data` at `offset`; when
  // done==true validate and atomically install as the current snapshot.
  // Must be idempotent for retransmitted chunks (offset already covered) and
  // safe under concurrent use (serialise internally).
  virtual bool receiveChunk(Index lastIncludedIndex, Term lastIncludedTerm,
                            uint64_t offset, const Bytes& data, bool done) = 0;

  // ---- M5.4（§8.3 流式序列化 / §8.4 断点续传） ----
  // 流式落盘：两趟走 view.stream()（先算 payloadLen/crc32，再分块写），
  // 峰值 O(块) 而不是 O(状态)。默认实现退化为 serialize()+save()（内存适配器沿用它）。
  virtual bool saveStreaming(const SnapshotView& view, Index lastIncludedIndex,
                             Term lastIncludedTerm, const Bytes& config);
  // 已安装快照的**编码后**字节数（InstallSnapshot 分块发送的总长度）
  virtual uint64_t installedBytes() const { return 0; }
  // 读取已安装快照的 [offset, offset+len) 片段（不再常驻整块编码缓冲）
  virtual bool readInstalled(uint64_t offset, uint64_t len, Bytes& out) const {
    (void)offset;
    (void)len;
    (void)out;
    return false;
  }
  // 接收进度（载荷字节数）：跨进程重启时从 .recv 头部恢复，用于告诉 leader 从哪里续传
  virtual uint64_t recvProgress() const { return 0; }
};

// In-memory adapter for deterministic unit tests (no I/O).
class MemorySnapshotStore : public SnapshotStore {
 public:
  bool load(SnapshotData& out) override;
  bool save(const SnapshotData& data) override;
  bool receiveChunk(Index lastIncludedIndex, Term lastIncludedTerm,
                    uint64_t offset, const Bytes& data, bool done) override;

  // Test helper: force a store state without going through the RaftNode path.
  bool installed() const {
    std::lock_guard<std::mutex> lock(mu_);
    return has_;
  }

  // M5.4（§8.3/§8.4）：内存适配器把"编码后的整块"当作安装物保留（测试用）
  bool saveStreaming(const SnapshotView& view, Index lastIncludedIndex,
                     Term lastIncludedTerm, const Bytes& config) override;
  uint64_t installedBytes() const override;
  bool readInstalled(uint64_t offset, uint64_t len, Bytes& out) const override;
  uint64_t recvProgress() const override;

 private:
  mutable std::mutex mu_;
  bool has_ = false;
  SnapshotData data_;
  Bytes encoded_;   // encodeSnapshotFile(data_)：供分块发送/切片读取
  Bytes recv_;  // in-progress chunk buffer
  // Transfer bookkeeping (B7): only offset == recvEnd_ is appended; a chunk that
  // is already fully covered is an idempotent duplicate.
  Index recvIndex_ = kNoIndex;
  Term recvTerm_ = kNoTerm;
  uint64_t recvEnd_ = 0;
};

// File-backed adapter: `<dir>/raft/snapshot.dat` (+ `.recv` while receiving).
// Real implementation lands in M3.4; declared now so B-group tests can link.
class FileSnapshotStore : public SnapshotStore {
 public:
  explicit FileSnapshotStore(std::string dir);

  bool load(SnapshotData& out) override;
  bool save(const SnapshotData& data) override;
  bool receiveChunk(Index lastIncludedIndex, Term lastIncludedTerm,
                    uint64_t offset, const Bytes& data, bool done) override;

  // M5.4（§8.3/§8.4）
  bool saveStreaming(const SnapshotView& view, Index lastIncludedIndex,
                     Term lastIncludedTerm, const Bytes& config) override;
  uint64_t installedBytes() const override;
  bool readInstalled(uint64_t offset, uint64_t len, Bytes& out) const override;
  uint64_t recvProgress() const override;

 private:
  std::string dir_;
  std::string path_;      // <dir>/raft/snapshot.dat
  std::string recvPath_;  // <dir>/raft/snapshot.dat.recv

 private:
  void resetRecv();  // caller holds mu_
  // M5.4（§8.4）：把 (index, term, receivedLen, crcSoFar) 落到 .recv **尾部**，
  // 让**下一个进程**能接着传；载荷因此保持从偏移 0 连续，安装时 ftruncate+rename 即可。
  bool writeRecvTrailerLocked(uint64_t receivedLen, uint32_t crcSoFar);
  bool readRecvTrailerLocked(Index& idx, Term& term, uint64_t& receivedLen,
                             uint32_t& crcSoFar);

  mutable std::mutex mu_;
  Index boundary_ = kNoIndex;  // boundary currently stored in snapshot.dat
  uint64_t installedSize_ = 0;  // snapshot.dat 的字节数（0 = 无）
  Index recvIndex_ = kNoIndex;
  Term recvTerm_ = kNoTerm;
  uint64_t recvEnd_ = 0;   // 已收到的**载荷**字节数（不含 .recv 头）
  uint32_t recvCrc_ = 0;   // 已收到载荷的增量 CRC32（跨重启可校验）
};

// ---- Snapshot file codec (m3-design.md v1.1 §5.1) ----
// magic(4)="RKS1" | version(1)=1 | lastIncludedIndex(8) | lastIncludedTerm(8)
// | payloadLen(4) | crc32(4, over payload) | payload[payloadLen]
Bytes encodeSnapshotFile(const SnapshotData& data);
bool decodeSnapshotFile(const Byte* bytes, size_t n, SnapshotData& out);

}  // namespace raftkv::raft
