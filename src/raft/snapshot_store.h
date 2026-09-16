#pragma once

#include <cstdint>
#include <mutex>
#include <string>

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

 private:
  mutable std::mutex mu_;
  bool has_ = false;
  SnapshotData data_;
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

 private:
  void resetRecv();  // caller holds mu_

  std::string dir_;
  std::string path_;      // <dir>/raft/snapshot.dat
  std::string recvPath_;  // <dir>/raft/snapshot.dat.recv

  mutable std::mutex mu_;
  Index boundary_ = kNoIndex;  // boundary currently stored in snapshot.dat
  Index recvIndex_ = kNoIndex;
  Term recvTerm_ = kNoTerm;
  uint64_t recvEnd_ = 0;
};

// ---- Snapshot file codec (m3-design.md v1.1 §5.1) ----
// magic(4)="RKS1" | version(1)=1 | lastIncludedIndex(8) | lastIncludedTerm(8)
// | payloadLen(4) | crc32(4, over payload) | payload[payloadLen]
Bytes encodeSnapshotFile(const SnapshotData& data);
bool decodeSnapshotFile(const Byte* bytes, size_t n, SnapshotData& out);

}  // namespace raftkv::raft
