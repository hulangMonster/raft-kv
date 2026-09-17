#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "types.h"

namespace raftkv::raft {

// Durable log + metadata (term / votedFor). This is the single source of
// persistence truth in M2 (decision D1). Implementations must serialize
// internal state (file I/O is the only blocking operation they may do).
class LogStore {
 public:
  virtual ~LogStore() = default;

  // Startup recovery: read term / votedFor / last log index, truncate a torn
  // tail. Returns false on unrecoverable I/O error.
  virtual bool load(Term& term, int& votedFor, Index& lastIndex) = 0;

  // Must return true only after term/votedFor are durable (fsync'd).
  virtual bool persistMeta(Term term, int votedFor) = 0;

  // Append entries; returns true only after they are durable (fsync'd).
  virtual bool append(const std::vector<LogEntry>& entries) = 0;

  // M5 group commit: write entries WITHOUT fsync, then flush a whole batch
  // once with sync(). appendNoSync() must still make the entries visible to
  // slice()/lastIndex() and must be serialized by the caller (RaftNode::mu_).
  // sync() may be called WITHOUT RaftNode::mu_ (it is the only operation that
  // runs outside it), so every implementation must make sync() safe against
  // concurrent fd/structural mutation internally.
  virtual bool appendNoSync(const std::vector<LogEntry>& entries) = 0;
  virtual bool sync() = 0;

  // Drop [fromIndex, lastIndex] (conflict overwrite).
  // 含 fsync 的截断：只允许在**不持** RaftNode::mu_ 时调用（I9）。
  virtual bool truncateSuffix(Index fromIndex) = 0;
  // M5.2（I9）：只做 ftruncate + 内存截断、不做 fsync 的变体，供持 mu_ 的路径使用
  // （ftruncate 与 write 同属 page-cache/metadata 变更，不算 I9 禁止的 fsync/网络 IO）。
  // durability 由同一批的锁外 log_.sync() 覆盖（fsync 会一并持久化 size 变更）。
  // 只做 ftruncate/内存截断、不 fsync 的变体，供持 mu_ 的冲突回滚路径使用（I9）。
  // 调用方必须在同一批里随后 sync()；否则崩溃后变短的日志可能复活。
  // 默认实现转调 truncateSuffix()，因此**凡是 truncateSuffix() 会 fsync 的实现
  // 都必须同时覆写本方法**（FileLogStore 已覆写；M5.A9 会抓住漏网者）。
  virtual bool truncateSuffixNoSync(Index fromIndex) {
    return truncateSuffix(fromIndex);
  }

  virtual std::vector<LogEntry> slice(Index from, size_t maxEntries,
                                      size_t maxBytes) const = 0;

  virtual Index lastIndex() const = 0;
  virtual Term lastTerm() const = 0;              // kNoTerm when empty
  virtual Term termAt(Index index) const = 0;     // kNoTerm when out of range

  // ---- M3: prefix compaction (m3-design.md v1.1 §4.3, D3) ----
  virtual void setBoundary(Index lastIncludedIndex, Term lastIncludedTerm) = 0;
  virtual bool compact(Index upTo, Term termAtUpTo) = 0;
  virtual Index firstIndex() const = 0;  // lastIncludedIndex + 1
  virtual Index lastIncludedIndex() const = 0;
  virtual Term lastIncludedTerm() const = 0;
};

// In-memory adapter used by deterministic unit tests (no I/O).
class MemoryLogStore : public LogStore {
 public:
  bool load(Term& term, int& votedFor, Index& lastIndex) override;
  bool persistMeta(Term term, int votedFor) override;
  bool append(const std::vector<LogEntry>& entries) override;
  bool appendNoSync(const std::vector<LogEntry>& entries) override;
  bool sync() override;
  bool truncateSuffix(Index fromIndex) override;
  bool truncateSuffixNoSync(Index fromIndex) override;
  std::vector<LogEntry> slice(Index from, size_t maxEntries,
                              size_t maxBytes) const override;
  Index lastIndex() const override;
  Term lastTerm() const override;
  Term termAt(Index index) const override;

  // M3 (stubs in Phase #2; real logic in M3.1)
  void setBoundary(Index lastIncludedIndex, Term lastIncludedTerm) override;
  bool compact(Index upTo, Term termAtUpTo) override;
  Index firstIndex() const override;
  Index lastIncludedIndex() const override;
  Term lastIncludedTerm() const override;

  // Test helper: the whole log, entries_[i-1] == index i.
  const std::vector<LogEntry>& all() const { return entries_; }

 private:
  Term term_ = kNoTerm;
  int votedFor_ = -1;
  std::vector<LogEntry> entries_;
  // M3 (D3): compaction boundary. firstIndex() == lastIncluded_ + 1.
  Index lastIncluded_ = kNoIndex;
  Term lastIncludedTerm_ = kNoTerm;
};

// File-backed adapter: `<dir>/raft/meta.dat` + `<dir>/raft/raft.log`.
// meta.dat is written atomically (tmp + rename); raft.log uses CRC-framed
// records and a torn tail is truncated on load(). Keeps an in-memory copy of
// the log (rebuilt at load) for fast slice()/termAt().
class FileLogStore : public LogStore {
 public:
  explicit FileLogStore(std::string dir);
  ~FileLogStore() override;

  bool load(Term& term, int& votedFor, Index& lastIndex) override;
  bool persistMeta(Term term, int votedFor) override;
  bool append(const std::vector<LogEntry>& entries) override;
  bool appendNoSync(const std::vector<LogEntry>& entries) override;
  bool sync() override;
  bool truncateSuffix(Index fromIndex) override;
  bool truncateSuffixNoSync(Index fromIndex) override;
  std::vector<LogEntry> slice(Index from, size_t maxEntries,
                              size_t maxBytes) const override;
  Index lastIndex() const override;
  Term lastTerm() const override;
  Term termAt(Index index) const override;

  // M3 (stubs in Phase #2; real logic in M3.1/M3.4)
  void setBoundary(Index lastIncludedIndex, Term lastIncludedTerm) override;
  bool compact(Index upTo, Term termAtUpTo) override;
  Index firstIndex() const override;
  Index lastIncludedIndex() const override;
  Term lastIncludedTerm() const override;

 private:
  std::string dir_;
  std::string metaPath_;
  std::string logPath_;
  int logFd_ = -1;
  // B4 + M5.2: **所有**触碰内部状态的入口都持这把锁，包括只读访问器
  // （lastIndex/lastTerm/termAt/slice/firstIndex/lastIncluded*）。
  // 原因：M5.2 把 compact() 移到 RaftNode::mu_ 之外（I9：锁内不做 fsync），原先
  // "只读访问器由调用方的 RaftNode::mu_ 串行化"这一 M2 约定就被打破 ——
  // compact 在 store 锁下改 entries_/offsetOf_/lastIncluded_ 时，别的线程仍在
  // mu_ 下读它们 → 数据竞争 → UB/SIGSEGV（ASan 实测栈顶 termAt/slice）。
  // 用 recursive_mutex：内部实现之间互相调用（compact→lastIndex 等）不会自死锁。
  mutable std::recursive_mutex mu_;
  // 以下三个是公共入口的内部实现：调用方必须已持有 mu_。
  bool appendNoSyncLocked(const std::vector<LogEntry>& entries);
  bool syncLocked();
  bool truncateSuffixLocked(Index fromIndex, bool sync);

  Term term_ = kNoTerm;
  int votedFor_ = -1;
  std::vector<LogEntry> entries_;                 // entries_[i-1] == index i
  std::unordered_map<Index, int64_t> offsetOf_;   // index -> record start offset
  // M3 (D3): compaction boundary. firstIndex() == lastIncluded_ + 1.
  Index lastIncluded_ = kNoIndex;
  Term lastIncludedTerm_ = kNoTerm;
};

}  // namespace raftkv::raft
