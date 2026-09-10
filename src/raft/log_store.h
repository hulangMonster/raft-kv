#pragma once

#include <cstdint>
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

  // Drop [fromIndex, lastIndex] (conflict overwrite).
  virtual bool truncateSuffix(Index fromIndex) = 0;

  virtual std::vector<LogEntry> slice(Index from, size_t maxEntries,
                                      size_t maxBytes) const = 0;

  virtual Index lastIndex() const = 0;
  virtual Term lastTerm() const = 0;              // kNoTerm when empty
  virtual Term termAt(Index index) const = 0;     // kNoTerm when out of range
};

// In-memory adapter used by deterministic unit tests (no I/O).
class MemoryLogStore : public LogStore {
 public:
  bool load(Term& term, int& votedFor, Index& lastIndex) override;
  bool persistMeta(Term term, int votedFor) override;
  bool append(const std::vector<LogEntry>& entries) override;
  bool truncateSuffix(Index fromIndex) override;
  std::vector<LogEntry> slice(Index from, size_t maxEntries,
                              size_t maxBytes) const override;
  Index lastIndex() const override;
  Term lastTerm() const override;
  Term termAt(Index index) const override;

  // Test helper: the whole log, entries_[i-1] == index i.
  const std::vector<LogEntry>& all() const { return entries_; }

 private:
  Term term_ = kNoTerm;
  int votedFor_ = -1;
  std::vector<LogEntry> entries_;
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
  bool truncateSuffix(Index fromIndex) override;
  std::vector<LogEntry> slice(Index from, size_t maxEntries,
                              size_t maxBytes) const override;
  Index lastIndex() const override;
  Term lastTerm() const override;
  Term termAt(Index index) const override;

 private:
  std::string dir_;
  std::string metaPath_;
  std::string logPath_;
  int logFd_ = -1;

  Term term_ = kNoTerm;
  int votedFor_ = -1;
  std::vector<LogEntry> entries_;                 // entries_[i-1] == index i
  std::unordered_map<Index, int64_t> offsetOf_;   // index -> record start offset
};

}  // namespace raftkv::raft
