// B-group: disk persistence tests (FileLogStore) — real temp dirs, real files.
#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include <unistd.h>

#include "raft/log_store.h"

using namespace raftkv::raft;

namespace {

std::string tempDir() {
  static uint64_t counter = 0;
  auto p = std::filesystem::temp_directory_path() /
           ("raftkv_raft_test_" + std::to_string(::getpid()) + "_" +
            std::to_string(counter++));
  std::filesystem::create_directories(p);
  return p.string();
}

LogEntry entry(Index index, Term term, const std::string& key,
               const std::string& value) {
  LogEntry e;
  e.index = index;
  e.term = term;
  e.op = raftkv::OpCode::kPut;
  e.key = key;
  e.value = value;
  e.clientId = 1;
  e.requestId = index;
  return e;
}

}  // namespace

TEST(FileLogStore, RestartRestoresMetaAndLog) {
  const std::string dir = tempDir();
  {
    FileLogStore store(dir);
    ASSERT_TRUE(store.persistMeta(2, 1));
    std::vector<LogEntry> entries;
    entries.push_back(entry(1, 1, "a", "1"));
    entries.push_back(entry(2, 2, "b", "2"));
    ASSERT_TRUE(store.append(entries));
  }
  {
    FileLogStore store(dir);
    Term term = 0;
    int votedFor = -1;
    Index lastIndex = 0;
    ASSERT_TRUE(store.load(term, votedFor, lastIndex));
    EXPECT_EQ(term, 2);
    EXPECT_EQ(votedFor, 1);
    EXPECT_EQ(lastIndex, 2);
    EXPECT_EQ(store.lastTerm(), 2);
    EXPECT_EQ(store.termAt(1), 1);
    EXPECT_EQ(store.termAt(2), 2);
  }
  std::filesystem::remove_all(dir);
}

TEST(FileLogStore, TruncatesTornTail) {
  const std::string dir = tempDir();
  {
    FileLogStore store(dir);
    std::vector<LogEntry> entries;
    entries.push_back(entry(1, 1, "a", "1"));
    ASSERT_TRUE(store.append(entries));
  }
  {
    // Simulate a crash mid-write: append garbage after the valid record.
    std::ofstream out(dir + "/raft/raft.log", std::ios::binary | std::ios::app);
    const char junk[] = "torn-tail-junk-not-a-record";
    out.write(junk, sizeof(junk) - 1);
  }
  {
    FileLogStore store(dir);
    Term term = 0;
    int votedFor = -1;
    Index lastIndex = 0;
    ASSERT_TRUE(store.load(term, votedFor, lastIndex));
    EXPECT_EQ(lastIndex, 1) << "torn tail must be truncated to the last valid record";
  }
  std::filesystem::remove_all(dir);
}
