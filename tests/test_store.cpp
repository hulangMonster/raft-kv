#include <filesystem>
#include <fstream>
#include <string>

#include <unistd.h>

#include "store.h"
#include "test.h"

namespace {

std::string makeTempDir() {
  static uint64_t counter = 0;
  const auto dir = std::filesystem::temp_directory_path() /
                   ("raftkv_test_store_" + std::to_string(::getpid()) + "_" +
                    std::to_string(counter++));
  std::filesystem::create_directories(dir);
  return dir.string();
}

}  // namespace

RAFTKV_TEST(store_put_get) {
  const std::string dir = makeTempDir();
  raftkv::Store store(dir, /*sync=*/true);
  const auto r = store.put("name", "raft");
  if (!r.ok) return false;
  const auto g = store.get("name");
  if (!g.ok || !g.found || g.value != "raft") return false;
  if (store.size() != 1) return false;
  return true;
}

RAFTKV_TEST(store_overwrite_and_del) {
  const std::string dir = makeTempDir();
  raftkv::Store store(dir, /*sync=*/true);
  if (!store.put("k", "v1").ok) return false;
  if (!store.put("k", "v2").ok) return false;
  if (store.get("k").value != "v2") return false;

  if (!store.del("k").ok) return false;
  const auto g = store.get("k");
  if (g.found) return false;
  if (store.size() != 0) return false;

  // Deleting a missing key is a no-op that still succeeds.
  if (!store.del("absent").ok) return false;
  return true;
}

RAFTKV_TEST(store_binary_value) {
  const std::string dir = makeTempDir();
  raftkv::Store store(dir, /*sync=*/true);
  const std::string binary("a\0b\xff\x01", 5);
  if (!store.put("bin", binary).ok) return false;
  const auto g = store.get("bin");
  return g.found && g.value == binary;
}

RAFTKV_TEST(store_replay_after_reopen) {
  const std::string dir = makeTempDir();
  {
    raftkv::Store store(dir, /*sync=*/true);
    if (!store.put("alpha", "1").ok) return false;
    if (!store.put("beta", "2").ok) return false;
    if (!store.del("alpha").ok) return false;
  }  // store destroyed

  // Reopen from the same WAL: replay must reproduce the final state.
  raftkv::Store reopened(dir, /*sync=*/true);
  if (reopened.get("alpha").found) return false;
  const auto b = reopened.get("beta");
  if (!b.found || b.value != "2") return false;
  if (reopened.size() != 1) return false;
  return true;
}

RAFTKV_TEST(store_truncates_torn_tail) {
  const std::string dir = makeTempDir();
  {
    raftkv::Store store(dir, /*sync=*/true);
    if (!store.put("keep", "value").ok) return false;
  }

  // Simulate a crash mid-write: append garbage that is not a valid record.
  {
    std::ofstream out(dir + "/raftkv.wal", std::ios::binary | std::ios::app);
    const char junk[] = "not-a-valid-record-tail";
    out.write(junk, sizeof(junk) - 1);
  }

  raftkv::Store reopened(dir, /*sync=*/true);
  const auto g = reopened.get("keep");
  if (!g.found || g.value != "value") return false;
  if (reopened.size() != 1) return false;
  return true;
}
