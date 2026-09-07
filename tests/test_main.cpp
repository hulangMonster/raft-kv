#include <cstddef>
#include <cstdio>

#include "test.h"

int main() {
  size_t failed = 0;
  for (const auto& [name, fn] : raftkv_test::cases()) {
    const bool ok = fn();
    std::printf("%-4s %s\n", ok ? "PASS" : "FAIL", name.c_str());
    if (!ok) ++failed;
  }
  if (failed != 0) {
    std::printf("%zu test(s) failed\n", failed);
    return 1;
  }
  std::printf("all tests passed (%zu)\n", raftkv_test::cases().size());
  return 0;
}
