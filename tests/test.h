#pragma once

#include <functional>
#include <string>
#include <utility>
#include <vector>

namespace raftkv_test {

inline std::vector<std::pair<std::string, std::function<bool()>>>& cases() {
  static std::vector<std::pair<std::string, std::function<bool()>>> c;
  return c;
}

}  // namespace raftkv_test

#define RAFTKV_TEST(NAME)                                          \
  static bool NAME();                                              \
  [[maybe_unused]] static const bool raftkv_reg_##NAME =           \
      (raftkv_test::cases().emplace_back(#NAME, NAME), true);      \
  static bool NAME()
