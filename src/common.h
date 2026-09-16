#pragma once

#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>

namespace raftkv {

using Byte = uint8_t;
using Bytes = std::vector<Byte>;

inline constexpr uint8_t kVersion = 0x01;

enum class OpCode : uint8_t {
  kPut = 1,
  kGet = 2,
  kDel = 3,
  // M4: Raft 配置条目（仅 raft 层产生/消费；M1 协议与 codec 白名单不含它，
  // 因此客户端无法伪造配置条目 —— 见 m4-prerequisites.md §7.1 P3）
  kConfig = 4,
};

enum class StatusCode : uint8_t {
  kOk = 0,
  kNotFound = 1,
  kErr = 2,
};

// ---- tiny logging helpers (stderr) ----
inline void logInfo(const std::string& msg) {
  std::fprintf(stderr, "[raftkv] [info] %s\n", msg.c_str());
}
inline void logError(const std::string& msg) {
  std::fprintf(stderr, "[raftkv] [error] %s\n", msg.c_str());
}

// ---- big-endian encode/decode helpers ----
inline void putU16(Bytes& out, uint16_t v) {
  out.push_back(static_cast<Byte>((v >> 8) & 0xff));
  out.push_back(static_cast<Byte>(v & 0xff));
}
inline void putU32(Bytes& out, uint32_t v) {
  out.push_back(static_cast<Byte>((v >> 24) & 0xff));
  out.push_back(static_cast<Byte>((v >> 16) & 0xff));
  out.push_back(static_cast<Byte>((v >> 8) & 0xff));
  out.push_back(static_cast<Byte>(v & 0xff));
}
inline uint16_t getU16(const Byte* p) {
  return static_cast<uint16_t>((static_cast<uint16_t>(p[0]) << 8) | p[1]);
}
inline uint32_t getU32(const Byte* p) {
  return (static_cast<uint32_t>(p[0]) << 24) |
         (static_cast<uint32_t>(p[1]) << 16) |
         (static_cast<uint32_t>(p[2]) << 8) |
         static_cast<uint32_t>(p[3]);
}

}  // namespace raftkv
