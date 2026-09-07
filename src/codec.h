#pragma once

#include <string>

#include "common.h"

namespace raftkv {

// Wire protocol (binary, big-endian):
//   request : [ver:1][op:1][keyLen:2][valLen:4][key...][value...]
//   response: [status:1][valLen:4][payload...]  (payload: GET value / error msg)
inline constexpr size_t kReqHeaderLen = 8;
inline constexpr size_t kRespHeaderLen = 5;
inline constexpr uint32_t kMaxRequestBytes = 64u * 1024u * 1024u;

struct Request {
  OpCode op = OpCode::kGet;
  std::string key;
  std::string value;  // only meaningful for PUT
};

struct Response {
  StatusCode status = StatusCode::kOk;
  std::string payload;  // GET: value; error: human-readable message
};

// Returns empty Bytes when the request cannot be encoded (key > 64KiB,
// total size > kMaxRequestBytes).
Bytes encodeRequest(const Request& req);

// Returns false when the buffer is not a well-formed request.
bool parseRequest(const Byte* data, size_t n, Request& out);

Bytes encodeResponse(const Response& resp);
bool parseResponse(const Byte* data, size_t n, Response& out);

}  // namespace raftkv
