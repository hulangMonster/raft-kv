#include "codec.h"

#include <limits>

namespace raftkv {

Bytes encodeRequest(const Request& req) {
  if (req.key.size() > std::numeric_limits<uint16_t>::max()) return {};
  if (req.value.size() > std::numeric_limits<uint32_t>::max()) return {};
  if (kReqHeaderLen + req.key.size() + req.value.size() > kMaxRequestBytes) {
    return {};
  }

  Bytes out;
  out.reserve(kReqHeaderLen + req.key.size() + req.value.size());
  out.push_back(kVersion);
  out.push_back(static_cast<Byte>(req.op));
  putU16(out, static_cast<uint16_t>(req.key.size()));
  putU32(out, static_cast<uint32_t>(req.value.size()));
  out.insert(out.end(), req.key.begin(), req.key.end());
  out.insert(out.end(), req.value.begin(), req.value.end());
  return out;
}

bool parseRequest(const Byte* data, size_t n, Request& out) {
  if (n < kReqHeaderLen || n > kMaxRequestBytes) return false;
  if (data[0] != kVersion) return false;

  const uint8_t opRaw = data[1];
  if (opRaw != static_cast<uint8_t>(OpCode::kPut) &&
      opRaw != static_cast<uint8_t>(OpCode::kGet) &&
      opRaw != static_cast<uint8_t>(OpCode::kDel)) {
    return false;
  }

  const size_t keyLen = getU16(data + 2);
  const size_t valLen = getU32(data + 4);
  if (keyLen + valLen + kReqHeaderLen != n) return false;

  out.op = static_cast<OpCode>(opRaw);
  out.key.assign(reinterpret_cast<const char*>(data + kReqHeaderLen), keyLen);
  out.value.assign(
      reinterpret_cast<const char*>(data + kReqHeaderLen + keyLen), valLen);
  return true;
}

Bytes encodeResponse(const Response& resp) {
  Bytes out;
  out.reserve(kRespHeaderLen + resp.payload.size());
  out.push_back(static_cast<Byte>(resp.status));
  putU32(out, static_cast<uint32_t>(resp.payload.size()));
  out.insert(out.end(), resp.payload.begin(), resp.payload.end());
  return out;
}

bool parseResponse(const Byte* data, size_t n, Response& out) {
  if (n < kRespHeaderLen) return false;
  const uint8_t statusRaw = data[0];
  if (statusRaw > static_cast<uint8_t>(StatusCode::kErr)) return false;

  const size_t payloadLen = getU32(data + 1);
  if (kRespHeaderLen + payloadLen != n) return false;

  out.status = static_cast<StatusCode>(statusRaw);
  out.payload.assign(
      reinterpret_cast<const char*>(data + kRespHeaderLen), payloadLen);
  return true;
}

}  // namespace raftkv
