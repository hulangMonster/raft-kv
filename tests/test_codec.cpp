#include <cstdint>
#include <string>

#include "codec.h"
#include "common.h"
#include "test.h"

namespace {

using raftkv::Bytes;
using raftkv::OpCode;
using raftkv::StatusCode;

bool checkRequest(const raftkv::Request& in) {
  const Bytes frame = raftkv::encodeRequest(in);
  if (frame.empty()) return false;
  raftkv::Request out;
  if (!raftkv::parseRequest(frame.data(), frame.size(), out)) return false;
  return out.op == in.op && out.key == in.key && out.value == in.value;
}

bool checkResponse(const raftkv::Response& in) {
  const Bytes frame = raftkv::encodeResponse(in);
  raftkv::Response out;
  if (!raftkv::parseResponse(frame.data(), frame.size(), out)) return false;
  return out.status == in.status && out.payload == in.payload;
}

}  // namespace

RAFTKV_TEST(codec_put_roundtrip) {
  raftkv::Request req;
  req.op = OpCode::kPut;
  req.key = "user:42";
  req.value = "hello \x01\x02\xff";
  return checkRequest(req);
}

RAFTKV_TEST(codec_get_del_roundtrip) {
  raftkv::Request get;
  get.op = OpCode::kGet;
  get.key = "user:42";
  if (!checkRequest(get)) return false;

  raftkv::Request del;
  del.op = OpCode::kDel;
  del.key = "user:42";
  return checkRequest(del);
}

RAFTKV_TEST(codec_long_key_value) {
  raftkv::Request req;
  req.op = OpCode::kPut;
  req.key.assign(1000, 'k');  // key > 255 bytes exercises 16-bit length
  req.value.assign(65536 + 17, 'v');  // value > 64KiB exercises 32-bit length
  return checkRequest(req);
}

RAFTKV_TEST(codec_empty_value) {
  raftkv::Request req;
  req.op = OpCode::kPut;
  req.key = "empty";
  req.value.clear();
  return checkRequest(req);
}

RAFTKV_TEST(codec_rejects_truncated_frame) {
  const raftkv::Request req{OpCode::kGet, "k", ""};
  const Bytes frame = raftkv::encodeRequest(req);
  if (frame.size() <= raftkv::kReqHeaderLen) return false;
  raftkv::Request out;
  // Cut in the middle of the header.
  if (raftkv::parseRequest(frame.data(), raftkv::kReqHeaderLen - 2, out)) {
    return false;
  }
  // Valid header but missing the body.
  if (raftkv::parseRequest(frame.data(), raftkv::kReqHeaderLen, out)) {
    return false;
  }
  return true;
}

RAFTKV_TEST(codec_rejects_bad_op_and_version) {
  Bytes frame = raftkv::encodeRequest({OpCode::kGet, "k", ""});
  frame[1] = 0x7f;  // unknown op
  raftkv::Request out;
  if (raftkv::parseRequest(frame.data(), frame.size(), out)) return false;

  frame = raftkv::encodeRequest({OpCode::kGet, "k", ""});
  frame[0] = 0x99;  // unknown version
  return !raftkv::parseRequest(frame.data(), frame.size(), out);
}

RAFTKV_TEST(codec_response_roundtrip) {
  return checkResponse({StatusCode::kOk, "v"}) &&
         checkResponse({StatusCode::kNotFound, ""}) &&
         checkResponse({StatusCode::kErr, "something went wrong"});
}

RAFTKV_TEST(codec_rejects_key_too_long) {
  raftkv::Request req;
  req.op = OpCode::kPut;
  req.key.assign(static_cast<size_t>(UINT16_MAX) + 1, 'k');
  const Bytes frame = raftkv::encodeRequest(req);
  return frame.empty();
}
