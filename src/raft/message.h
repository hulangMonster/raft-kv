#pragma once

#include "common.h"
#include "types.h"

namespace raftkv::raft {

// Frame: [len:4][type:1][payload...]  where len = 1 + payload.size().
// Types 1-4 are node-to-node RPC; 10/11 are the raft cluster client protocol.
enum class MsgType : uint8_t {
  kRequestVote = 1,
  kRequestVoteReply = 2,
  kAppendEntries = 3,
  kAppendEntriesReply = 4,
  kClientRequest = 10,
  kClientReply = 11,
  kStatusRequest = 12,
};

Bytes encodeFrame(MsgType type, const Bytes& payload);
bool decodeFrame(const Byte* data, size_t n, MsgType& type, Bytes& payload);

Bytes encodeRequestVote(const RequestVoteArgs& args);
bool decodeRequestVote(const Byte* data, size_t n, RequestVoteArgs& out);

Bytes encodeRequestVoteReply(const RequestVoteReply& reply);
bool decodeRequestVoteReply(const Byte* data, size_t n, RequestVoteReply& out);

Bytes encodeAppendEntries(const AppendEntriesArgs& args);
bool decodeAppendEntries(const Byte* data, size_t n, AppendEntriesArgs& out);

Bytes encodeAppendEntriesReply(const AppendEntriesReply& reply);
bool decodeAppendEntriesReply(const Byte* data, size_t n, AppendEntriesReply& out);

Bytes encodeClientRequest(const ClientRequest& req);
bool decodeClientRequest(const Byte* data, size_t n, ClientRequest& out);

Bytes encodeClientReply(const ClientReply& reply);
bool decodeClientReply(const Byte* data, size_t n, ClientReply& out);

}  // namespace raftkv::raft
