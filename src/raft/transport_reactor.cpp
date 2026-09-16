// M5.3：TransportReactor 实现（帧编解码复用 M2 的 encodeFrame/decodeFrame）。
#include "raft/transport_reactor.h"

#include <utility>

#include "raft/message.h"

namespace raftkv::raft {

TransportReactor::TransportReactor(
    const std::unordered_map<int, std::string>& peers, uint64_t rpcTimeoutMs)
    : timeoutMs_(rpcTimeoutMs == 0 ? 30 : rpcTimeoutMs) {
  reactor_.start();
  for (const auto& kv : peers) reactor_.addPeer(kv.first, kv.second);
}

TransportReactor::~TransportReactor() { stop(); }

void TransportReactor::stop() { reactor_.stop(); }

size_t TransportReactor::inflight() const { return reactor_.inflight(); }

void TransportReactor::sendFrame(
    int peerId, MsgType type, const Bytes& payload, MsgType expect,
    std::function<void(const Byte*, size_t)> onReply) {
  const Bytes frame = encodeFrame(type, payload);
  // 回调在 reactor 线程执行：只做解码 + 调用上层回调（上层自己取 mu_）。
  reactor_.send(peerId, frame, timeoutMs_,
                [expect, onReply = std::move(onReply)](const Bytes& replyFrame,
                                                       bool ok) {
                  if (!ok) return;  // 超时/连接错误：不触发回调
                  MsgType replyType = MsgType::kClientReply;
                  Bytes replyPayload;
                  if (!decodeFrame(replyFrame.data(), replyFrame.size(),
                                   replyType, replyPayload)) {
                    return;
                  }
                  if (replyType != expect) return;
                  onReply(replyPayload.data(), replyPayload.size());
                });
}

void TransportReactor::sendRequestVote(int peerId, const RequestVoteArgs& args,
                                       VoteCb cb) {
  sendFrame(peerId, MsgType::kRequestVote, encodeRequestVote(args),
            MsgType::kRequestVoteReply,
            [cb = std::move(cb)](const Byte* d, size_t n) {
              RequestVoteReply r;
              if (decodeRequestVoteReply(d, n, r)) cb(r);
            });
}

void TransportReactor::sendAppendEntries(int peerId,
                                         const AppendEntriesArgs& args,
                                         AppendCb cb) {
  sendFrame(peerId, MsgType::kAppendEntries, encodeAppendEntries(args),
            MsgType::kAppendEntriesReply,
            [cb = std::move(cb)](const Byte* d, size_t n) {
              AppendEntriesReply r;
              if (decodeAppendEntriesReply(d, n, r)) cb(r);
            });
}

void TransportReactor::sendInstallSnapshot(int peerId,
                                           const InstallSnapshotArgs& args,
                                           InstallCb cb) {
  sendFrame(peerId, MsgType::kInstallSnapshot, encodeInstallSnapshot(args),
            MsgType::kInstallSnapshotReply,
            [cb = std::move(cb)](const Byte* d, size_t n) {
              InstallSnapshotReply r;
              if (decodeInstallSnapshotReply(d, n, r)) cb(r);
            });
}

void TransportReactor::sendReadProbe(int peerId, const ReadProbeArgs& args,
                                     ReadProbeCb cb) {
  sendFrame(peerId, MsgType::kReadProbe, encodeReadProbe(args),
            MsgType::kReadProbeReply,
            [cb = std::move(cb)](const Byte* d, size_t n) {
              ReadProbeReply r;
              if (decodeReadProbeReply(d, n, r)) cb(r);
            });
}

void TransportReactor::addPeer(int id, const std::string& addr) {
  reactor_.addPeer(id, addr);
}

void TransportReactor::removePeer(int id) { reactor_.removePeer(id); }

}  // namespace raftkv::raft
