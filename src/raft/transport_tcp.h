#pragma once

#include <mutex>
#include <string>
#include <unordered_map>

#include "message.h"
#include "transport.h"

namespace raftkv::raft {

// Blocking (synchronous) TCP transport over long-lived per-peer connections.
//
// Each send is one write-request / read-reply round trip with a timeout, and
// sends are serialized by a single transport mutex. Fine for a small demo
// cluster; making sends asynchronous / per-peer is a listed M5 optimization.
class TcpTransport : public Transport {
 public:
  // peers: node id -> "host:port".
  TcpTransport(std::unordered_map<int, std::string> peers,
               uint64_t rpcTimeoutMs = 100);
  ~TcpTransport() override;

  void sendRequestVote(int peerId, const RequestVoteArgs& args,
                       VoteCb cb) override;
  void sendAppendEntries(int peerId, const AppendEntriesArgs& args,
                         AppendCb cb) override;
  void sendInstallSnapshot(int peerId, const InstallSnapshotArgs& args,
                           InstallCb cb) override;
  void sendReadProbe(int peerId, const ReadProbeArgs& args,
                     ReadProbeCb cb) override;
  // M4：地址簿动态更新（L10：只在锁外作业中调用）
  void addPeer(int id, const std::string& addr) override;
  void removePeer(int id) override;

 private:
  bool roundTrip(int peerId, MsgType reqType, const Bytes& reqPayload,
                 MsgType& replyType, Bytes& replyPayload, uint64_t timeoutMs);
  int getConnection(int peerId);
  void dropConnection(int peerId);

  std::unordered_map<int, std::string> peers_;
  uint64_t rpcTimeoutMs_;
  uint64_t installTimeoutMs_;  // longer: the peer does disk I/O on install
  std::mutex mu_;
  std::unordered_map<int, int> conns_;
};

}  // namespace raftkv::raft
