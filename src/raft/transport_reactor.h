#pragma once

// M5.3（设计 §7）：以既有 `Transport` 接口实现的异步引擎。
//
// 与 `TcpTransport` 的差异（接口签名不变、语义变化 —— 设计 §7.2）：
//   * `sendX()` **入队即返回**（不再在调用线程上阻塞一个 RPC 往返）；
//   * 完成回调在 **reactor 线程**触发（`RaftNode` 的所有 reply 回调自己取 mu_，因此兼容）；
//   * 每 peer 独立连接（长连接复用，不再"每次 RPC 建连"），一个慢/挂 peer 不影响其它 peer；
//   * 超时或连接错误 -> 丢弃该请求、不触发回调（与旧同步实现的"超时即失败"一致）；
//   * `removePeer()` 丢弃该 peer 的在途回调（A6 守门）；`stop()` 后不再触发任何回调。
//
// `TcpTransport`（同步阻塞版）保留为对照引擎与回退路径（`--transport=sync`）。

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>

#include "raft/message.h"  // MsgType（帧类型，用于私有 sendFrame）
#include "raft/reactor.h"
#include "raft/transport.h"

namespace raftkv::raft {

class TransportReactor : public Transport {
 public:
  TransportReactor(const std::unordered_map<int, std::string>& peers,
                   uint64_t rpcTimeoutMs);
  ~TransportReactor() override;

  void stop();  // 必须在 RaftNode 析构之前调用（L15）

  bool isAsync() const override { return true; }

  void sendRequestVote(int peerId, const RequestVoteArgs& args,
                       VoteCb cb) override;
  void sendAppendEntries(int peerId, const AppendEntriesArgs& args,
                         AppendCb cb) override;
  void sendInstallSnapshot(int peerId, const InstallSnapshotArgs& args,
                           InstallCb cb) override;
  void sendReadProbe(int peerId, const ReadProbeArgs& args,
                     ReadProbeCb cb) override;

  void addPeer(int id, const std::string& addr) override;  // L10：仅在锁外调用
  void removePeer(int id) override;

  size_t inflight() const;  // 在途请求数（指标 / 用例断言）

 private:
  void sendFrame(int peerId, MsgType type, const Bytes& payload, MsgType expect,
                 std::function<void(const Byte*, size_t)> onReply);

  Reactor reactor_;
  uint64_t timeoutMs_;
};

}  // namespace raftkv::raft
