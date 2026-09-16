#pragma once

#include <functional>
#include <vector>

#include "types.h"

namespace raftkv::raft {

class RaftNode;  // forward decl: MemoryTransport delivers to a node directly

// Node-to-node RPC transport (async callback style). Production uses TCP;
// tests use the synchronous in-memory adapter below.
class Transport {
 public:
  using VoteCb = std::function<void(const RequestVoteReply&)>;
  using AppendCb = std::function<void(const AppendEntriesReply&)>;
  using InstallCb = std::function<void(const InstallSnapshotReply&)>;

  virtual ~Transport() = default;
  virtual void sendRequestVote(int peerId, const RequestVoteArgs& args,
                               VoteCb cb) = 0;
  virtual void sendAppendEntries(int peerId, const AppendEntriesArgs& args,
                                 AppendCb cb) = 0;
  // M3.3 (D4): InstallSnapshot chunk transfer.
  virtual void sendInstallSnapshot(int peerId, const InstallSnapshotArgs& args,
                                   InstallCb cb) = 0;

  // M4 (L10): 地址簿动态更新；只在锁外作业中调用。
  // 默认空实现 -> MemoryTransport 之外的测试桩零改动即可编译。
  virtual void addPeer(int id, const std::string& addr) {
    (void)id;
    (void)addr;
  }
  virtual void removePeer(int id) { (void)id; }
};

// Synchronous in-memory adapter for unit tests. Delivers directly to the
// peer's RaftNode handlers, then invokes the callback immediately. Supports
// isolating a node to simulate a network partition.
class MemoryTransport : public Transport {
 public:
  void addNode(int id, RaftNode* node);
  void isolate(int id);  // drop messages TO id (partition)
  void heal(int id);     // resume delivering TO id

  void sendRequestVote(int peerId, const RequestVoteArgs& args,
                       VoteCb cb) override;
  void sendAppendEntries(int peerId, const AppendEntriesArgs& args,
                         AppendCb cb) override;
  void sendInstallSnapshot(int peerId, const InstallSnapshotArgs& args,
                           InstallCb cb) override;

  // M4: 让测试把"尚未进入配置"的节点接上 transport（等价于 Leader 的 addPeer）
  void addPeer(int id, const std::string& addr) override;
  void removePeer(int id) override;

 private:
  std::vector<RaftNode*> nodes_;   // indexed by id (id >= 1)
  std::vector<bool> isolated_;     // parallel to nodes_
};

}  // namespace raftkv::raft
