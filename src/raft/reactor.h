#pragma once

// M5.3（设计 §7 / 决策③）：单线程 epoll 事件循环 + 每 peer 连接状态机。
//
// 语义（与旧同步实现的关键差异）：
//   * send() **入队即返回**（非阻塞）；完成回调在 **reactor 线程**触发；
//   * 超时 / 连接失败 -> 丢弃该请求且**不触发回调**（与旧实现"超时即失败"一致）；
//   * removePeer() 关闭连接并丢弃该 peer 的在途请求（回调不触发，用 A6 守门）；
//   * 每 peer 独立状态：一个慢/挂掉的 peer 不阻塞其它 peer，也不阻塞调用方（ticker）。
//
// 字节层面职责：Reactor 只负责"把整帧字节写出去、读回一整帧字节"，不做消息解析
// （解析交给 TransportReactor，复用 M2 的 encodeFrame/decodeFrame）。

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "common.h"

namespace raftkv::raft {

class Reactor {
 public:
  using Bytes = raftkv::Bytes;
  // replyFrame = 完整的应答帧（含 4 字节长度前缀）；ok=false 表示不会再有回调内容。
  using Completion = std::function<void(const Bytes& replyFrame, bool ok)>;

  Reactor();
  ~Reactor();
  Reactor(const Reactor&) = delete;
  Reactor& operator=(const Reactor&) = delete;

  void start();
  void stop();  // 幂等：停事件循环 + join（此后不再触发任何回调）

  void addPeer(int id, const std::string& hostPort);
  void removePeer(int id);

  // 入队一帧并等待一帧应答。返回 false 表示未能入队（peer 不存在）。
  bool send(int peerId, const Bytes& frame, uint64_t timeoutMs, Completion cb);

  size_t inflight() const;  // 在途请求数（指标 / 用例断言用）

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace raftkv::raft
