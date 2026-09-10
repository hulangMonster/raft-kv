# M2 代码评审报告（#4）

> 评审方式：独立评审子代理因网络中断未产出，改由主流程从严自审。
> 评审范围：`src/raft/*`、`src/kv/kv_state_machine.*`、`src/main_raft_*.cpp`、
> `CMakeLists.txt`、`tests/raft_*.cpp`、`scripts/raft_*.sh`。
> 现状基线：`raftkv_raft_tests` 14/14、`raft_e2e.sh` 8/8、`raft_fault.sh --repeat 50` 全绿。

## 结论

M2（Raft 选主 + 日志复制）**核心正确、可上简历**：选举、日志复制、§5.4.2 提交规则、
持久化、崩溃恢复、故障注入都有测试覆盖。没有发现会导致数据丢失或双主的阻断性 bug；
下面是 1 个正确性缺口（线性一致读）和若干工程优化项。

## 【正确性缺口——已修复 ✅】

1. **新 Leader 不提交 no-op，导致重选后读旧数据**（高）→ **已实现**
   - 位置：`src/raft/raft_node.cpp::tick` + `src/raft/types.h::RaftConfig::appendNoop`
   - 修复：Leader 上任后若 `log_.lastTerm() != currentTerm_`，追加一条 no-op
     （op=kGet、空 key、clientId=0、requestId=index），走正常复制提交；
     `KvStateMachine::apply` 对 kGet 显式 no-op（只推进 applied index，不改数据/去重表）。
   - `RaftConfig::appendNoop` 默认 true；§5.4.2 反例测试用 `makeCluster(5, false)`
     关闭 no-op 以保留"旧 term 条目复制到多数派仍不得提交"的精确断言。
   - 验证：单测 14/14、`raft_e2e.sh` 12/12、`raft_fault.sh --repeat 50` 全绿；
     e2e 已去掉 marker 写规避。

## 【优化建议】

2. **fsync 在 RaftNode 锁内执行**（中）
   - `onAppendEntries/propose/startElection` 持 `mu_` 时调用 `LogStore::append/persistMeta`，
     FileLogStore 内部会 fsync（阻塞磁盘）。不是正确性问题，但会让 ticker/其他线程被磁盘延迟拖住。
   - 建议：M5 引入"锁内改内存日志 → 解锁落盘 → 回锁确认"的两段式，或 group commit。

3. **TcpTransport 全局单锁串行所有 RPC**（中）
   - `transport_tcp.cpp::roundTrip` 用一把 mutex 串行所有 peer 的阻塞请求；已用 30ms 超时缓解
     单点挂起对 ticker 的饥饿，但扩展性差。
   - 建议：M5 改为每 peer 一个连接+锁、异步发送，或换 epoll/Reactor。

4. **LogEntry 序列化在两处重复实现**（低）
   - `message.cpp` 与 `file_log_store.cpp` 各自写了一份 41 字节的 LogEntry 编解码，布局漂移风险。
   - 建议：抽到 `src/raft/log_codec.{h,cpp}` 共用。

5. **FileLogStore 不校验 append 的 index 连续性**（低）
   - `append` 对 `e.index > lastIndex()+1`（gap）直接 push_back。RaftNode 保证连续，但作为独立组件
     缺少防御。建议加 `assert(e.index == lastIndex()+1)` 或返回 false。

6. **meta.dat rename 后未 fsync 目录**（低）
   - `persistMeta` 写了文件并 fsync，但 rename 后没有 fsync 父目录；极端掉电下目录项可能丢失。
     建议 `open(dir, O_RDONLY)` + `fsync(fd)`。

7. **main_raft_node 优雅关闭用 `_Exit(0)` 规避 detach 线程竞态**（低，可接受）
   - 关闭时 `ticker.join()` 后 `_Exit(0)`，跳过 FileLogStore 析构（fd 由 OS 回收）。可接受，
     但评审建议至少注释说明，并考虑 M5 改为显式 join 连接线程。

8. **选举超时是确定性伪随机**（低）
   - `electionTimeoutMs()` 用 (selfId,term) 公式，测试友好；生产建议混入真随机避免可预测。

## 已确认无问题（抽查）

- 投票持久化顺序：`onRequestVote` 先 `persistMeta` 再返回 grant（I5）✓
- §5.4.2：`advanceCommitAndApply` 只对 `log[n].term == currentTerm_` 推进 commit ✓（有反例测试）
- 冲突截断：`onAppendEntries` 同 index 异 term → `truncateSuffix` ✓
- 幂等：`KvStateMachine::apply` 按 (clientId,requestId) 去重 ✓
- torn-tail：FileLogStore `load` CRC/长度校验 + `ftruncate` ✓（B 组测试）
- CMake：M1 目标未动，GTest 仅测试目标、核心零第三方运行时依赖 ✓

## 建议执行顺序

1. ✅ no-op 已补（正确性修复，本轮完成）。
2. 其余优化项（fsync 锁外、异步 transport、LogEntry 编解码去重等）按 M5 规划逐步做，不阻塞当前里程碑。
