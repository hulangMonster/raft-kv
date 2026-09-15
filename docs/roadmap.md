# Roadmap

每个里程碑都带**验收标准**：完成的标准不是"写了代码"，而是
`cmake --build build && ./build/bin/raftkv_tests && ./scripts/e2e.sh`（或对应阶段脚本）通过，
并且能对着架构图讲清楚一个设计决策。

## M1 ✅ 单机版 KV + WAL（当前已完成）

- 内存 KV（put/get/del）、WAL 追加写与启动重放
- 写路径：WAL 先行 → 内存应用，加锁保证顺序一致
- CRC32 检测 torn tail，启动时截断
- 线程池 TCP 服务端、命令行客户端、顺序压测
- 验收：单测全绿；`e2e.sh` 覆盖 kill -9 后重启数据仍在

**M1 → M2 之间补课**：Raft 论文 §5（5.1–5.3）、gRPC 入门示例、
一致性哈希 / CAP 通俗材料，读一遍即可动手。

## M2 ✅ Raft：选主 + 日志复制（已完成）

> 详细设计见 [m2-design.md](m2-design.md)，前置校验见
> [m2-prerequisites.md](m2-prerequisites.md)。

- 3 节点集群：Leader 选举（确定性随机超时、心跳续任、任期规则、投票日志新旧限制）
- 日志复制：AppendEntries 一致性校验 / 冲突截断 / 掉队追赶；§5.4.2 提交规则（只提交当前 term 的多数派）
- `KvStateMachine` 幂等 apply（clientId/requestId 去重）；`propose` 等待 提交/降级/超时
- `FileLogStore`：meta.dat 原子写（tmp+rename+fsync）+ raft.log CRC 帧 + torn-tail 截断 + truncateSuffix —— 决策 D1（Raft log 为唯一持久化真相源）
- 节点进程 `main_raft_node`（RaftNode + TcpTransport + ticker 线程 + 锁纪律）；集群客户端 `main_raft_client`（NotLeader 重定向重试 + REPL）
- M2.5：conflictIndex 快速回退 + **§5.4.2 严格反例测试**（5 节点确定性构造：旧 term 条目复制到多数派仍不得提交）
- 验收（均实测通过）：
  - `raftkv_raft_tests` **14/14**（含 §5.4.2 反例、崩溃重启、torn-tail）
  - `scripts/raft_e2e.sh` **PASS**：kill -9 Leader 自动重选、数据存活、无多数派写失败
  - `scripts/raft_fault.sh --repeat 50` **PASS**：SIGSTOP 分区 + 追平 + 稳定性

## M3 ✅ 快照与日志压缩（已完成）

> 详细设计见 [m3-design.md](m3-design.md)（**v1.2**，含 D3/D4 修订），前置校验见
> [m3-prerequisites.md](m3-prerequisites.md)。

- `SnapshotStore` seam：`MemorySnapshotStore` / `FileSnapshotStore`（`RKS1` 文件格式、tmp+fsync+rename+fsync(dir)、CRC、torn-snapshot 丢弃、只留最新）
- `StateMachine` 最小扩展 `snapshotView()/restore()`；`KvStateMachine` 快照视图与恢复（**含幂等去重表**）
- `RaftNode`：按 `lastApplied` **两段式**生成快照（锁内取一致视图、锁外序列化+落盘）、`lastIncluded*`、**五步启动恢复**（先快照后日志；`setBoundary` 先于 `load`）
- `LogStore`：`compact` 前缀物理压缩 + **D3 基址模型**（`firstIndex_`、边界感知的 `termAt/slice/lastIndex/lastTerm`、`load` 相对 `firstIndex_` 校验）
- InstallSnapshot（msgType **5/6**，分块 + `nextOffset` 回带）+ `Transport::sendInstallSnapshot`（D4）；follower 安装 / 保留边界之后后缀 / 忽略旧快照
- 验收（均实测通过）：
  - `raftkv_raft_tests` **35/35**（M2 14 + M3 14 + #4 评审回归 7，含单节点 propose、丢回复续传、安装后当选能服务快照、并发 save+install、文件存储集群压缩后继续提交）
  - 同一套用例在 **ASan** 下全绿
  - `scripts/raft_snapshot_e2e.sh` **PASS**
  - `scripts/raft_snapshot_fault.sh --repeat 50` **PASS**（A: 10w 条后 `raft.log` 有界；B: 空节点经 InstallSnapshot 追平；C: kill -9 / SIGSTOP 注入 50 次）

### M3 加固（#4 独立评审）：15 个阻断项已修复

> 决策表见 [m3-design.md](m3-design.md) **v1.3** §12；评审结论与修复记录见 [code-review.md](code-review.md)。

- **正确性/持久化**：单节点组提交的提交点（B1）、`sync()` 失败不得计为已持久（B2）、
  `flushTarget` 与并发 truncate 的耐久性夹紧（B3）、`FileLogStore` fd 锁下沉到 store 内部（B4/D5/L9）、
  恢复路径失败即拒绝启动（B10/B11）、`compact` 失败即 fatal + `fsyncDir`（B12）
- **InstallSnapshot**：幂等续传（丢回复不卡死，B7）、接收端并发串行（B14）、安装后 `snapshotBytes_` 刷新（B6）、
  后缀 term 校验 + `commitIndex = min(leaderCommit, 最后一条已匹配条目)`（B8）、`commit/applied` 单调不回退（B9）、
  压缩区守卫与 fast-backup 夹紧（B13）
- **快照生命周期**：`SnapshotStore` 拒绝回退边界 + `installEpoch_`（B5）
- **客户端**：`fill --pipeline` 每 worker 独立 `clientId`，消除静默丢写（B15）；新增 `verify <n>` 写回校验

### M5 前置成果（已落地）：group commit + 客户端并发

- `LogStore::appendNoSync()` + `sync()`；`RaftNode::propose` 组提交（`syncInFlight_` / `syncedIndex_`：一次 fsync 摊一批），`buildAppendEntries` 只复制**已持久化**条目（`syncedIndex_` 门控）
- 客户端 `raftkv_raft_cli fill <N> --pipeline K`（K 条**并发连接**，每 worker 独立 `clientId`）+ `verify <N>`（写回校验零丢写）
- 实测（3 节点，threshold 5000，`scripts/bench_group_commit.sh`，每次 `fill` 后 `verify` 校验）：
  **pipeline=1 → 129 qps；pipeline=8 → 746 qps；pipeline=64 → 2840 qps（≈22×）**，三次校验均 `missing 0`
  （旧记录的 2623 qps 是在共享 `clientId` 静默丢写的 bug 下测得的，已被 #4 评审推翻并修正）
- 遗留（见 #4 评审，已明确推迟到 M5）：异步/每 peer 连接 transport、快照流式序列化、tick no-op 绕组提交、
  `main_raft_node` 线程池化

## M4 成员变更 + 客户端路由

- 单节点增减（Joint Consensus 简化版：先只支持一次加/减一个）
- 客户端从任意节点路由到 Leader；GET 默认读 Leader（线性一致）
- 验收：5 节点集群在线增删节点，压测期间无长时间不可用

## M5 性能与可观测性（压测故事 + 优化）

- 观测：接入 perf / 火焰图，先量化再优化
- 优化方向：epoll/Reactor 替代 thread-per-conn、分片锁或并发哈希、
  WAL 组提交（group commit）、批处理复制
- 输出：优化前后 QPS / p99 / 锁竞争对比表（这部分直接进简历）
- 可选：Node Exporter 风格指标端点、gRPC 接口层

## 贯穿性工程要求

- 每个阶段先写"怎么验证"，再写实现
- 提交信息讲清楚动机（如 `wal: truncate torn tail on replay`）
- 每完成一个里程碑更新 README 的"当前能力"与架构图
