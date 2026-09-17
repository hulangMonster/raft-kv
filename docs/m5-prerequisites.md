# M5 前置校验（#1 /verification-before-completion）

> 对应设计：[m5-design.md](m5-design.md) **v1.0**（8 项决策冻结 + 基线证据 + 崩溃契约）
> 本文档只做"实现前置校验"：不变量、锁纪律、风险、改动清单、边界与前置假设。
> 结论：**可以进入 #2（TDD 先行）**；下列 I9–I14、L12–L15 与禁止清单是 M5 全部实现的约束。

---

## 1. 不变量

### 1.1 沿用（不得削弱）

- **I1–I8**（`m2-prerequisites.md`）：下标从 1 起 / term 单调 / 一个 term 至多一票 / commit&applied 单调 /
  **I5：回复 RequestVote 前已持久化 term+votedFor，回复 AppendEntries success 前已持久化该批 entries** /
  只有 Leader 收写 / KV apply 幂等 / `RaftNode` 不直接依赖 socket 与文件系统。
- **M3**：快照 ≤ `lastApplied_`；先 durable 快照后 compact；`compact` 失败即 fatal；`syncedIndex_` 只认成功的 fsync。
- **M4**：J1（一次一个变更）/ J2（新旧双多数派，且 `n >= inFlightConfigIndex_` 时校验 C_old 对配置条目的多数派）/
  J3（版本单调，回退拒绝启动）/ J4（非成员/退役节点无票且不得票）/ J5（C_old ∩ C_new ≠ ∅）/
  ReadIndex §8 同任期屏障 / "提交 ≠ 送达完成"的送达目标语义 / `membershipMu_` 叶子锁。

### 1.2 新增（M5）

| # | 不变量 | 如何断言 |
|---|---|---|
| **I9** | 持 `mu_` 期间不发生 `fsync`、不发生任何网络 IO（锁内允许 page-cache `write`） | `SpyLogStore` + `MuHeldGuard` 线程局部标志：持锁窗口内 `sync/compact/persistMeta` 调用数 == 0（M5.A1） |
| **I10** | 一次 fsync 摊一批；只有 durable 之后才对外声明持久（`syncedIndex_` 语义不变） | M5.A2：`fsync 次数 ≤ 批次数`，且 sync 失败时 `syncedIndex_` 不推进 |
| **I11** | 回复 `AppendEntries success` / `RequestVote granted` 之前，该批 entries / `term+votedFor` 必须已 durable | M5.A3（卡住 sync → 不 ack）、M5.A4（卡住 persistMeta → 不 granted） |
| **I12** | 异步发送不改变 §5.4.2 / J2 / §8 屏障的判定输入集合 | M5.A5：乱序/重复 ack 下 `matchIndex_` 单调、多数派判定与同步引擎一致 |
| **I13** | 指标只读、无副作用、不得成为任何正确性判定的输入 | M5.A7：指标单调；关闭指标的全部用例行为不变；指标不在任何分支条件里 |
| **I14** | 不改变 wire 格式、磁盘格式与对外错误码语义（`RKS1` v1 仍可解） | M5.B2 + 既有 B4（`Rks1V1SnapshotStillLoads`）保持绿 |
| **I15** | （M5.3 实测新增）异步引擎下"该 peer 已有一批在途"的**静音窗口必须严格小于 follower 的最小选举超时**（取 `electionTimeoutMinMs/3`，且不超过 `2*rpcTimeoutMs`） | M5.A10 钉住不变量（含 tick 10ms 余量与极端配置）；reactor 引擎 `raft_fault.sh --repeat 50` 连续 3 轮通过 |
| **I17** | （M5.6 新增）滑动窗口下 `nextIndex_` 允许**乐观推进**（发出即推进），但每一批的"未确认区间"必须始终存在回退/重发路径：① 槽位 TTL 过期 → `nextIndex_` 回退到该批起点；② 收到失败应答 → 清空该 peer 全部在途槽位并从冲突点重发。否则丢一帧（reactor 超时丢弃且不回调）= **丢写** | M5.A11（乱序/重复 ack 幂等 + `matchIndex_` 单调 + 窗口宽度不超限）；reactor 故障注入 3 脚本通过 |
| **I16** | （M5.3 实测新增）**会改日志边界/快照文件的整段操作必须彼此串行**（`maybeSnapshot` 的 save+compact 与 `onInstallSnapshot` 的 receive+load+compact）；且 **transport 回调中逃逸的异常不得让节点无诊断地静默死亡** | 机制复现脚本（reactor + `--snapshot-threshold 200` + `fill 20000 --pipeline 64`）×5 全部无节点死亡；reactor 全量 e2e/fault 五个脚本全绿 |

## 2. 线程安全契约与锁纪律

### 2.1 沿用 L1–L11（`m2-prerequisites.md` / `m4-design.md`）

`mu_` 保护全部共识状态；公开方法不在持锁时调用 `transport_`；出站作业"锁内构造、锁外发送"；
`cv_` 谓词含 role/term；序列化与 IO 在锁外（L8）；fd 生命周期归 `FileLogStore::mu_`（L9）；
地址簿更新只在锁外（L10）；锁序 `RaftNode::mu_ → Transport::mu_ → LogStore::mu_`（L11）。

### 2.2 新增 L12–L18

| # | 契约 |
|---|---|
| **L12** | **锁内只允许 `LogStore::appendNoSync`（write）**；`sync/compact/persistMeta` 一律在锁外执行，回锁后必须重新校验（term/role/lastIndex/installEpoch） |
| **L13** | `metaPersistMu_` 是叶子锁，锁序 **`metaPersistMu_ → mu_`**；持 `mu_` 时绝不获取它。"决定 term/votedFor → 落盘 → 回锁校验"整体在 `metaPersistMu_` 下串行，保证**磁盘 meta 版本单调** |
| **L14** | `Reactor` 线程**从不持有 `mu_`**；它只触发回调，回调入口自己取 `mu_`（与今日同步回调一致）。`Transport` 内部锁序列保持在 `mu_` 之外，锁序不变 |
| **L15** | 关闭顺序：`Reactor::stop()`（停 epoll → 关连接 → 丢弃在途回调）→ join reactor → join ticker → 再析构 `RaftNode`/store；**禁止再用 `std::_Exit(0)` 绕过析构** |
| **L16** | （M5.2 实测新增）任何"把 IO 移出 `mu_`"的改动，必须确认被移出方与**仍留在锁内**的调用方之间原有的互斥/顺序关系是否被打破；被移出方若曾借用 `mu_` 互斥，必须自己补锁（例：`FileLogStore::appendNoSync` 依赖 `mu_` 与 `compact()` 互斥，出锁后必须改为 store 自持锁） |
| **L18** | （M5.6 新增）在途槽位表 `appendInflight_` 受 `mu_` 保护；`peerSendAllowedLocked()` 只**读**（统计未过期槽位），回收与 `nextIndex_` 回退只在非 const 的 `pruneInflightLocked()` 里做（`approveAppendSend` 开头调用）。槽位 TTL 复用 I15 的 `inflightMuteGapMs()`，因此"窗口满"的静音时长仍严格小于最小选举超时 | （M5.3 实测新增）`snapshotOpMu_` 是叶子锁，锁序 **`snapshotOpMu_ → RaftNode::mu_`**；只在 `mu_` 之外获取，持 `mu_` 时绝不获取它。持它期间做的是 save/load/compact/install（含 fsync），因此**不违反 I9**（共识锁仍未被长 IO 占住） |

### 2.3 原子性与可见性

- `Metrics` 计数器用 `std::atomic`（`fetch_add`/`load`）；直方图用 `atomic<uint64_t>` 分桶数组。
- 允许在锁内做指标累加的**只有** O(1) 原子操作（目标开销 <1%）；任何需要分配/遍历的采集放锁外。
- 新增的 `metaDirty_`/`pendingSyncIndex_` 等标志均受 `mu_` 保护，不引入独立原子状态。

## 3. 风险清单（含检测手段）

| 风险 | 触发条件 | 后果 | 对策 / 检测 |
|---|---|---|---|
| R1 no-op 不 durable 卡住读路径 | no-op 走 appendNoSync 但 `syncedIndex_` 不推进 | §8 屏障永不满足 → **所有线性一致读失败** | §8.1 锁外 fsync；**先写 M5.A8 用例再看实现** |
| R2 follower 配置生效延后 | `applyAppendedConfigLocked` 移到 durable 之后 | 在途窗口内 `currConfig_` 落后于日志 | 全量回归 A20–A29（尤其 A24/A25/A29）；窗口内不对外回复 |
| R3 回锁后状态已变 | 解锁窗口内发生 step-down / truncateSuffix / compact / InstallSnapshot | 用过期视图推进 `syncedIndex_`/回复错误结果 | 每个回锁点重校验；M5.A3/M5.B4 覆盖 |
| R4 meta 落盘乱序 | 两个投票处理器并发，低 term 落盘晚于高 term | I3 被破坏（崩溃后重复投票） | `metaPersistMu_`（L13）+ M5.A4 |
| R5 Reactor 悬垂回调 | `removePeer` 与在途请求竞争 | 回调访问已析构状态 / 触发已移除 peer 的逻辑 | 在途表按 peer 清理；M5.A6 + TSan |
| R6 部分写/重连风暴 | 非阻塞写只写了一半 / 对端反复断连 | 帧错位 / CPU 打满 | 写队列 + `EPOLLOUT`；重连退避；帧解析仅在完整帧时交付 |
| R7 关闭期在途异步发送 | 退出时仍有未完成 RPC | 回调打到半析构对象 | L15 关闭顺序；`stop()` 后不再触发回调 |
| R8 快照流式化改坏格式 | 分块写出与既有 `RKS1` 布局不一致 | 重启/安装失败 | 只改"如何写出"不改布局；M5.B2 + 既有 B4 |
| R9 断点续传半写 | 传输中重启 / 头不匹配 | 装到残缺快照 | `.recv` 头带 `(index,term,receivedLen,crcSoFar)`；不匹配即丢弃重传；M5.B3 |
| R10 基准自欺 | 机器状态漂移 / 未 verify / 单次取样 | 假性能结论 | 同机交替 + 预热 + 3 次中位数 + `verify missing 0`（§10 方法学） |
| R11 指标开销 | 高频采样 / 锁内复杂计算 | 反噬主指标 | 原子 O(1) + 采样只在 `status` 请求时做差；M5.5 做开/关 A/B（<1%） |
| R12 TSan 报告 | 新旧并发面（Reactor + 回调） | 数据竞争 | TSan 全量门禁（M5.3 起硬性） |

## 4. 存量代码修改点清单

### 4.1 【必须改】

| 文件 | 改动 |
|---|---|
| `src/raft/raft_node.{h,cpp}` | `onAppendEntries` 两段式（§6.3）；`tick` no-op 走 `appendNoSync` + 锁外 fsync；`startElection`/`onRequestVote` 的 I5 三段式；`becomeFollower` 去 IO + `metaDirty_`；`maybeSnapshot`/`onInstallSnapshot` 的 `compact` 出锁；新增 `syncLogOutsideLock()`/`flushMetaOutsideLock()`；指标挂点（可选指针） |
| `src/main_raft_node.cpp` | 默认改用 `TransportReactor`；接入 `Metrics`；`status` 增字段；msgType 15 处理；**显式 stop+join 替换 `_Exit(0)`** |
| `src/raft/file_snapshot_store.cpp` | 流式 `save()`（分块 + 增量 CRC + rename + fsyncDir）；`.recv` 头带 `(index,term,receivedLen,crcSoFar)` + 续传 |
| `src/kv/kv_state_machine.{h,cpp}` | `SnapshotView` 增流式接口（分块产出，峰值 O(块)）；**语义不变** |
| `CMakeLists.txt` | `raftkv_raft` 追加新源文件；`raftkv_raft_tests` 追加 `tests/raft_perf_test.cpp`；新增可选 `-DENABLE_TSAN=ON` 构建开关 |
| `tests/raft_test_harness.h` | **仅追加**：`MuHeldGuard`、`SpyLogStore`、`BlockingLogStore`、`AsyncMemoryTransport`、`makeSlowPeerCluster` |

### 4.2 【可选扩展】

`src/raft/transport_tcp.{h,cpp}`（保留为同步对照引擎，可加指标计数）；`src/raft/file_log_store.cpp`（fsync 计数上报）；
`src/main_raft_client.cpp`（延迟直方图采样，仅用于压测报告）；`src/raft/log_codec.{h,cpp}`（消除 M2/M3 两份 LogEntry 编解码）。

### 4.3 【禁止改动】

M1 全部源码与 `tests/test_*.cpp`、`scripts/e2e.sh`；M2 §5.4.2 提交规则、fast-backup 语义、wire 字节布局；
M3 `RKS1` 字节布局与 D2/D3 语义；M4 `J1–J5`、`membershipMu_`、ReadIndex §8 屏障、送达目标语义。

## 5. 边界 case 全集（实现必须逐条回答）

1. 单节点集群（无 peer）：两段式路径不得引入额外的 fsync 或死等；no-op 必须在同 tick 内提交并满足 §8。
2. peer 长时间挂起（SIGSTOP）：Reactor 侧在途请求超时清理；ticker 与其它 peer 不受阻。
3. 解锁窗口内 step-down：follower 不得 ack（I11）；leader 不得推进 `syncedIndex_` 后回复 `kOk`。
4. 解锁窗口内 `truncateSuffix`/`compact`：`syncedIndex_` 夹紧到 `lastIndex()`；被截断的配置条目不得生效。
5. `compact` 期间并发 `InstallSnapshot`：边界不回退（M5.B4）；`installEpoch_` 变化时字段更新让位。
6. `fsync` 失败：不推进 `syncedIndex_`、不回复 success、写路径 `kErr`；store 进入"broken"后不得假装成功。
7. 磁盘满：`write` 失败 → `appendNoSync` 返回 false → 条目不存在，任何路径不得把它算进多数派。
8. 退出时在途异步发送：`stop()` 丢弃回调；不得在析构后再触达 `RaftNode`。
9. `removePeer` 与在途请求竞争：不触发该 peer 的回调（M5.A6）。
10. 重复/乱序 ack：`matchIndex_` 单调、`nextIndex_` 只按既有 fast-backup 规则回退。
11. 指标在请求中途被读取：只读快照语义；不得因读指标改变状态。
12. 快照流式化 + 压缩边界：块大小与小快照（单块）都要正确；v1/v2 兼容。
13. 断点续传：`.recv` 头匹配但数据被截断 → 校验失败 → 重传；offset 跳跃/重叠 → 重传。
14. 元数据落盘顺序：并发投票请求下磁盘 meta 版本单调（L13）。
15. 滑动窗口（条件启用）：乱序/重复 ack、`nextIndex_` 乐观推进不得造成日志空窗提交。

## 6. 未定义行为（实现中必须消除）

- 在持锁窗口内调用任何可能阻塞的 syscall（fsync/connect/send/recv/rename/fsyncDir）。
- 让回调在 `Reactor` 线程上调用 `RaftNode` 的**持锁内部**方法（只能调公开入口）。
- 依赖"`syncedIndex_` 一定等于 `lastIndex()`"的假设（组提交下不成立）。
- 依赖"配置条目一定已 durable"的假设（追加即生效 ≠ 已持久化）。
- 在 `Reactor::stop()` 之后仍向在途回调投递结果。

## 7. 性能测试的前置假设

1. **机器状态**：同一 VM、无其他负载（`load < 0.5`）、同一文件系统与磁盘占用（记录 `df` 与 load）；
   ASan/TSan 构建**不得**用于性能数字（只用于正确性）。
2. **参数固定**：`--snapshot-threshold 5000`、pipeline ∈ {1,8,64}、n = {5000,20000,20000}、
   预热 200 写丢弃、每档 3 次取中位数、每次 `fill` 后 `verify`。
3. **可比性**：M4 基线二进制由 `git worktree` 在 `dc6c56a` 单独构建，与 M5 **交替**运行；
   禁止跨会话/跨机器状态比较（M3/M4 已踩坑）。
4. **判定**：同档中位数 延迟 ≤ M4×0.9 或吞吐 ≥ M4×1.5 记为改善，否则记未达标。
5. **记录**：`docs/m5-bench.md` 记机器状态、命令、中位数、`verify` 结果与原始日志路径。

## 8. 结论

- I9–I14 与 L12–L15 可被用例与 code review 双向验证；所有必须改动点都限定在 §4.1 白名单内，
  M1–M4 语义与格式（I14）有既有测试守门。
- **允许进入 #2（TDD 先行）**：先写 `tests/raft_perf_test.cpp`（A/B 组）与 P 组脚本骨架，
  实测 RED 并记录输出，再进入 #3 分阶段实现。
