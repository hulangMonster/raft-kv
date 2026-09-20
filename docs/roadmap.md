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

## M4 ✅ 成员变更 + 客户端路由（已完成）

> 详细设计见 [m4-design.md](m4-design.md)（**v1.5**，含 5 次修订；v1.5 记录第三轮评审后的口径修正），前置校验见 [m4-prerequisites.md](m4-prerequisites.md)。

- ClusterConfig/Member + 配置条目（复用 M2 entry 布局，OpCode::kConfig）+ 启动配置重建（seed → 快照配置 → 日志条目，版本严格单调）
- 被移除节点的送达：确认收到移除条目（或超 `catchUpTimeoutMs` 预算）才移出复制目标并回收（设计 v1.4(a)）
- 成员变更：一次一个（J1，`changeMembership` 全程串行化）、新节点 CatchUp 后加入（判据：`matchIndex >= commitIndex` 且本任期已应答）、提交需双多数派（J2，且 **commitIndex 只要触及/越过在途配置条目就必须满足 C_old 多数派**）、移除的送达与退役、Leader 自我移除（§5.7）
- 选举与投票资格（J4）：非成员 / CatchUp 目标 / 退役节点既不给票也拿不到票
- 快照携带配置（RKS1 **v2**，兼容 v1）；**在途配置绝不写进快照**（按边界取配置）；InstallSnapshot 安装即无条件重置配置基线并按剩余日志重算；配置条目被截断时**回滚**到基线重算（§5.2）；重启后日志尾部配置条目重新标记为**在途**（J1/J2 不因重启失效）
- 线性一致读：ReadIndex（探针 msgType 9/14）+ **§8 同任期提交屏障**（新 Leader 未提交本任期条目时宁可变读失败也不返回陈旧值）
- 客户端拓扑发现：缓存 `{configVersion, id→addr}`，`kNotLeader` / 连接失败时失效重取，`--peers` 只需一个可达种子
- 验收（均实测通过）：
  - raftkv_raft_tests **68/68**（M2 14 + M3 14 + M4 A1–A29 / B1–B4，含评审回归 A20–A29）
  - scripts/raft_membership_e2e.sh **PASS**（3 节点压测 → 第 4 节点 seed 启动 → add → 4 节点压测 + 线性一致读 → remove → 退役校验 → `--peers` 只给一个 follower 种子仍能路由 → 初始节点不可达时自动换节点）
  - scripts/raft_membership_fault.sh --repeat 50 **PASS**（5 节点；每轮轮换 follower 注入 kill -9 / SIGSTOP、每 4 轮额外杀 Leader，并在窗口内做成员变更；每轮校验配置收敛 + 追平 + 压测可继续 + 线性一致读）
  - 既有 M2/M3 脚本与 e2e.sh 全部保持 PASS
- M4 期间修掉的真实缺陷：op 白名单两处漏加 kConfig（TCP 复制 / 重启恢复会静默丢配置条目）、
  TcpTransport::addPeer/removePeer 未实现（CatchUp 永远连不上新节点）、配置回滚缺失（A19）
- **#4 第三轮独立评审（7 阻断 + 9 优化）全部处置**（详见 [code-review.md](code-review.md) M4 段）：
  J2 被"后一条条目先提交"绕过、J4 未落实（非成员/退役节点仍投票）、新 Leader 陈旧 ReadIndex 返回旧值、
  在途配置写进快照导致重启拒绝启动、重启丢在途配置（J1/J2 失效）、`changeMembership` check-then-act 竞态、
  `decodeAppendEntries` count 未校验导致远程 `bad_alloc` 终止进程；优化项覆盖 ReadIndex 双重多数派、
  被移除 peer 回收、`readAcks_` 回收、回滚重算、客户端拓扑缓存、wire codec 加固、SM 的 kConfig 契约。
  每个修复都有对应回归用例，且**在移除修复的状态下确认过 RED**（探针现象复现）
- 吞吐：同一机器状态下 M3 收尾版与 M4 的 fill 200 --pipeline 1 均为 13.2 ms/写（**无可测回归**）；
  bench 的绝对数字受机器状态与 --snapshot-threshold 影响很大，跨会话不可直接比较

## M5 性能与可观测性（**已交付**；p=8/64 同机比值已在 P2a 修复达标）

- ✅ 观测：`status` 指标（qps/延迟分位/fsync 次数与耗时/批量/复制滞后/选举/快照/锁等待/在途 RPC），
  指标只读；未接 perf/火焰图（`perf_event_paranoid=4`，已记录）
- ✅ epoll/Reactor 传输（`--transport=reactor`，默认仍 sync）、组提交（两段式：锁内 write / 锁外 fsync）
- ✅ 批处理与滑动窗口（`--inflight-per-peer`、乐观 `nextIndex_` + TTL 回退）、蓄批旋钮（实测无增益，默认关）
- ✅ 流式快照序列化（§8.3）与跨进程断点续传（§8.4）
- ✅ 输出：同机交替 A/B（3 次中位数）+ 微基准 + 分段实测，见 `docs/m5-bench.md` §3 与 `docs/m5-review.md`
- ✅ **p=8/64 同机比值（P2a 修复后达标）**：0.60×/0.57× → **1.34× / 2.20×**，p=1 延迟 1.03×（门槛 ≤1.2×）。
  真实根因**不是** §3.10 说的"批间唤醒 + 全局锁争用"，而是**复制发送段在 `TcpTransport` 全局锁上排队**
  （`syncInFlight_` 在 fsync 返回即放开 → 多个 flusher 并发发送）：peer=2 的发送"拿到锁之前"中位等
  **11.5 ms**，而一次真正往返 **<1 ms**。修法：flusher 等本批两次 `sendAppendEntries` 都发出后再放开该窗口
  （M5.A16 守门）。见 `m5-bench.md` §3.11 与提交 `ab7bd57`
- ✅ **节点规模与引擎选择（3/5/10 节点实测，2026-09-20）**：扇出随 N 线性增长（sync p=1 每写
  11.3→18.5→44.6 ms），reactor 非阻塞把它压成常数（≈11 ms）⇒ **N ≥ 5 建议 `--transport=reactor`**；
  N=10 单机上 sync 出现过 leader 变更而 reactor 稳定。数据见 `m5-bench.md` §3.12
- ⬜ 未做（非本里程碑目标）：分片锁 / 并发哈希、Node Exporter 风格指标端点、gRPC 接口层

## 贯穿性工程要求

- 每个阶段先写"怎么验证"，再写实现
- 提交信息讲清楚动机（如 `wal: truncate torn tail on replay`）
- 每完成一个里程碑更新 README 的"当前能力"与架构图
