# M4 前置校验（#1 verification-before-completion）

> 目的：在动手写 M4 实现之前，把**不变量、锁纪律、风险、存量改动点、边界 case、测试前置假设**钉死，
> 并**逐条核对 m4-design.md 的设计前提与真实代码是否一致**。本文件**不含业务实现代码**。
> 关联：[m4-design.md](m4-design.md)（v1.1）· [m3-design.md](m3-design.md)（v1.3）· [m2-prerequisites.md](m2-prerequisites.md) · [code-review.md](code-review.md)
> 规则：若本文件发现设计缺陷 → **回退 #0 修设计**（本轮的 1 处已按此处理，见 §7.3），禁止在校验阶段私自改设计。

---

## 0. 基线状态与验证证据（本轮已在 ubuntu-vm 实测）

| 检查 | 命令 | 结果 |
|---|---|---|
| 基线提交 | `git rev-parse --short HEAD` / `git describe --tags` | `4d5a314` / `m3-snapshot-1-g4d5a314` |
| 工作区 | `git status --porcelain` | 空（干净） |
| 构建 | `cmake --build build -j4` | **PASS** |
| Raft 单测 | `./build/bin/raftkv_raft_tests` | **PASS（35/35）** |
| M1 单测 | `./build/bin/raftkv_tests` | **PASS（13/13）** |
| M1 e2e | `./scripts/e2e.sh` | **PASS** |
| M2 e2e | `./scripts/raft_e2e.sh` | **PASS** |
| M3 快照 e2e | `./scripts/raft_snapshot_e2e.sh` | **PASS** |

> M3 收尾时的完整验收记录（`raft_fault.sh --repeat 50`、`raft_snapshot_fault.sh --repeat 50`、`bench_group_commit.sh` + ASan 35/35）
> 见 [code-review.md](code-review.md) 的"M4 之前"验证证据表；M4 开发期间不重复跑长脚本，收尾时统一复跑。

---

## 1. 不变量

### 1.1 沿用（不得破坏）

| 编号 | 内容 | M4 关注点 |
|---|---|---|
| I1–I4 | 选举安全、Leader 唯一、日志匹配、状态机安全（详见 m2-prerequisites.md） | 多数派必须按**配置集**计票（§5.3） |
| I5 | term/votedFor 先持久化后回复 | 退役节点不得再投票（J4） |
| I6 | 只提交当前任期条目（§5.4.2） | 成员变更后该规则**不变**（A15 用例守它） |
| I7/I8 | 幂等 apply、崩溃恢复 | 配置恢复顺序（§5.1）不得回退拓扑 |
| M3-1 | 快照只覆盖 `≤ lastApplied` 的前缀 | 快照携带配置时同样只允许覆盖已 apply 前缀 |
| M3-2 | **先 durable 快照、后 compact** | M4 不改变该顺序；配置段随快照一起 durable |

### 1.2 新增（J1–J5）

| 编号 | 精确表述 | 违反后果 | 守它的用例 |
|---|---|---|---|
| **J1** | 同一时刻至多一个成员变更在途：`inFlightConfigIndex_ != kNoIndex` 时拒绝新的 `add`/`remove` | 两个变更并发 → 新旧多数派集合混乱、可能双主 | A7 |
| **J2** | 在途配置条目自身的提交判定需 `majority(prevConfig_)` **且** `majority(currConfig_)` | 只用新配置 → 可能在没有旧多数派认可时提交；只用旧配置 → 变更被旧节点挟持 | A8 |
| **J3** | `ClusterConfig::version` 严格单调且只能由日志推进；启动时若"日志配置版本 < 快照配置版本"或收到更小版本配置 → **拒绝**（启动失败/拒绝应用） | 拓扑回退 → 已移除节点复活、脑裂 | A11、B1 |
| **J4** | 被移除节点不再参与投票、不计入任何多数派分母、不作为被计票对象 | 移除无效、可用性下降或双主 | A6、A9、A10 |
| **J5** | `C_old ∩ C_new ≠ ∅`（由 J1 的一次一个保证） | 过渡期两个不相交多数派可各自选出 Leader | A7 的隐含断言 + A15 |

**验证方式**：J1/J2/J4/J5 由 A 组单测（A4–A9、A15）覆盖；J3 由 A11（拒绝小版本）与 B1/B3（重启后不回退）覆盖。

### 1.3 决策 D2 在 M4 的落实

- 快照仍只允许在 `lastApplied_` 处生成（M3 两段式不变），**配置段随同一次 save 一起落盘**；
- 顺序不变：`save(含配置) → 成功 → compact`；保存失败 → 不推进边界、不 compact；
- 配置条目的**物理删除**只可能发生在它已被快照覆盖之后（否则 `compact` 的上界 `≤ lastIncludedIndex` 也不可能越过它）。

---

## 2. 复用 / 扩展 / 重构点

| 类别 | 明细 |
|---|---|
| **复用（零改动）** | 选主与复制核心算法、§5.4.2 提交规则、组提交 `syncedIndex_` 门控、InstallSnapshot 分块与幂等续传、`LogStore`/`SnapshotStore` seam、`Clock`、KvStateMachine 幂等去重、M3 五步恢复顺序 |
| **扩展（追加语义）** | `RaftNode` 增配置状态机与多数派单一入口；`Transport` 增地址簿方法（带默认实现）；`SnapshotData` 增 `config`；`RaftConfig` 增两个超时；msgType 7/8/9/14 |
| **重构（受限、必须做）** | 把散落的 `cfg_.peerIds` 多数派/遍历（**6 个函数、8 处**，见 §5.1）统一改走 `currConfig_`/`pendingPeers_`；**不改算法本身**，只改"成员集合从哪里来" |
| **刻意不做** | 不新增独立配置存储（延续 D1）；不做完整 Joint Consensus；不改 M2 消息字节布局 |

---

## 3. 线程安全契约与锁纪律（L1–L11）

| 编号 | 内容 |
|---|---|
| L1–L7 | 沿用 M2：单锁 `mu_` 保护共识状态；锁内禁 fsync/网络 IO；出站消息"锁内建作业、锁外发送"；propose 用 `cv_` 等待 |
| L8 | 沿用 M3：锁内只取快照边界与一致性视图，序列化/落盘在锁外 |
| L9 | 沿用 M3：`LogStore` 内部锁保护 fd 生命周期（`sync` 在 `mu_` 外） |
| **L10（新）** | **地址簿与成员清理一律锁外作业**：`Transport::addPeer/removePeer`、被移除节点的连接清理，锁内只登记作业，解锁后执行（与出站消息同一模式） |
| **L11（新）** | **锁序**：`RaftNode::mu_` → `Transport::mu_` → `LogStore::mu_`；由于 L10，Transport 锁只在锁外获取；任何路径**不得在持有 Transport 锁时回调 RaftNode**（既有 `roundTrip` 已满足：callback 在锁外） |

**可执行判定（评审可直接 grep 复核）**：
1. `addPeer/removePeer` 的调用点必须出现在 `{ // 解锁后 }` 之后或锁作用域之外；
2. `currConfig_`/`prevConfig_` 的引用不得逃出锁外（`clusterConfig()` 返回值拷贝）；
3. `cv_` 等待谓词必须包含 `retired_` 与配置代际，否则退役/变更后请求可能永久阻塞。

---

## 4. 风险清单（触发条件 / 后果 / 对策 / 守它的用例）

| # | 触发条件 | 后果 | 对策 | 用例 |
|---|---|---|---|---|
| R1 | 变更在途时用错配置集算多数派 | 双主 / 提交丢失 | §5.3 单一入口 + J2 双重判定 | A8、A15 |
| R2 | 配置条目与快照不一致 | 重启后拓扑回退 | 快照携带配置 + J3 单调校验 + 启动即拒绝回退 | A11、A12、B1、B3 |
| R3 | 新节点未追平即计票 | 提交需空节点 ack → 可用性下降 | CatchUp 前置 + `voting=false` 不进配置 | A4、A5 |
| R4 | 被移除节点继续竞选 | 任期抖动、集群被搅乱 | J4 + 退役态（§5.6） | A9、A10 |
| R5 | 配置条目被 compact 后边界语义破坏 | `termAt`/`slice` 异常、恢复失败 | D3 语义不变；配置随快照；B2 专测 | A12、B2、B4 |
| R6 | ReadIndex 与 Leader 切换竞态 | 返回陈旧值（破坏线性一致） | 同任期探针 + majority + 等 `lastApplied_ >= readIndex` + `seq` 校验 | A13、A14 |
| R7 | 客户端拓扑缓存过期 | 请求打到已移除节点 / 死循环重定向 | 缓存带版本 + 失效重取 + 节点侧统一 `leaderHint` | A10、脚本 e2e |
| R8 | 变更中途崩溃 | 拓扑处于中间态 | 配置条目走日志（原子）；未提交即被截断（安全）；`pendingPeers_` 为 Leader 内存态，重启由运维重试 | B3、`raft_membership_fault.sh` |
| R9 | 锁内做地址簿/IO | 共识被阻塞数秒 | L10/L11 | 代码审计项（评审 §2） |
| R10 | `RKS1` v2 与 v1 不兼容 | 旧数据目录起不来 | 按版本分支解码；v1 → "未携带配置"回退 seed/日志 | B4 |

---

## 5. 存量代码修改点清单（精确到函数与行号，行号基于 `4d5a314`）

### 5.1 【必须改】

| # | 文件:行 | 函数 | 改什么 / 为什么 |
|---|---|---|---|
| 1 | `src/common.h`（`OpCode`，:15-18） | — | 追加 `kConfig = 4`。**M1 唯一改动**，纯新增枚举值，不改任何既有行为 |
| 2 | `src/raft/raft_node.cpp:125` | `becomeLeader()` | `nextIndex_/matchIndex_/lastSentEndIndex_/snapshotSendOffset_/snapshotChunkEnd_` 初始化改用 `currConfig_ ∪ pendingPeers_` |
| 3 | `src/raft/raft_node.cpp:147,152` | `startElection()` | 单节点短路判据与拉票列表改用配置；退役态直接返回 |
| 4 | `src/raft/raft_node.cpp:191,196` | `advanceCommitAndApply()` | 多数派改走 §5.3 单一入口；新增**配置条目拦截**（`op==kConfig` → `onConfigEntry`，不下发给 SM）与 J2 双重判定 |
| 5 | `src/raft/raft_node.cpp:248` | `tick()` | 心跳/快照发送循环改用配置（含 CatchUp 目标） |
| 6 | `src/raft/raft_node.cpp:419` | `onRequestVoteReply()` | 选举多数派改用配置；移除节点的回包直接丢弃 |
| 7 | `src/raft/raft_node.cpp:533` | `propose()` flusher | 复制作业循环改用配置（含 CatchUp 目标） |
| 8 | `src/raft/raft_node.cpp` 构造器（:46-80） | `RaftNode()` | 新增 §5.1 启动配置解析：快照配置 → 日志配置条目重放 → J3 单调校验（回退即抛异常拒绝启动） |
| 9 | `src/raft/raft_node.cpp:470-477` | `propose()` GET 分支 | 改为调用 `linearizableGet()`（D-2）；写路径不变 |
| 10 | `src/raft/raft_node.cpp`（`maybeSnapshot` :623 / `onInstallSnapshot` :692） | — | 填充 `SnapshotData::config`；安装后按快照配置重建并在锁内校验版本 |
| 11 | `src/raft/message.cpp:35-41` | `decodeLogEntry()` | op 白名单追加 `kConfig`（**必须**，否则日志中的配置条目无法解码） |
| 12 | `src/raft/message.cpp`（新增函数） | — | 新增 msgType 7/8/9/14 的编解码（复用长度前缀帧） |
| 13 | `src/raft/message.cpp:229-237` | `decodeClientRequest()` | **保持不变**（刻意）：客户端无法伪造 `op=kConfig` 的日志条目，成员变更只能经 `kConfigRequest` → `RaftNode::changeMembership` |
| 14 | `src/raft/file_log_store.cpp:123-126` | `decodeEntry()` | op 白名单追加 `kConfig`（**必须**，否则重启后日志重放失败） |
| 15 | `src/raft/snapshot_store.h:12` | `SnapshotData` | 追加 `Bytes config;`（Raft 层语义，store 只透传） |
| 16 | `src/raft/file_snapshot_store.cpp:102/118/127` | `encodeSnapshotFile/decodeSnapshotFile` | v2 布局 + 版本分支解码；**保留 v1 路径** |
| 17 | `src/raft/memory_snapshot_store.cpp:59-60` | `receiveChunk` | 透传 config（解码后校验 index/term 不变） |
| 18 | `src/raft/transport.h:21-27` | `Transport` | 追加 `addPeer/removePeer`（**带默认空实现**，保证 `MemoryTransport` 与测试桩零改动可编译） |
| 19 | `src/raft/transport_tcp.{h,cpp}` | `TcpTransport` | 覆写两个方法：持自身 `mu_` 更新 `peers_`；remove 时 `dropConnection` |
| 20 | `src/raft/memory_adapters.cpp`（`MemoryTransport`） | — | 覆写两个方法（`nodes_`/`isolated_` 按 id 扩容/置空） |
| 21 | `src/kv/kv_state_machine.cpp:65-71` | `apply()` | 增加 `kConfig` 显式 no-op 分支（与 kGet 并列，只推进 `lastApplied_`） |
| 22 | `src/raft/types.h:90-103` | `RaftConfig` | 追加 `catchUpTimeoutMs=30000`、`readIndexTimeoutMs=500` |
| 23 | `src/main_raft_node.cpp:94-160` | `handleConnection()` | 分发 7/8/9/14；`status`（:137-150）追加 `config_version/members/read_index/retired` |
| 24 | `src/main_raft_client.cpp:114,144` | `request()/requestOnFd()` | 新增 `config/add/remove` 子命令；拓扑缓存 + 失效重取（**不改既有 put/get/del/fill/verify 语义**） |
| 25 | `CMakeLists.txt:43-52,68-76` | — | `raftkv_raft` 追加 `src/raft/cluster_config.cpp`；`raftkv_raft_tests` 追加 `tests/raft_membership_test.cpp` |

**新增文件**：`src/raft/cluster_config.{h,cpp}`、`tests/raft_membership_test.cpp`、`scripts/raft_membership_e2e.sh`、`scripts/raft_membership_fault.sh`。

### 5.2 【可选扩展】（非必须，做了要在设计文档记录）

- `Transport::peerAddr(id)`（调试/status 用）；
- `main_raft_node` 增加 `--catch-up-timeout` 等参数；
- `status` 输出 `in_flight_config=`。

### 5.3 【禁止改动】

- **M1**：除 `common.h` 一行外的全部源码；`src/store.{h,cpp}`、`src/server.{h,cpp}`、`src/wal.{h,cpp}`、`src/codec.{h,cpp}`、`src/thread_pool.h`、`main_server.cpp`、`main_client.cpp`、`tests/test_*.cpp`、`scripts/e2e.sh`。
- **M2**：选主/复制算法路径与 §5.4.2 提交规则本身；`AppendEntries`/`InstallSnapshot` 的**字节布局**。
- **M3**：`RKS1` **v1 解码路径**、D2（先快照后 compact）、D3（`firstIndex_ = lastIncluded_ + 1` 边界语义）、五步恢复顺序（只在尾部追加 config 段）。
- **既有测试**：`tests/raft_*_test.cpp`（M2/M3 用例）只允许"追加"，不得修改既有断言。

---

## 6. 边界 case 全集 / 未定义行为 / 单测前置假设

### 6.1 成员变更边界（A 组必测或显式声明不适用）

1. 同一时刻第二个 `add`/`remove`（J1 拒绝）；2. `add` 已存在的 id；3. `remove` 不存在的 id；
4. `add`/`remove` 自身（self）；5. 同一 id 变更地址（先 remove 再 add）；6. CatchUp 超时/新节点中途崩溃；
7. 追平过程中 Leader 下台/换主；8. 变更期间触发快照与 InstallSnapshot；9. 变更期间客户端 `fill` 压测；
10. 集群从 5 节点降到 2 节点、再降到 1 节点；11. 移除当前 Leader；12. 被移除节点收到旧 Leader 的 AppendEntries；
13. 被移除节点自我竞选（必须失败）；14. 新节点以退役态启动（自身不在 seed 配置）；15. 空配置（0 投票成员）时的多数派计算必须被显式拒绝。

### 6.2 读路径边界

16. 少数派 Leader 的 ReadIndex（超时失败）；17. 探针回包带回更高 term（立即降级）；18. 探针未凑齐 majority 时 Leader 下台；
19. 读与快照生成并发（读必须等 `lastApplied_ >= readIndex`）；20. 退役节点的读请求（回 `kNotLeader`）。

### 6.3 持久化边界

21. 配置条目被 `truncateSuffix` 截断（配置回滚到日志剩余前缀的状态）；22. `RKS1` v1 文件 + 日志含配置条目；
23. 快照配置版本 < 日志配置版本（正常，取更大者）；24. 快照配置版本 > 日志配置版本（**拒绝启动**）；
25. 变更中途 kill -9（B3）；26. 快照文件 torn（沿用 M3 丢弃语义）。

### 6.4 未定义行为（必须显式定义或禁止）

- **多数派计算依赖容器迭代顺序** → 禁止（只用计数）；
- **`votingCount()==0`** → `hasMajority` 必须返回 false（不得退化为"1 即多数"）；
- **`version == 0`**（seed 配置）与首个配置条目（`index >= 1`）比较 → 必须严格 `< >` 判定，不接受相等覆盖；
- **`currConfig_` 中不含 self** → 必须走退役路径，不得继续以 Leader/Candidate 身份行动；
- **transport 地址缺失**（配置有 id 但 transport 无地址）→ 视为不可达 peer，记录并跳过，不得崩溃。

### 6.5 单元测试前置假设

- `FakeClock` 单调、`tick()` 由测试手动驱动；`MemoryTransport` 同步投递、回调可重入（既有结论）；
- 测试用的配置条目 index 必须由 `propose` 自然产生，不得手工伪造不连续 index；
- harness 只允许"追加"辅助函数（`makeMembershipCluster` 等），M1/M2/M3 既有用例与 `makeCluster`/`makeSnapshotCluster` 断言不变；
- A 组不得触碰真实磁盘/网络；B 组必须使用独立临时目录并在结束后清理。

---

## 7. 设计前提核对（#1 的核心产出）

### 7.1 已核对成立的前提（证据 = 代码位置）

| # | 设计前提 | 证据 | 结论 |
|---|---|---|---|
| P1 | 配置条目可复用 `LogEntry` 落盘布局（41B 定长 + key/value），不需要动文件格式 | `file_log_store.cpp:30`（`kEntryFixedLen=41`）、`message.cpp:36/45` | ✅ 成立 |
| P2 | 旧日志（op=1/2/3）解析路径可保持不变 | `message.cpp:35-41`、`file_log_store.cpp:123-126` 均为白名单校验，追加 kConfig 即可 | ✅ 成立 |
| P3 | `decodeClientRequest` 不改也能工作，且能阻止客户端伪造配置条目 | `message.cpp:229-237` | ✅ 成立（并成为一条安全性质） |
| P4 | `RKS1` 可在不改 v1 语义的前提下扩到 v2 | `file_snapshot_store.cpp:118-132`（长度校验在 :127） | ✅ 成立 |
| P5 | `SnapshotData` 加字段的改动面很小 | `snapshot_store.h:12` + 使用点 `raft_node.cpp:56/623/692`、两 store 透传 | ✅ 成立 |
| P6 | 成员集合的使用点集中、可枚举 | `cfg_.peerIds` 共 **8 处 / 6 个函数**（§5.1 第 2–7 项） | ✅ 成立（改动可控） |
| P7 | 新消息可在既有分发链内接入 | `main_raft_node.cpp:94-160`（if-else 链，:109/117/125/137/151） | ✅ 成立 |
| P8 | 客户端拓扑缓存有单一挂点 | `main_raft_client.cpp:114 request()`、`:144 requestOnFd()` | ✅ 成立 |
| P9 | `Transport` 加默认实现方法不会破坏测试桩 | `transport.h:21-27` 仅 3 个纯虚；`MemoryTransport` 按 id 扩容（`memory_adapters.cpp` `addNode`） | ✅ 成立 |
| P10 | M3 基线全绿，可直接在其上增量开发 | §0 实测（6 项 PASS、35/35） | ✅ 成立 |
| P11 | GET 短路点就是 D-2 的落点 | `raft_node.cpp:470-477` | ✅ 成立 |

### 7.2 未发现需要改动算法本身的前提

选主、复制、§5.4.2、组提交、快照/安装、D2/D3 均**不需要改算法**，M4 只改"成员集合来源 + 配置状态机 + 读路径 + 地址簿"。

### 7.3 本轮发现的设计偏差（已回退 #0 修订）

| # | 偏差 | 处理 |
|---|---|---|
| **D1** | m4-design.md v1.0 §4.2 写"三处 op 白名单需追加 kConfig（message.cpp 2 处 + file_log_store.cpp 1 处）"。核对后 `message.cpp` 的两处中，`decodeClientRequest` **不应**扩展（客户端不得伪造配置条目），实际只需 **2 处**：`decodeLogEntry`(:38) 与 `FileLogStore::decodeEntry`(:123) | **回退 #0**：m4-design.md §4.2 修订、§12 记录 **v1.1**；本文件 §5.1 第 11/13/14 项按修订后口径执行 |

---

## 8. 自检记录

1. **占位符**：无 `TBD/TODO/待定`；所有行号、编号、超时值、用例编号均为具体值。
2. **与设计一致性**：不变量 J1–J5 与 m4-design §2.2 一致；锁纪律 L10/L11 与 §6 一致；改动点清单与 §10 文件清单一致（唯一差异是本轮 D1 修正，已回退 #0）。
3. **可执行性**：§5.1 每条都能直接定位到函数与行号；§3 的 3 条判定可 grep 复核；§8.1/8.2 用例编号与 m4-design §8 测试矩阵一一对应。
4. **范围**：仅覆盖 M4 子系统；M5 项与完整 Joint Consensus 明确不在本文件范围。
