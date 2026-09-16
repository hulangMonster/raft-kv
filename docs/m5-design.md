# M5 设计：性能优化与可观测性（v1.0）

> 里程碑：M5（性能与可观测性）
> 前置：M1 单机 KV+WAL、M2 Raft 选主+复制、M3 快照与日志压缩、M4 成员变更+客户端路由+线性一致读（tag `m4-membership`，`28e1e37..dc6c56a`）
> 本文档是 M5 的唯一权威设计；任何偏离必须在本文件 §15 追加修订记录。

---

## 0. 文档状态与授权记录

- 阶段：**#0 /brainstorming 产出**（architectural 路径）。
- **授权记录**：用户于本次会话逐条选定了 8 项开放决策（见 §2），随后明确表示"没有异议，直接进行不用批准"，
  **豁免 #0 硬闸门**并授权连续执行 #0→#4。因此本文件在无逐节确认的情况下定稿；决策本身均来自用户的显式选择，
  未由实现者擅自决定。
- 设计原则：**先量化，再优化**；宁可少做也不破坏 M1–M4 已固化的语义。

## 1. 目标 / 非目标 / 验收（决策①，用户选定：A）

### 1.1 目标

1. **消除锁内 IO**：把 fsync 移出 `RaftNode::mu_` 临界区（锁内只保留 page-cache 级 `write`），
   ticker / propose / 读路径不再被磁盘延迟直接阻塞。
2. **Transport 并发化**：epoll/Reactor 事件循环 + 每 peer 长连接、请求-响应配对；
   一个慢/挂掉的 peer 不再阻塞其它 peer 的复制与读。
3. **复制与快照效率**：批大小与合并调优、tick no-op 绕组提交、快照流式序列化（内存峰值 O(块)）、
   InstallSnapshot 断点续传。
4. **可观测性与压测故事**：进程内指标（qps、p50/p99 写延迟、fsync 次数/耗时、组提交批大小、
   复制 lag、选举/快照次数、锁等待、Reactor 在途请求）+ perf/火焰图流程 + **同机 A/B 对比表**。

### 1.2 验收口径（冻结）

| 项 | 基线（`dc6c56a` 实测，见 §3） | M5 目标 | 判定方式 |
|---|---|---|---|
| pipeline=1 写延迟 | **16.4 ms/写** | **≤ 8 ms/写** | `bench_m5_ab.sh` 同档 3 次取中位数 |
| pipeline=64 吞吐 | **589 qps** | **≥ 1200 qps** | 同上 |
| 锁内 fsync 次数 | 每次写 ≈1.1 次（follower 侧全在锁内） | **== 0** | spy `LogStore` 断言 + 端到端用例 |
| 写正确性 | — | 每次 `fill` 后 `verify` **missing 0** | 所有基准脚本 |
| 回归 | M1 13/13、raft 68/68、4 个 e2e/fault 脚本 | 全部保持 PASS、构建 0 warning | 全套脚本 |
| 并发正确性 | M2/M3 仅 ASan | **TSan 通过**（Reactor 引入后为硬门禁） | `-fsanitize=thread` 全量 raft 用例 |

### 1.3 非目标（禁止纳入 M5）

完整 Joint Consensus、Learner 日志追赶、自动扩缩容、跨机房/多区域、多分片；
`RKS1` 快照布局与 M2 entry 布局的破坏性变更（只允许"只增不改"）；
gRPC/TLS/鉴权/服务发现；替换存储引擎（RocksDB/LevelDB）、`O_DIRECT`、`io_uring`；
重写共识算法或改变 §5.4.2 提交规则、组提交语义、快照 D2/D3 语义。

### 1.4 前置成果（已落地，禁止当新工作重做）

- group commit：`appendNoSync()`/`sync()`、`syncedIndex_` 门控、`syncInFlight_` 单 flusher、
  `buildAppendEntries` 只复制已 durable 条目。
- 客户端并发：`raftkv_raft_cli fill <N> --pipeline K`（每 worker 独立 `clientId`）+ `verify <N>`。
- 线性一致读（ReadIndex + §8 同任期屏障）、成员变更全套（J1–J5）、客户端拓扑缓存。
- `awaitCommit()` 的 `log_.sync()` **已经在锁外**（M3 组提交 + #4 评审的成果）——leaders 的 propose 路径
  不需要再改；M5.2 要改的是 **follower ACK 路径、tick no-op、`persistMeta` 三处、`compact` 两处**。

## 2. 决策记录（用户逐条选定，①–⑧）

| # | 决策 | 选定 | 关键理由 |
|---|---|---|---|
| ① | M5 主线与主指标 | **A 性能为主 + 可观测性配套**，目标 ≤1/2 写延迟、≥2× qps | 基线实测瓶颈明确（fsync 1.4–2.8ms/次、每 RPC 建连、transport 全局锁） |
| ② | 两段式持久化形态 | **A 锁内 write / fsync 出锁 + 回锁确认** | 直接命中"锁内 fsync == 0"，不引入新线程；B（IO 线程+队列）留作演进 |
| ③ | Transport 并发模型 | **B epoll/Reactor 事件循环** | 用户选择更激进的方案；彻底解除"一个慢 peer 拖住所有 peer"，心跳/复制可解耦 |
| ④ | 复制批处理 | **A 单批在途 + 批大小/合并调优 + no-op 绕组**；**B 滑动窗口列为条件触发项** | 触发条件：M5.4 收尾后 pipeline=64 < 1200 qps |
| ⑤ | 锁粒度边界 | **A 保持单 `mu_`，只把 IO 移出** + 禁止清单 | 全部不变量建立在"单锁一致视图"；实测瓶颈不在 `mu_` 的 CPU 时间 |
| ⑥ | 可观测性载体 | **A `status` 一行 `k=v` 为主 + 可选 `/metrics`，共用同一指标源** | 既有脚本可直接断言；端点作为加分项 |
| ⑦ | 快照/日志优化范围 | **A 只做快照流式化 + InstallSnapshot 断点续传** | 分段日志要动 M3 恢复/边界/compact 语义，收益与主指标无关 |
| ⑧ | 基准与对比方法学 | **A `bench_m5_ab.sh` 同机 A/B + 落档**；`iptables/tc` 真分区列为可选加分 | 跨会话数字不可比是 M4 的痛点，必须交替测量 + 中位数 + verify |

## 3. 基线证据（M4 收尾版 `dc6c56a`，本次会话连续实测）

原始证据文件：VM `/tmp/m5-baseline-dc6c56a.txt`。机器：nproc=8、load≈0.04、mem 7.8G（avail 6.1G）、`/` 使用率 79%。

### 3.1 吞吐 / 延迟（`scripts/bench_group_commit.sh`，3 节点，`--snapshot-threshold 5000`）

| pipeline | n | 耗时 | qps | verify |
|---|---|---|---|---|
| 1 | 5000 | 82.1 s | **61**（≈16.4 ms/写） | missing 0 |
| 8 | 20000 | 98.0 s | **204** | missing 0 |
| 64 | 20000 | 34.0 s | **589** | missing 0 |

> README/roadmap 记录的 34/175/814 属另一次机器状态。**跨会话绝对数字不可比**，M5 的一切对比只认同机交替测量。

### 3.2 fsync 成本（`fill 200 --pipeline 1`，节点运行在 `strace -c -f -e trace=fsync,fdatasync` 下）

| 节点 | fsync 次数 | 累计 | 平均 |
|---|---|---|---|
| node2（leader） | 221 | 0.593 s | 2757 µs |
| node1（follower） | 221 | 0.302 s | 1365 µs |
| node3（follower） | 219 | 0.304 s | 1389 µs |

⇒ **≈1.1 次 fsync/写/节点**；单次 fsync **1.4–2.8 ms**；follower 侧这 ~0.3 s **全部发生在 `mu_` 内**。

### 3.3 持 `mu_` 期间发生 IO 的调用点（`src/raft/raft_node.cpp` 行号为 `dc6c56a`）

| 位置 | 调用 | 类型 |
|---|---|---|
| L118 `becomeFollower` | `log_.persistMeta` | fsync |
| L165 `startElection` | `log_.persistMeta` | fsync（I5：发投票前必须 durable） |
| L300 `tick`（no-op） | `log_.append` | write + fsync（每任期一次） |
| L373 `onRequestVote` | `log_.persistMeta` | fsync（I5：授权前必须 durable） |
| L420/431/449 `onAppendEntries` | `truncateSuffix` / `append` / `sync` | **最热路径** |
| L561 `appendEntryLocked` | `log_.appendNoSync` | lseek + write（无 fsync） |
| L732 `maybeSnapshot` | `log_.compact` | fsync + rename + fsyncDir |
| L800/804/806 `onInstallSnapshot` | `truncateSuffix` / `sm_.restore` / `compact` | 混合 |

已出锁（M3/M4 成果）：`awaitCommit` 的组提交 `sync`、所有 `transport_*` 发送、`snapshotView` 拷贝、`drainPeerQueues`。

### 3.4 其它事实

- `TcpTransport::roundTrip`（`transport_tcp.cpp:118`）用**一把全局 `mu_`** 串行所有 peer 的所有 RPC，
  且**每次 RPC 内部建连**；RPC 超时 30 ms → 一个慢 peer 可拖住整轮 tick 与所有 peer。
- `FileLogStore::append()` = `appendNoSync()` + `sync()`；`appendNoSync` 每条目 `lseek`+`write`（不持 store 锁），
  `sync()` 持 store 锁做 fsync。
- `perf` 存在但 `perf_event_paranoid=4` → 非 root 无法采样；火焰图需 sudo 或降 paranoid（M5.1 处理）。
- `main_raft_node` 为 thread-per-connection + `std::_Exit(0)`（跳过析构，靠 OS 回收）→ Reactor 落地时必须改成显式 stop+join。

## 4. 与 M1–M4 的关系、不变量增补、禁止清单

### 4.1 复用

`LogStore`/`Transport`/`Clock`/`SnapshotStore` 注入 seam、`MemoryTransport`（测试确定性）、组提交、
快照两段式生成、`syncedIndex_` 门控、成员变更全套（J1–J5）、ReadIndex 探针与 §8 屏障、客户端拓扑缓存。

### 4.2 新增 M5 不变量（写入 `docs/m5-prerequisites.md` 的 I9–I14）

- **I9**：持 `mu_` 期间**不发生 fsync、不发生任何网络 IO**（可被 spy 断言；锁内允许 page-cache `write`）。
- **I10**：一次 fsync 摊一批（组提交不退化），且**只有 durable 之后**才对外声明持久（`syncedIndex_` 语义不变）。
- **I11**（= I5 的工程化表述）：**回复 `AppendEntries success` / `RequestVote granted` 之前，该批 entries /
  `term+votedFor` 必须已 durable**——由"锁内决定 → 解锁落盘 → 回锁校验后回复"保证。
- **I12**：异步发送不改变 §5.4.2 / J2 / §8 屏障的**判定输入集合**（只改投递方式）。
- **I13**：指标只读、无副作用、**不得成为任何正确性判定的输入**。
- **I14**：性能优化不得改变 wire 格式、磁盘格式与对外错误码语义（`RKS1` v1 仍必须可解）。

### 4.3 禁止清单（"看起来很香但会破坏不变量"）

1. 无锁 `commitIndex_`（§5.4.2 与 J2 的判定需要与 `currConfig_/prevConfig_/matchIndex_` 同一临界区）。
2. 快照边界 `lastIncluded_` 与 `lastApplied_` 分离到不同锁（D3 的"边界必须先于 load 校验"）。
3. `matchIndex_`/`nextIndex_` 无锁读（多数派判定会看到撕裂视图）。
4. `drainingPeers_`/`inFlightConfigIndex_` 移出 `mu_`（J1/J2 与送达目标的一致性）。
5. 用原子替换 I5 的持久化顺序（投票/ACK 的 durable-before-reply 顺序）。
6. 为省一次拷贝而让 `snapshotView()` 在锁外做序列化（L8 的"锁内只做内存拷贝"反过来也成立）。

## 5. 文件面

### 5.1 新增（优先）

| 文件 | 职责 |
|---|---|
| `src/raft/metrics.{h,cpp}` | 进程内计数器 + 分桶延迟直方图 + 渲染（`status` 片段 / `/metrics` 文本）；`Metrics* = nullptr` 时零改动接入 |
| `src/raft/reactor.{h,cpp}` | epoll 事件循环 + 每 peer 连接状态机（nonblocking connect/read/write、帧解析、写队列、请求-响应配对、超时清理、关闭） |
| `src/raft/transport_reactor.{h,cpp}` | 以 `Transport` 接口实现的 Reactor 版；`sendX` 入队，回调在 reactor 线程触发 |
| `src/raft/log_codec.{h,cpp}` | 消除 `message.cpp` 与 `file_log_store.cpp` 两份 LogEntry 编解码（M2 评审 O4） |
| `tests/raft_perf_test.cpp` | M5 契约用例（A/B 组，确定性、零 flaky） |
| `scripts/bench_m5_ab.sh` | 同机交替 A/B + 中位数 + `verify` + 落档 |
| `scripts/perf_flame.sh`（可选） | perf 权限检测 + 采样 + 折叠栈输出 |

### 5.2 最小修改

`src/raft/raft_node.{h,cpp}`（8 个 IO 点两段式 + 指标挂点）、`src/raft/transport_tcp.{h,cpp}`（保留为**同步对照引擎**）、
`src/raft/file_log_store.cpp`、`src/raft/file_snapshot_store.cpp`（流式 + `.recv` 头）、
`src/raft/kv/kv_state_machine.{h,cpp}`（流式快照视图，只增不改语义）、
`src/main_raft_node.cpp`（指标 + 显式 join reactor + 优雅关闭）、`CMakeLists.txt`。

### 5.3 禁止改动

M1 全部源码与测试、`scripts/e2e.sh`；M2 §5.4.2 提交规则与 wire 字节布局；M3 `RKS1` 布局与 D2/D3 语义；
M4 `J1–J5`、`membershipMu_` 叶子锁、ReadIndex §8 屏障、"提交 ≠ 送达完成"的送达语义。

## 6. 两段式持久化协议（决策② = A）

### 6.1 状态与不变量

- `syncedIndex_`：已 durable 的最高日志 index（不变）。
- `syncInFlight_`：单 flusher 标志（不变）。
- **新**：`metaPersistMu_`（叶子锁，锁序 `metaPersistMu_ → mu_`）串行化"决定 term/votedFor → 落盘 → 回锁校验"，
  保证**磁盘上的 meta 版本单调**（低 term 的落盘绝不能晚于高 term 的落盘）。
- **新**：`metaDirty_` 标志：`becomeFollower` 在锁内更新 `currentTerm_/votedFor_` 后置位；
  由 tick 的 flush 步骤（锁外）落盘，保证任期变更在 ~10ms 内 durable。

### 6.2 leader propose 路径（已是两段式，保持）

`appendEntryLocked`（锁内 `appendNoSync` + 配置条目追加即生效）→ `awaitCommit`（锁外 `sync()` → 回锁推进 `syncedIndex_`）。
**唯一改动**：`appendNoSync` 内部仍有每条目 `lseek+write`——保留（page-cache，微秒级；符合 I9）。

### 6.3 follower `onAppendEntries` 路径（I11 的关键改造）

```
lock(mu_)
  term/prevLogIndex/冲突检查（不变）
  appendNoSync(toAppend)                # 只 write，不 fsync
  记录 lastMatched；needSync = (lastMatched > syncedIndex_)
unlock
if needSync: log_.sync()                # 锁外 fsync（1.4–2.8ms 不再阻塞 ticker）
lock(mu_)
  若 currentTerm_ 已变 → 回 {currentTerm_, false}          # 不得 ack
  若 sync 失败 → 回 false（syncedIndex_ 不推进，沿用 M3 B2 语义）
  夹紧 durable = min(lastMatched, log_.lastIndex())
  syncedIndex_ = max(syncedIndex_, durable)
  applyAppendedConfigLocked(toAppend)   # 配置生效推迟到"已 durable"之后（比现状更保守，见 §13 风险 2）
  leaderCommit 推进 commitIndex_ + advanceCommitAndApply()
  构造并返回 success=true               # 此时 I5 已满足
unlock
```

### 6.4 I5 元数据（`persistMeta`）三处

| 场景 | 协议 |
|---|---|
| `startElection`（自投票） | 持 `metaPersistMu_` → 锁内 `++currentTerm_`、`votedFor_=self` → **解锁 mu_** → `persistMeta` → 回锁校验 `currentTerm_` 未变 → 生成 voteJobs → 解锁 → 发送 |
| `onRequestVote` 授权 | 持 `metaPersistMu_` → 锁内判定可授权并置 `votedFor_` → 解锁 → `persistMeta` → 回锁校验 term 未变 → 返回 `granted=true` |
| `becomeFollower`（任意路径） | 锁内只更新内存 + `metaDirty_=true`；tick 的 flush 步骤（锁外）`persistMeta` 并清标志。**降级回复不依赖它**（崩溃只会忘记 term，Raft 可自愈；而"忘记自己投过票"才会破坏 I3） |

### 6.5 `compact` 出锁（`maybeSnapshot` / `onInstallSnapshot`）

锁内校验（epoch、边界、`snapIndex <= commitIndex_ && > lastIncluded_`）→ 解锁 → `log_.compact()` →
回锁复核（`installEpoch_` 是否变化、边界是否仍成立）→ 更新 `lastIncluded_/lastIncludedTerm_/snapshotBytes_`。
若期间 epoch 变化（更新快照落地），本次 compact 结果保留但字段更新让位（更靠后的边界以新快照为准）。

### 6.6 崩溃契约（`kill -9` 语义，供 B 组用例逐条断言）

| 时点 | 期望 |
|---|---|
| `appendNoSync` 后、`sync` 前 | 该批**可以丢失**；不得被计入 `commitIndex_`（`syncedIndex_` 未推进）；重启后从磁盘看是 torn/缺失尾部，`load()` 截断 |
| `sync` 返回后 | 该批**必须可见**；重启后 `log_.lastIndex()` 覆盖它 |
| `sync` 失败 | 不推进 `syncedIndex_`，不回复 `success`；客户端写路径返回 `kErr`（沿用 M3 B2） |
| follower 回复 `success` 之后 | 该批必已 durable（I11）；leader 才会计入多数派 |
| `persistMeta` 返回后 | `term/votedFor` 必须可见；重启后不得重复投票（I3） |
| `compact` 期间 | 目录 fsync 前崩溃 → 旧快照仍可用（M3 D2：先 durable 快照后 compact） |

### 6.7 断言方式

`SpyLogStore`（继承 `MemoryLogStore`）记录"持锁窗口内的 `sync/append/compact/persistMeta` 调用序列"：
实现侧用 `MuHeldGuard`（RAII）+ 线程局部标志把"是否持锁"传给 spy（**仅测试构建启用**，生产零成本），
用例断言**持锁期间 sync 调用数 == 0**。

## 7. Transport / Reactor（决策③ = B）

### 7.1 组件

- `Reactor`：单线程 epoll 循环（`epoll_wait` + 事件分发），持有 `peer → Conn` 表。
- `Conn` 状态机：`CONNECTING → READY → CLOSING/CLOSED`；非阻塞 `connect`、读缓冲（复用 length-prefix 帧）、
  写队列（部分写 + `EPOLLOUT` 回填）、`seq → cb` 在途表、每请求超时。
- `TransportReactor`：实现既有 `Transport` 接口；`addPeer/removePeer` 建/拆连接；`stop()` 显式停止并 join。

### 7.2 接口语义（签名不变、语义变化）

- `sendX(peer, args, cb)`：**入队即返回**；`cb` 在 reactor 线程触发（成功=对端应答；超时/连接错误=不触发，与今日同步超时行为一致）。
- `MemoryTransport` **保持同步触发**（测试确定性）+ 新增 `AsyncMemoryTransport`（把回调投递到独立线程）用于真异步路径测试
  → 两个引擎可对照，Reactor 出问题时可回退到 `TcpTransport`。
- 调用点兼容性论证：所有 `sendX` 都在锁外调用（L2/L4），所有 `onXxxReply` 自己获取 `mu_`
  → **异步回调天然安全**，这是本次敢上 Reactor 的前提。

### 7.3 生命周期与悬垂回调

- `removePeer(id)`：关闭连接 + **丢弃该 peer 在途回调**（不触发 cb）+ 释放缓冲；用例断言"移除后不再有该 peer 的回调"。
- 节点析构 / 进程退出：`stop()` 停止 epoll 循环 → 关闭所有连接 → join → 之后再析构 `RaftNode`
  （替代现在的 `_Exit(0)`；M5.3 必须同时删除 `_Exit` 并让 ticker/连接线程显式 join）。

### 7.4 门禁

TSan（全量 raft 用例 + `raft_perf_test`）必须 0 报告；注入用例：挂起 peer、乱序/重复 ack、连接中断重连、
`removePeer` 与在途请求竞争。

## 8. 复制与快照（决策④ = A + 条件 B；决策⑦ = A）

- **8.1 批处理与 no-op 绕组**：`maxEntriesPerAppend`（128）/`maxBytesPerAppend`（1MiB）按实测调优；
  `tick` 的 no-op 改走 `appendNoSync` + 锁外 fsync（§6.2 的 `syncLogOutsideLock` 辅助），
  使 `syncedIndex_` 门控不因 no-op 卡住读路径（否则 §8 屏障永不满足 → 读永远失败，见 §13 风险 1）。
- **8.2 滑动窗口（条件触发）**：每 peer 允许 N 批在途 + `nextIndex_` 乐观推进；触发条件：
  M5.4 收尾后 pipeline=64 < 1200 qps。落地时需配"乱序/重复 ack 幂等 + `matchIndex_` 单调"用例。
- **8.3 快照流式序列化**：`SnapshotView` 增加流式接口（`nextChunk()` + 增量 CRC），
  `FileSnapshotStore::save` 分块写临时文件 + fsync + rename + fsyncDir；内存峰值从 O(状态) 降到 O(块)。
  断言：单次快照期间进程 RSS 增量 ≤ 块大小 × 常数。
- **8.4 InstallSnapshot 断点续传**：`.recv` 文件头追加 `(index, term, receivedLen, crcSoFar)`；
  接收端重启后按 `receivedLen` 续传，`nextOffset` 回报已收长度（字段 M3 已预留）；
  接收端校验 (index,term) 不匹配则丢弃重传。

## 9. 指标（决策⑥ = A）

指标源：`Metrics`（原子计数器 + 分桶直方图），`RaftNode`/`reactor`/`FileLogStore` 通过可选指针挂点。

| `status` 字段 | 含义 |
|---|---|
| `qps` | 最近窗口内完成的客户端写数 / 秒（窗口由采样时间戳做差） |
| `lat_p50_us` / `lat_p99_us` | 写延迟分位（1/2/5/10/20/50/100 ms 分桶 → 线性插值） |
| `fsync_calls` / `fsync_ms` | 累计 fsync 次数与总耗时 |
| `batch_avg` / `batch_max` | 组提交批大小（条目数） |
| `repl_lag_max` | leader 视角：`commitIndex_ - min(matchIndex_ of voters)` |
| `elections_total` / `snapshots_total` / `snapshot_bytes` | 计数 |
| `lock_wait_us_total` / `lock_wait_max` | `mu_` 获取等待（`try_lock` 失败次数亦可） |
| `inflight_rpc` | Reactor 在途请求数（M5.3 起） |

可选 `/metrics`：新增 **msgType 15 = kMetricsRequest**（只增不改），返回 `text/plain` 指标文本；与 `status` **共用同一份指标源**。
开销目标：单次采样 < 1%（M5.5 用 A/B 验证：开启/关闭指标的 qps 差异 < 1%）。

## 10. 基准与对比方法学（决策⑧ = A）

`scripts/bench_m5_ab.sh`：
1. `git worktree add /tmp/m5base dc6c56a` + 独立 build 目录 → 产出 M4 基线二进制；
2. 同一脚本内 **M4→M5→M4→M5** 交替运行（同机、同阈值、同网络端口段错开）；
3. 参数矩阵：pipeline ∈ {1,8,64}，n = {5000, 20000, 20000}，`--snapshot-threshold 5000`；
4. **预热**：每档先跑 200 写并丢弃；**每档 3 次取中位数**；
5. 每次 `fill` 后 `verify`，**任何一次 missing != 0 则整轮作废**；
6. 判定：同档中位数 **延迟 ≤ M4×0.9 或吞吐 ≥ M4×1.5** 记为"改善"，否则记"未达标"（防噪声自欺）；
7. 产物：`docs/m5-bench.md`（对比表 + 机器状态 + 完整命令 + 原始输出路径），原始日志落 `/tmp/m5bench-logs/`（不入库）。

## 11. 测试矩阵

### A 组（契约，确定性：FakeClock + Memory* 适配器，零 flaky）

| # | 用例 | 断言 |
|---|---|---|
| M5.A1 | `LockHeldFsyncIsZero` | spy：持锁窗口内 `sync/append(durable)/compact/persistMeta` 调用数 == 0 |
| M5.A2 | `GroupCommitBatchesFsync` | 一次 flush 的 fsync 次数 ≤ 批次数；且 `syncedIndex_` 只在 sync 成功后推进 |
| M5.A3 | `FollowerAcksOnlyAfterDurable` | 用 BlockingLogStore 卡住 sync → 该批不可见 ack；放行后才 ack |
| M5.A4 | `VoteGrantedOnlyAfterMetaDurable` | 卡住 persistMeta → 不返回 granted；放行后 granted |
| M5.A5 | `AsyncAckIdempotentAndMonotonic` | 重复/乱序 ack 下 `matchIndex_` 单调、不越界 |
| M5.A6 | `NoDanglingCallbackAfterPeerRemoval` | `removePeer` 后在途回调不触发 |
| M5.A7 | `MetricsAreMonotonicAndReadOnly` | 计数器单调；`status` 指标不改变任何共识状态 |
| M5.A8 | `NoopWrapsGroupCommitAndUnblocksRead` | no-op 走 `appendNoSync` + 锁外 fsync；§8 屏障在下一 tick 满足 |

### B 组（磁盘/进程级）

| # | 用例 | 断言 |
|---|---|---|
| M5.B1 | `AckedWriteSurvivesKillBeforeSync` | fsync 前 kill -9 → 该写不得被 ack（本轮不 ack）；fsync 后 kill -9 → 重启可见 |
| M5.B2 | `StreamedSnapshotKeepsV1V2Compat` | 流式写出的快照可被既有 `RKS1` v2 解码；v1 仍可解 |
| M5.B3 | `InstallSnapshotResumeAfterRestart` | 半传后重启 → 按 `receivedLen` 续传成功；`.recv` 头不匹配则重传 |
| M5.B4 | `CompactOutsideLockKeepsBoundary` | compact 期间注入并发 InstallSnapshot → 边界不回退 |

### P 组（脚本层，关系断言，不进 gtest）

`bench_m5_ab.sh` 的中位数阈值判定、锁内 fsync == 0（端到端 spy 构建）、每次 `verify missing 0`、
指标开启/关闭的 qps 差异 < 1%。

## 12. 里程碑拆分与通过判据

| 阶段 | 内容 | 通过判据 |
|---|---|---|
| **M5.1** | `metrics.{h,cpp}` + `status` 扩展（+ 可选 `/metrics`）+ bench 脚本参数化 + perf 权限处理 + **冻结 M4 基线** | 指标可脚本断言；`docs/m5-bench.md` 基线段落档；M1/raft 全绿 |
| **M5.2** | 两段式持久化（§6 全部：follower ACK、no-op、I5 三处、compact 两处） | M5.A1–A4/A8 + M5.B1 通过；**锁内 fsync == 0**；`raft_fault`/`raft_snapshot_fault` `--repeat 50` 绿 |
| **M5.3** | Reactor（§7）+ 同步引擎保留对照 | M5.A5/A6 + 挂起 peer/重连注入；**TSan 全绿**；pipeline=1 延迟显著下降 |
| **M5.4** | §8 全部（批处理/no-op/流式快照/断点续传；滑动窗口条件触发） | M5.A8/B2/B3/B4；**pipeline=1 ≤8 ms/写** 且 **pipeline=64 ≥1200 qps** |
| **M5.5** | A/B 对比表 + 火焰图 + README/roadmap + #4 独立评审 | `bench_m5_ab.sh` 判定全过 + 全部脚本 PASS + 对比表与方法学落档 |

## 13. 风险与对策

1. **no-op 卡住读路径**（高）：no-op 若走 `append()` 但 `syncedIndex_` 未推进，`buildAppendEntries` 不会复制它 →
   leader 永远无法提交本任期条目 → §8 屏障永不满足 → **所有线性一致读失败**。对策：§8.1 的锁外 fsync +
   M5.A8 用例（先写用例再改）。
2. **follower 配置生效时机变化**（中）：把 `applyAppendedConfigLocked` 推迟到 durable 之后，比现状更保守；
   必须验证 J1–J5 与 A20–A29 全部回归（尤其 A24 在途配置快照、A25 重启在途、A29 送达确认）。
3. **Reactor 并发与生命周期**（高）：悬垂回调、部分写、重连风暴、关闭顺序 → TSan + A5/A6 + `removePeer` 竞争用例；
   保留 `TcpTransport` 作为回退对照引擎。
4. **两段式"回锁后状态已变"**（中）：每个回锁点都必须重校验 `currentTerm_/role_/lastIndex()/installEpoch_`；
   用例覆盖"解锁窗口内发生 step-down / truncate / InstallSnapshot"。
5. **meta 落盘乱序**（中）：靠 `metaPersistMu_` 把"决定+落盘+复核"串行化，保证磁盘版本单调（§6.1）。
6. **断点续传的半写**（中）：`.recv` 头带 `(index,term,receivedLen,crcSoFar)`；不匹配即丢弃重传。
7. **指标开销/自欺**（低-中）：指标只读、目标 <1% 开销；基准判定用中位数阈值 + `verify`。
8. **快照流式化破坏格式**（中）：只改"如何写出"，不改 `RKS1` 字节布局；M5.B2 守门。

## 14. 自检记录（占位符 / 矛盾 / 歧义 / 范围）

1. **占位符扫描**：无 `TBD/TODO/待定`；目标数字、指标名、msgType（15）、分桶、参数矩阵均为具体值。
2. **矛盾消解**：
   - 决策② "锁内只 write" 与 I9 "锁内无 fsync/网络 IO" 一致（`write` 是 page-cache，不是 durable 点）；
   - 决策③ Reactor 异步回调 与 I5 "回复前 durable" 不冲突：**回复由被调方在回锁校验后返回**，与投递方式无关；
   - 决策④ 滑动窗口为条件项，与"M5.4 判据只看两个主指标"不冲突（达标即不启用）；
   - 决策⑤ 单锁 与 决策③ Reactor（自带线程）不冲突：`Reactor` 不持有 `RaftNode::mu_`，回调入口自己取锁，锁序仍为 `mu_ → transport`。
3. **歧义消除**：`syncedIndex_`/`pendingSync` 语义在 §6.1/§6.2 明确；"锁内"= 持有 `mu_` 的区间；
   "durable"= `LogStore::sync()` 返回 true / `persistMeta` 返回 true。
4. **范围检查**：M5.1–M5.5 每步可独立测试与提交；非目标（§1.3）明确排除；不触碰 M1–M4 语义。
5. **接口稳定性**：`Transport` 签名不变（语义异步）；`LogStore` 不变；`SnapshotData` 只增字段；
   `status` 只增字段（脚本兼容）；新 msgType 15 为纯追加。
6. **可验证性**：§11 每条用例对应 §12 的判据；§1.2 的每个数字都有实测基线（§3）与判定方法（§10）。

## 15. 修订记录

- **v1.0**（#0 brainstorming，用户豁免硬闸门）：8 项决策冻结；基线证据落档；两段式协议（含 I5 三处）、
  Reactor 语义与生命周期、快照流式化/断点续传、指标集合、基准方法学、测试矩阵与里程碑拆分定稿。

- **v1.1**（#2 TDD 阶段，实测 RED 后回填）：
  1. **#2 实测结果**：新增 `tests/raft_perf_test.cpp` 共 7 例，**4 红 3 绿**（75 例总计 71 绿 + 4 预期红）。
     RED：`A1`（持锁窗口内 48 次 durable 调用）、`A2`（follower 新条目路径不显式 sync 且锁被占）、
     `A3`（`persistMeta` 在持锁状态）、`A6`（metrics 仍为计数桩）。
     绿（回归守门，M5 改动不得让其变红）：`A4`（sync 失败不 ack/不推进）、`A5`（no-op 满足 §8 读屏障）、
     `A7`（成员变更仍提交）。
  2. **#2 阶段发现的真实缺陷（评审未提及）**：`onAppendEntries` 的新条目路径**从不显式调用 `sync()`** ——
     它在 `log_.append()` 之后**无条件**把 `syncedIndex_` 推到该 index，等于把"durable"寄托在
     `LogStore::append()` 内含 fsync 这一实现细节上。`FileLogStore::append()` 确实 = `appendNoSync()+sync()`，
     但 `MemoryLogStore::append()` **不含 fsync**，于是 I10/I11（durable 之后才声明/才 ack）在 Memory 适配器上
     根本无法表达、也不可测。M5.2 必须改为显式 `appendNoSync()` + **锁外 `sync()`**，成功后才推进 `syncedIndex_`
     与回复 `success`（对应 M5.A2/A3）。
  3. **脚手架（本阶段新增）**：`src/raft/lock_probe.h`（`ProbedMutex`：lock/unlock 维护 thread_local 持有深度，
     零调用点侵入地把"是否持锁"暴露给测试）、`src/raft/metrics.{h,cpp}`（接口 + 计数桩）、
     `tests/raft_test_harness.h` 追加 `SpyLogStore` / `BlockingLogStore` / `SpyTransport`（仅追加，既有桩零改动）。
     为兼容 `ProbedMutex`，`RaftNode::cv_` 由 `std::condition_variable` 改为 `condition_variable_any`
     （标准要求 `cv` 只接受 `unique_lock<std::mutex>`）——语义等价，等待期间 depth 归零。
  4. **文档化偏差**：B 组 `B2`（流式快照兼容）/`B3`（断点续传）依赖 M5.4 才引入的接口，按子阶段 RED-first
     在 M5.4 补写；`B1`（kill -9 前后可见性）在实现侧无注入 seam（进程内无法制造"未 fsync 即崩溃"），
     由 P 组脚本（`raft_fault --repeat 50` + `verify missing 0`）承担。

- **v1.2**（M5.2 落地后回填两处口径修正 + 一条新发现）：
  1. **I9 的判定口径精确化**：`lockprobe` 改为**按锁类别**计数（`kConsensus`=mu_ / `kMembership` /
     `kMeta`），I9 判定统一用 `lockprobe::consensusHeld()` —— "锁内"专指**共识锁 `mu_`**；
     `membershipMu_`（成员变更串行化）与 `metaPersistMu_`（meta 落盘串行化）是叶子锁，在它们内部
     做 IO 是设计允许的（否则"串行化 meta 落盘"本身就无从实现）。第一版探针把三类锁合并计数，
     导致 A1/A3 把合法的 meta 串行化也算成 I9 违反。
  2. **`tick` 的实际结构**（实现比 v1.0 描述更细）：拆成
     [锁段1：leader 追加 no-op（只 write）/ follower 触发选举] → [锁外1：no-op 的 fsync + 推进
     `syncedIndex_` + `advanceCommitAndApply`] → [锁外2：`flushMetaOutsideLock()`，选举场景下落盘
     失败或任期被取代则**丢弃 voteJobs**] → [锁段2：构造心跳/复制作业] → `drainPeerQueues()` → 发送。
     `becomeFollower` 只置 `metaDirty_`，由锁外2 延迟落盘（~10ms 内 durable）。
  3. **新发现（重要，已修）**：把 `log_.compact()` 移出 `mu_` 会**打破 FileLogStore 依赖的外部互斥**——
     `FileLogStore::appendNoSync()` 刻意不持 store 锁，其正确性建立在"它只在 `RaftNode::mu_` 下被调用，
     而 `compact()/truncateSuffix()` 也在同一把锁下"这一隐含约定上（log_store.h 原注释明确写了）。
     出锁后 append 会与 compact 的 `close/rename/reopen logFd_` 并发 → `log append failed` +
     节点 `Aborted (core dumped)`（被 `raft_snapshot_fault.sh` 的 100k 条目 + 压缩阶段抓到）。
     修复：**FileLogStore 自持锁**（`append/sync/appendNoSync/truncateSuffix` 公共入口统一取 `mu_`，
     内部拆出 `*Locked` 实现避免自死锁），不再依赖 `RaftNode::mu_` 串行化。
     ⇒ **新增工程准则 L16**：任何"把 IO 移出 `mu_`"的改动，必须逐一确认被移出方与**仍留在锁内**的
     调用方之间原有的互斥/顺序关系是否被打破；被移出方若曾借用 `mu_` 做互斥，就必须自己补锁。
