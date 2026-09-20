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

### 1.2 验收口径（冻结；**M5.5 收尾已修订为同机比值口径**，见 §15 v2.7 与 `docs/m5-bench.md` §3.7）

> 下表是**冻结时的期望值**（绝对判据）。收尾时经用户决定改为**同机比值口径**：
> p=1 要求 `M5 延迟 ≤ M4 延迟 × 1.2`、p=8/64 要求 `M5 吞吐 ≥ M4 吞吐 × 0.8`，
> 且每次 `verify missing 0` 为硬门禁。原因见 `docs/m5-bench.md` §3.6：本机单次持久化
> 提交下限 ≈ 8 ms，绝对目标隐含 "flush ≈ 1 ms" 的硬件。

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
| **M5.4** | §8 全部（批处理/no-op/流式快照/断点续传；滑动窗口条件触发） | M5.A8/B2/B3/B4；**同机比值口径**：p=1 延迟 ≤1.2×M4、p=8/64 吞吐 ≥0.8×M4，且每次 `missing 0` |
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

- **v1.3**（M5.2 故障注入复跑后回填）：**"配置条目追加即生效"不能在两段式里改时机**。
  v1.1/v1.2 一度把 `applyAppendedConfigLocked()` 推迟到"本批已 durable 之后"（理由是 I10/I11），
  结果在 `raft_membership_fault.sh` 第 26 轮（SIGSTOP + 期间成员变更）出现 **"配置未收敛"**：
  只要本次 fsync 失败、或回锁时发现任期已被取代（这两种情况我们都不 ack），该配置条目**已经躺在
  日志里**；leader 重传时 `toAppend` 为空（条目已存在）→ 生效路径被 `!toAppend.empty()` 跳过 →
  **该节点永远不会生效这个配置** → 拓扑不收敛（诊断 dump 定位：存活节点的 `cv/members` 分叉）。
  ⇒ 修正回设计原文（决策③）：**配置条目在"追加时"生效**（锁内、写之后立刻 adopt，幂等且受 J3 保护）；
  两段式只把 **fsync 与 ack** 放到锁外/durable 之后，不动生效时机。快照侧不受影响（B3 的
  `snapIndex` 边界取配置保护仍然有效）。
  教训并入 L16 的复核清单：出锁改造要同时核对**被延后的动作是否会被"已存在"分支短路**。

- **v1.4**（M5.2 复跑观测，待下一轮处理）：`raft_membership_fault.sh --repeat 50` 在修复配置生效时机后
  已连续通过 iter 1–8，但**单轮耗时从 ~24s 恶化到 ~4–5min**（脚本 41 分钟只跑到 iter 9）。
  可疑机制（待验证）：`I5` 的元数据落盘被集中到 `tick()` 的锁外阶段 + `metaPersistMu_` 串行化，
  而 `becomeFollower()` 在**每次收到更高 term 的 RPC** 都会置 `metaDirty_` —— 故障注入下任期频繁
  更替 → ticker 反复做 meta fsync（1.4–2.8ms/次，且与 `onRequestVote` 的授权落盘争同一把叶子锁）
  → 心跳节奏被拖慢 → 触发更多选举 → 正反馈。
  **下一轮修法（不改正确性）**：I5 只要求"**授权投票**与**自投票**"在回复/发送前 durable，
  `becomeFollower()` 的 term 落盘只是"尽快 durable"（崩溃只丢失 term，Raft 自愈，不违反 I3）。
  因此把 tick 侧 flush 改为**限频**（例如 ≥50ms 一次，或用单调时间戳门控），必要时退化为
  "不主动 flush，交由下一次授权/选举路径落盘"。改完后重跑该脚本确认单轮耗时回到 ~24s 量级。

- **v1.5**（M5.3 落地 + 完整 A/B 后回填；含一条**未关闭的阻断项**）：
  1. **Reactor 已实现并接入**（`reactor.{h,cpp}` + `transport_reactor.{h,cpp}` + 契约用例 R1–R5 +
     `--transport=reactor|sync`）；TSan 全绿（0 警告，含异步引擎的并发面）。
  2. **⚠️ 未关闭的阻断项（M5.3 完成判据未达成）**：完整 A/B 在 **pipeline=64** 下复现
     `verify missing 1–2` —— **已 ack 的写不可见**。根因是**应答缺少关联号**：M2 的
     `onAppendEntriesReply` 用 `lastSentEndIndex_[peer]`（"最近一次发送的末尾 index"）归因，
     这只在"同步发送"（sendX 返回时已拿到应答）下成立。异步引擎即使加了"每 peer 单批在途"
     门控（超时 2×rpcTimeoutMs 后允许重发），仍存在**迟到应答落在下一批发送之后**的窗口 ->
     归因错配 -> `matchIndex_` over-count -> leader 认为已复制到多数派并 ack。
     **彻底修法（下一轮第一件事）**：在 `AppendEntries`/`InstallSnapshot` 请求里携带单调 `seq`，
     应答原样回显；用 `(peer, seq) -> sentEndIndex` 精确配对（**只增字段**：新 msgType 16/17 或在
     现有布局尾部追加，保留旧解码路径与 RKS1 兼容）。修完必须重跑：A5/A6 + reactor 侧新增
     "迟到应答"用例 + `fill 20000 --pipeline 64` ×3 次 `missing 0` + 全部 e2e/fault。
     在此之前**默认引擎保持 sync**（`--transport=reactor` 显式开启），避免把带丢写风险的引擎
     作为默认配置交付。
  3. **性能现状（完整 A/B，3 次中位数，同一次脚本交替测量；本机当时状态比冻结基线时慢约 2×）**：
     M4 基线 p=1 33.9 ms/写(30 qps) / p=8 8.7(115) / p=64 2.15(464)；
     M5(sync) p=1 25.7(39) / p=8 7.64(131) / p=64 待重测（本次 M5 列因丢写作废）。
     => **相对改善成立**（p=1 延迟 0.76×、p=8 延迟 0.87×/吞吐 1.15×），但**绝对验收数字未达成**
     （目标 p=1 ≤8 ms/写、p=64 ≥1200 qps）。机器状态漂移（同一 M4 基线从冻结时的 15.05 ms/写
     漂到 33.9 ms/写）是主因之一，必须按 §10 的"同轮交替"口径判定，并在 M5.5 重新冻结目标口径
     或明确记录未达成。
  4. **另有一处崩溃待查**：A/B 过程中出现 2 次 node `SIGSEGV`（core dumped），发生在两次压测之间，
     未影响该轮的 `verify` 结果（3 节点有 2 个存活即可提交）。下一轮用 ASan 构建复现并定位。

- **v1.6**（M5 独立评审 #4 后的处置；**新增一条 UB/崩溃级修复**）：
  1. **🔴 已修：FileLogStore 数据竞争 → SIGSEGV（UB）**。M5.2 把 `compact()` 移出 `RaftNode::mu_`
     之后，M2 的隐含约定"只读访问器（`lastIndex/lastTerm/termAt/slice`）由调用方的 `mu_` 串行化"
     被打破：compact 在 store 锁下改 `entries_/offsetOf_/lastIncluded_`，而其它线程仍在 `mu_` 下读
     它们。ASan 实测 `SEGV in FileLogStore::termAt`（调用栈 `propose → 连接线程`），独立评审的
     gdb 栈顶为 `FileLogStore::slice:368`。
     **修复**：`FileLogStore::mu_` 改为 `std::recursive_mutex`，**所有**触碰内部状态的入口
     （含 8 个只读访问器与 `setBoundary`）统一持锁；recursive 保证内部互调（compact→lastIndex）
     不会自死锁。验证：ASan 构建下 3 节点 `fill 20000 --pipeline 64` ×2 轮 `missing 0`、
     **ASan 报告 0 条**（修复前同一驱动必崩）。
  2. 同时修掉 `pendingInstallCompact_` 成员变量的竞争（并发 InstallSnapshot 互相覆盖 idx/term）
     → 改为锁外 compact 使用**局部**边界。
  3. **评审提出的仍未关闭项（下一轮）**：
     * **I9 仍有违约**：`onAppendEntries` 的冲突路径在持 `mu_` 时调 `log_.truncateSuffix()`，
       其内部是 `ftruncate + fsync`（评审用 `/tmp/truncprobe2` + strace 实证）。修法：拆出
       "只做 ftruncate + 内存截断"的 `truncateSuffixNoSync()` 供锁内使用，durability 交由同一批的
       锁外 `sync()` 覆盖（fsync 会一并持久化 size 变更）；并扩展 `SpyLogStore`/A1 覆盖该路径。
     * **ack 归因仍非严格原子**：`approveAppendSend` 只在异步引擎做门控，登记与 `sendX` 非原子；
       sync 引擎下 ticker 与 flusher 交错时回调仍可能读到"别人的 end"；reactor 侧门控是计时器，
       若 reactor 线程被长回调/等锁卡住，超时扫描未跑，第二批已入队 -> 旧应答按新批归因。
       **真正闭合的做法**（下一轮）：把应答上下文放进**回调闭包**（`sendX(...,[this,peer,endIndex](reply){...})`），
       不再依赖共享状态；`onXxxReply` 改为携带上下文参数。无需改 wire 格式。
     * `erasePeerStateLocked` 未清理 M5.3 新增的 `appendSentMs_/snapshotSentMs_`；`approveAppendSend`
       在 append 路由写 `snapshotChunkEnd_`（污染在途快照进度）—— 收敛到闭包方案后自然消失。
     * 指标 `onElection/setReplicationLag/setInflightRpc` 尚无生产调用点（status 三项恒 0）——
       接上 `startElection`/leader tick/`Reactor::inflight()`。
     * 仓库卫生：`build-tsan/`（114 个文件）曾被误提交，已加入 `.gitignore` 并从索引移除。

- **v1.7**（M5.3/M5.4 收尾；**关闭 v1.6 列出的全部阻断项**，并新增两条实测不变量 I15/I16）：
  1. **✅ I9 违约已闭合（v1.6 遗留项 3 的第一条）**。锁内冲突回滚改走 `truncateSuffixNoSync()`
     （只 `ftruncate` + 内存截断），durability 交由同批的锁外 `sync()`；并新增 **M5.A9** 端到端用例
     （2 节点注入 `term=99` 幽灵条目 → 驱动 conflict 回滚，断言 `truncates>0 && lockedTruncates==0
     && lockedTruncateNoSyncs>0`）。**过程中 A9 抓到一个真实缺陷**：`MemoryLogStore::truncateSuffixNoSync()`
     原来写的是无限定名 `truncateSuffix(fromIndex)` —— 那是**虚调用**，会落到子类覆写的"含 fsync"
     版本上，于是"no-sync 变体"在持锁路径里照样做了同步截断（探针实测 `lockedTruncates=1`）。
     修法：该处改限定名调用 `MemoryLogStore::truncateSuffix()`；`LogStore` 基类默认实现保留转调，
     但在注释里写明"凡 `truncateSuffix()` 会 fsync 的实现都必须覆写 `truncateSuffixNoSync()`"。
     同时 `erasePeerStateLocked` 补齐 `appendSentMs_/snapshotSentMs_/ackedTerm_` 回收（有界影响，见代码注释）。
     证据：`raftkv_raft_tests` 83/83 全绿（原 81 + A9 + A10）。
  2. **✅ ack 归因已收敛到闭包（v1.6 遗留项 3 的第二条）**：`onAppendEntriesReplyWithContext` /
     `onInstallSnapshotReplyWithContext` + 4 个发送点全部改为闭包携带 `sentEnd`/`chunkEnd`。
  3. **🔴→✅ I15：reactor 引擎的选举风暴（本轮新发现，此前从未归因）**。
     现象：`RAFTKV_TRANSPORT=reactor` 下 `raft_fault.sh --repeat 50` 约 2/3 的轮次失败，
     报 `write with one follower stopped returned 'NOT_LEADER'`；`status` 显示 term 每秒 +1.5
     （`elections_total` 单轮 38 次）。**根因（用环境变量 `RAFTKV_TRACE=1` 的临时插桩定位，已回退）**：
     `peerSendAllowedLocked()` 的静音窗口是 `2*rpcTimeoutMs = 200ms`，而 follower 的选举超时是
     **150–300ms**。一旦某帧被 reactor 逐帧超时丢弃（**丢弃时不回调**），该 peer 的
     `appendSentMs_` 会一直留到窗口结束 —— 健康的 follower 在 200ms 内收不到任何心跳，
     自行竞选并顶掉 leader，随后客户端写就返回 `NOT_LEADER`。trace 实证：
     node3 对 peer2 连续 `appendSkip (in-flight/mute)` 200ms，node2 恰在窗口末尾 `startElection term=63`。
     **修复**：新增 `RaftNode::inflightMuteGapMs()`，窗口 = `min(2*rpcTimeoutMs, electionTimeoutMinMs/3)`
     （默认 50ms，加 tick 10ms 粒度仍远小于 150ms），并用 **M5.A10** 钉住该不变量。
     验证：reactor `raft_fault.sh --repeat 50` **连续 3 轮 PASS**（修复前 3 轮里 2 轮 FAIL）。
  4. **🔴→✅ I16：reactor + 频繁 compact 下节点无日志静默死亡（本轮新发现）**。
     现象：`raft_snapshot_fault.sh`（reactor）报 `node3 did not apply 100k entries`；
     `raft_membership_fault.sh`（reactor）跑到第 47/48 轮无诊断退出。机制观测脚本显示
     **node3 进程直接消失**（`kill -0` 失败、日志只有启动两行、无 OOM 记录），
     而 1+2 仍构成多数派，所以 `fill` 甚至返回 rc=0。
     **根因**：`maybeSnapshot()`（main/ticker 线程）与 `onInstallSnapshot()`（reactor 线程）
     都在 `mu_` **之外**做 save/load/compact，二者**彼此没有任何互斥**；两处 `log_.compact()`
     会并发改同一个 `FileLogStore`（rename/truncate/setBoundary），一旦互相踩到就 `compact` 返回
     false，而这两条路径都是 `throw std::runtime_error(...)` —— 异常从 **reactor 回调**逃逸
     → `std::terminate` → 节点静默死亡。
     **修复**：① 新增叶子锁 `snapshotOpMu_`（L17），把"序列化/落盘 → compact → 推进边界"整段串行化
     （`maybeSnapshot` 与 `onInstallSnapshot` 各持一次，锁序 `snapshotOpMu_ → mu_`，不违反 I9）；
     ② `TransportReactor` 的回调包一层 catch：打印 `FATAL: reply callback threw: ...` 后**重新抛出**——
     不改变"致命错误必须停"的语义，但把"静默死亡"变成可诊断的死亡。
     验证：机制复现（reactor + `--snapshot-threshold 200` + `fill 20000 --pipeline 64`）**5/5 轮无节点死亡**
     （修复前连续两轮各死一个节点）；修复后 reactor 引擎全量脚本一次通过：
     `raft_snapshot_fault --repeat 20` / `raft_membership_fault --repeat 50` / `raft_snapshot_e2e` /
     `raft_membership_e2e` / `raft_fault --repeat 50` **全部 rc=0**。
  5. 构建告警清零：`src/server.cpp` 的 `switch (req.op)` 未处理 `OpCode::kConfig`（M4 引入的
     `-Wswitch` 告警）。补显式 `case`（该分支不可达：kConfig 不在 M1 codec 白名单内，P3），
     不用 `default:` 以免掩盖将来新增的 OpCode。**全量干净重建 0 warning**。
  6. TSan（当前源码，含上述全部改动）：`83/83` 通过、**`WARNING: ThreadSanitizer` 0 条**、构建 0 warning。
  7. **仍未关闭（进入 M5.5）**：① 双引擎 A/B（3 次中位数）与 `docs/m5-bench.md` §3 数据；
     ② §8.2 滑动窗口的**触发判定**——若 M5.4 收尾后 pipeline=64 仍 < 1200 qps，则必须按 §8.2 落地；
     ③ 默认引擎的最终选择（当前仍为 `sync`，reactor 已可全绿，待 A/B 数据决定）。

- **v1.8**（M5.5 实测回填：机器状态、引擎选型、一条**负结果**，以及 §8.2 触发判定）：
  1. **机器状态声明（先于一切数字）**：冻结基线期 p=1 = 16.4 ms/写；本次 M5.5 复测期间同一台机器
     p=1 = 28.7–41.5 ms/写（M4 二进制亦然），`load average` 1.4–4.0、宿主盘 jbd2 持续活动、
     p50 被顶到直方图最高桶。**绝对判据（p=1 ≤8 ms/写、p=64 ≥1200 qps）在本机当前状态下不可达**，
     且不是 M5 引入的退化（同轮 M4 也只有 27–529 qps）。按 §10，只采信同轮交替的相对结论。
     完整数据与原始日志见 `docs/m5-bench.md` §3（含 §3.2 顺序偏差检验）。
  2. **🔴 关键发现：M4 基线的"更快"来自丢写**。同轮交替 A/B 中，M4 在 p=8/p=64 的多数格子里
     `verify` 报 `missing 1102 / 171 / 12097 / 57 / 93 / 7`，并出现过 `fill: failed`；
     交换顺序复测（§3.2）后 M4 依然 4/4 丢写，而 **M5 在全部 10 个格子都 `missing 0`**。
     故 M4 的 ms/写 与 M5 不同口径：M5 用 12%(p=1)~50–90%(p=8/64) 的 wall-clock 代价买了"零丢写"。
  3. **引擎选型（同轮交替，2 次，全部 missing 0）**：reactor 在 **p=1 快 1.33×**（21.598 vs 28.807
     ms/写）、**p=8 快 2.06×**（6.037 vs 12.408），p=64 慢 1.20×（3.984 vs 3.310）。
     p=64 的落后正是"每 peer 只允许一批在途"造成的节流。→ 决策③ 的 reactor 作为**默认引擎**
     （M5.6 随滑动窗口一起切换并复跑全量脚本）。
  4. **组提交蓄批（决策④ 批调优）实测为负结果，默认关闭**：`--group-linger-us` 让 flusher 先短睡
     再 fsync。同轮交替 A/B（3 次）：p=8 12.505 → 12.621、p=64 3.372 → 3.383（无差异）；
     p=1 不触发（无并发等待者）。诊断显示蓄批确实把 fsync 次数降了 25%（699→586→524），
     但 **fsync 只占每批周期 ~14%**，瓶颈是"每批串行等两个 follower 的 fsync+RTT"（~51 ms/批，
     批量仅 19–20 条）。结论：蓄批不是本机的杠杆；实现与开关保留（便于在安静机器复现该负结果）。
     这也直接**排除了"fsync 摊薄"假设**，把矛头指向复制往返的串行化。
  5. **§8.2 触发判定：成立**（pipeline=64 < 1200 qps，且 reactor 在 p=64 明确落后）。
     故 **M5.6 落地滑动窗口**（每 peer N 批在途 + `nextIndex_` 乐观推进 + 乱序/重复 ack 幂等用例）。
     在此之前不切默认引擎、不打 tag。

- **v1.9**（M5.6：按 §8.2 触发条件落地**滑动窗口**，并新增 I17/L18）：
  1. **实现**（`--inflight-per-peer N`，默认 4；`cfg.maxInflightPerPeer`）：
     * `appendInflight_[peer]` 从"单个时间戳"升级为**槽位队列** `{sentMs, end, beginIdx}`；
       `peerSendAllowedLocked()` 改为"未过期槽位数 < N"。
     * **乐观推进**：`approveAppendSend()` 登记槽位后立即把 `nextIndex_[peer]` 推到本批之后——
       否则窗口里的多批都是同一批的重复发送，窗口毫无意义（实测：只开窗不推进时 p=64 无改善）。
     * **回退路径（I17，安全性要点）**：① `pruneInflightLocked()` 在槽位 TTL 过期时把
       `nextIndex_` 回退到最早未确认批的**起点**（丢帧必须能重发）；② 收到失败应答时清空该
       peer 的全部在途槽位（它们都建立在错误前缀上），再从冲突点重发。
     * ack 只释放**自己那一批**的槽位（闭包上下文精确对应），乱序/重复 ack 各自幂等。
     * 槽位 TTL 复用 I15 的 `inflightMuteGapMs()` → "窗口满"的静音仍 < 最小选举超时，不会
       重新引入 v1.7 的选举风暴。
  2. **测试 M5.A11**（§8.2 要求）：自建异步假 transport（`sendX` 只入队、应答由测试投递），
     8 写者 × 6 写并发；**逆序**投递全部应答且每条第 2 次重复投递。断言：48/48 写仍成功、
     `commitIndex` 单调不减、`maxInflightSeen ∈ [2, cfg.maxInflightPerPeer]`。
     **RED 证据**：把 `c1.maxInflightPerPeer` 改回 1 后该用例失败
     （`Expected: maxInflightSeen >= 2u, actual: 1`），证明它确实在测这条窗口。
  3. **实测收益**（reactor，同轮交替，每格 2 次，全部 `verify missing 0`）：

     | pipeline | 窗口=1 ms/写 | 窗口=4 ms/写 | 结论 |
     |---|---|---|---|
     | 1 | 23.337 / 21.730 | **13.682 / 13.206** | 窗口 4 快 **1.68×**（p=1 是验收关注点） |
     | 8 | 5.966 / 5.331 | 5.920 / 6.049 | 持平 |
     | 64 | 4.134 / 4.057 | 4.103 / 4.264 | 持平 |
     p=1 的收益来自"不必等上一批（含心跳/no-op）的 ack 才发下一批"。
     p=64 持平说明**本机瓶颈仍是 follower fsync 的串行往返**，不是窗口宽度——真正的上限
     受限于"每连接一次只有一个请求-应答在飞"（reactor 队首配对），要再进一步需要多连接/多路复用。
  4. **脚本健壮性修复（与 Raft 无关，但会伪装成里程碑失败）**：`raft_membership_fault.sh`
     第 181 行 `out="$(cli ... get f5)"` 是**裸命令替换**，而 `cli get` 在 key 不存在时返回非零，
     `set -e` 下会**静默退出**（无 FAIL 行、无 dump）——50 轮故障注入里偶发，曾被误读成
     "reactor 引擎不稳定"。用 `RAFTKV_TRACE`/ERR trap 定位（`ERR at line 181`，而当时 5 节点
     `term=11 cv=1334` 全部收敛、leader 正常）。改为"有限次重试 + 明确诊断"（`read_key_retry`），
     `raft_membership_e2e.sh` 同类写法一并修。

- **v2.0**（M5 收尾实测回填：**一条未关闭的阻断项 + 两项设计偏差 + 最终验收口径**）：
  1. **🔴 未关闭的阻断项：reactor 引擎在 p=64 压测下偶发丢写（并在本轮复现过一次 abort）**。
     * 现象：3 节点 + reactor + `fill 10000 --pipeline 64` → `verify` 报 `missing 1 / 12 / 34 / 36 / 49`
       （约 1/3 的轮次非 0）；**多次重复 verify 结果完全一致**（1/1/1、12/12/12、34/34/34）
       → 不是"读路径陈旧"，是**真的没进状态机**。
     * 归属实验（每格 3 轮 × 10000）：
       | 配置 | 结果 |
       |---|---|
       | sync（默认） | 0 / 0 / 0 —— 干净 |
       | reactor + 窗口=1 | 1 / 0 / 0 |
       | reactor + 窗口=4 | 0 / 0 / 0（但另一组 6 轮里出现 1 / 12 / 34） |
       | reactor + **关闭快照压缩**（threshold 1e8） | 0 / **49** / 0 —— 仍复现 → 属 append/复制路径，与快照无关 |
       另有 1 次 reactor 节点 **Aborted (core dumped)**（未捕获到栈）。
     * 已排除：投票"日志新旧"判据（§5.4.1 实现正确）、跨轮次 (clientId,requestId) 复用
       （每个 trial 都是全新集群 + 全新 CLI 进程，仍复现）、快照/InstallSnapshot 路径
       （关掉压缩仍复现）、`MemoryLogStore::truncateSuffixNoSync` 虚调用（v1.7 已修）。
     * 未定位的疑点（下一轮继续）：`advanceCommitAndApply()` 的 apply 循环在
       `log_.slice(next,1,·)` 取不到条目时**静默 break**，而 `lastApplied_` 仍可能被
       InstallSnapshot 路径置前；以及乐观 `nextIndex_` 与"失败应答清空槽位"的交互。
       复现命令（约 1 分钟/轮）：
       `RAFTKV_TRANSPORT=reactor ./build/bin/raftkv_raft_node ... --snapshot-threshold 5000`
       + `./build/bin/raftkv_raft_cli --peers ... fill 10000 --pipeline 64` + `verify 10000`。
     * **处置**：正确性优先 → **默认引擎保持 `sync`**（v2.0 起 `useReactor=false`），
       reactor 作为可选引擎（`--transport=reactor`）保留全部功能与故障注入证据，
       但**在修好之前不得作为默认、M5 也不据此宣称"性能里程碑达成"**。
  2. **设计偏差（§8.3 / §8.4，未实现或部分实现，按"偏差必须入档"记录）**：
     * **§8.3 快照流式序列化未实现**：`FileSnapshotStore::save()` 仍是一次性
       `encodeSnapshotFile(data)` 全量缓冲后写盘，`SnapshotView` 没有 `nextChunk()`/增量 CRC，
       因此内存峰值仍是 O(状态)，§8.3 要求的"RSS 增量 ≤ 块大小 × 常数"断言**也不存在**
       （仓库内无任何 RSS/getrusage 测试）。当前 `--snapshot-threshold` 与本机规模下快照
       仅数百 KB～数 MB，尚未造成问题，但它是一项**未完成的设计项**。
     * **§8.4 断点续传仅进程内有效**：`.recv` 文件**没有** `(index, term, receivedLen, crcSoFar)`
       头；续传状态 `recvIndex_/recvTerm_/recvEnd_` 只在内存里。行为是：同一进程内重传块幂等
       （`offset + size <= recvEnd_` 直接认成功）、出现空洞则整段重启、`done` 时校验
       (index,term) 后 rename。**跨进程重启的续传不成立**（重启后从头传）。
  3. **最终验收口径（本轮实测，默认引擎 = sync）**：构建 0 warning；`raftkv_raft_tests` 84/84；
     `raftkv_tests`(M1) 13/13；TSan 0 报告；`raft_snapshot_e2e` / `raft_membership_e2e` /
     `raft_snapshot_fault --repeat 20` / `raft_fault --repeat 50` / `raft_membership_fault --repeat 50`
     全 PASS（详见 `docs/m5-bench.md` §3 与提交信息）。
     **未达标且已入档**：p=1 ≤8 ms/写、p=64 ≥1200 qps 在本机当前状态下不可达（冻结基线期 16.4 ms/写
     → 本轮 M4 基线亦 28.7–41.5 ms/写），需在安静机器上复测。
  4. **结论**：M5.1/M5.2/M5.3/M5.5/M5.6 的目标与不变量已落地并有用例/脚本守门；
     M5.4 的 §8.3 未实现、§8.4 部分实现；reactor 的可选引擎存在未关闭的丢写阻断项。
     因此**本轮不打 `m5-performance` tag**：tag 应等到 reactor 丢写定位并修复之后再打。

- **v2.1**（TSan 收尾：一条**测试侧真实缺陷**、一条**库层误报**的判定与处置）：
  1. **测试侧真实缺陷（已修）**：M5.A11 的异步假 transport 原来没有加锁，而 `sendX` 会被
     **多个 proposer 线程**同时调用（`awaitCommit` 的 flusher 路径），测试线程又在并发地
     `swap`/`push_back` 同一个 vector → 数据竞争。TSan 在上面报出
     `allocation-size-too-big`（读到半更新的 `LogEntry`，字符串长度是垃圾值）
     与 `double lock`。修法：桩内部加 `std::mutex`，测试线程改用 `takeAppendJobs()/takeVoteJobs()`
     取队列。**这正是"TSan 必须跑"的价值：它抓的是我自己的测试代码，不是 Raft。**
  2. **库层误报（判定 + 窄抑制）**：改完之后仍剩 3 条，全部是
     `condition_variable_any::notify_all()` 相关（`double lock` 与
     `lock-order-inversion`：环 `RaftNode::mu_ <-> cv_ 的内部 _M_mutex`）。
     **判定为误报**，依据是 libstdc++ 源码自身的保证
     （`/usr/include/c++/11/condition_variable:307-313` 注释原文：
     "*__mutex must be unlocked before re-locking __lock so move ownership of *__mutex
     lock to an object with shorter lifetime"）——局部量按声明逆序析构，`_M_mutex` 一定在
     重新获取用户锁**之前**释放，所以"持 `_M_mutex` 再拿 `mu_`"这条边实际不存在。
     **处置**：新增 `tests/tsan.supp`，只抑制"栈里出现 `condition_variable_any::notify_all`"
     的 `mutex`/`deadlock` 两类（其余 race/mutex/deadlock 照报），并在文件头写明出处与佐证
     （两种引擎 100k 写 + 64 并发 + 50 轮故障注入从未挂起）。运行命令随之固定为
     `TSAN_OPTIONS=suppressions=tests/tsan.supp setarch $(uname -m) -R ./build-tsan/bin/raftkv_raft_tests`。
  3. **A11 的两处收紧**：① 期限 20s → 90s（TSan/ASan 下慢 10~100x，否则把"没跑完"误判成"丢写"）；
     ② 在途批数上界由 `<= N` 改为 `<= 3N` 并注明理由：槽位 TTL（I15 的 `inflightMuteGapMs`）
     到期会先回收，而那一帧可能仍在飞，因此"未确认帧数"= N 个未过期槽位 + 若干已过期未回包的帧；
     生产侧的硬约束由 reactor 帧超时给出。

- **v2.2**（reactor 丢写的**根因深挖**：证据链已到"同一 index 出现两个都被提交的条目"，
  仍未定位到具体代码路径；据此收紧 reactor 默认配置）：
  1. **观测手段**：`RAFTKV_TRACE=1` 的临时插桩（`[prop]` 收到写请求 / `[kv] APPLY`+`DROP` 状态机应用 /
     `[term]` 角色与任期 / `[vote]` 投票输入输出 / `[trunc]` 冲突截断 / `[snap]` 快照安装 /
     `[send]`+`[ack]` 复制与 matchIndex 推进 / `[commit]` 提交推进时的各投票者 matchIndex），
     跑 `reactor + --snapshot-threshold 5000 + fill 10000 --pipeline 64` 直到复现；定位后**已全部回退**
     （`git status` 干净，仓库不含诊断代码）。
  2. **已经确证的事实**（不是推测）：
     * 丢的是**已 ack 的写**：`fill` 成功、`verify` 报 `missing 1/12/34/36/43/49`，
       且**重复 verify 三次结果完全一致**（1/1/1、12/12/12、34/34/34）→ 不是读路径陈旧。
     * **每个节点缺的是不同的 ~30-40 个 key，而三节点并集恰好 = 10000** → 所有写都被"某处"提交过，
       集群日志发生了**分歧**（不是单点丢数据）。
     * 具体到同一个 index：**index 4534 上有两个不同条目都进入了"已提交且已应用"状态**——
       node1 的版本 `key=f4437 rid=4438`（term 8，`[commit] node1 ADVANCE commit=4558
       match={1:3438 2:4558 3:4530}` → 多数派 {1,2} 提交），
       node3 的版本 `key=f4442 rid=4443`（term 9，`[commit] node3 ADVANCE commit=4586
       match={1:4206 2:4586 3:4531}` → 多数派 {2,3} 提交）；
       node1 应用了前者、node2/node3 应用了后者。
     * 两轮 leader 都在**不同任期**（node1: term 5/8/12；node3: term 1/9/13），
       **没有任何"同任期双 leader"**；`[vote]` 行显示 `upToDate` 判据的输入输出都符合 §5.4.1
       （34 次 `upToDate=0` 全部拒绝，未见"该拒不拒"）。
     * 本轮**没有任何 `[snap] INSTALL`**（无快照安装），排除快照路径。
     * **node2 从未发生冲突截断**（`[trunc]` 计数 0），却只应用了 node3 的版本；
       而 node1 的 `[ack] node1 matchIndex[2]: 4530 -> 4558 (term=8)` 说明它把 node2 记成了
       "已拥有 4558"。→ **匹配到 node1 视角的 matchIndex 被高估**（一次不真实的 success ack）。
  3. **已排除的嫌疑**（逐条查过代码/实证）：投票"日志新旧"判据；同任期双 leader；
     快照安装导致的 `lastApplied_` 前移；`MemoryLogStore`/`FileLogStore::appendNoSync` 的
     连续性检查（两者都有 `e.index != lastIndex()+1 → false` 与冲突截断）；
     leader 侧闭包式 ack 归因（两处发送点都带 `sentEnd` 闭包，`onAppendEntriesReply` 旧路径**已无调用者**）；
     reactor 的"答复与队首请求配对"（同一连接上严格串行、按队列顺序触发回调）。
  4. **仍未定位的疑点**（下一轮继续，需要更强的观测手段）：
     * reactor 在"超时丢帧 + 关连接 + 重连 + 队列重排"组合下是否可能把某帧的答复记到另一帧上
       （需要给帧编号并在两端核对，而不是只看 matchIndex 结果）。
     * `matchIndex_` 被高估的那一次 ack 究竟对应哪一帧（需要给 `[send]`/`[ack]` 加帧序号与时间戳，
       并把 reactor 内部的 `queue` 变化也打出来）。
  5. **据此的处置（本轮）**：
     * reactor 引擎**默认窗口从 4 降回 1**（`--inflight-per-peer` 默认 1，仅显式传参才开窗）——
       窗口=4 会显著改善 p=1 延迟（22.5 → 13.4 ms/写），但也**放大了暴露面**（分歧更容易出现）。
       **实测残留率**（同机、每轮 10000 写 + verify）：`sync` 3/3 干净；`reactor --inflight-per-peer 1`
       **1/6 轮出现丢写（missing 29）**；`reactor` 默认窗口=4 时 ~2/3 轮出现。
       → 结论：**根因在 reactor 的异步 ack 归因，窗口只是放大器**；把窗口降回 1 只是收缩暴露面，
       不是修复。reactor 因此在修复前**不适合作为默认引擎**（出厂默认 sync）。
       保留窗口的实现与 A11 用例（A11 自己显式设 `maxInflightPerPeer=4`）。
     * 出厂默认仍是 `sync`（v2.0 起），reactor 为可选且标注"存在未关闭的丢写阻断项"。
     * **仍不打 `m5-performance` tag**。
     * 复现命令（约 1 分钟/轮，需保留数据目录以便对账）：
       `RAFTKV_TRANSPORT=reactor ./build/bin/raftkv_raft_node --id N --snapshot-threshold 5000 ...`
       + `./build/bin/raftkv_raft_cli --peers ... fill 10000 --pipeline 64` + `verify 10000`。

- **v2.3**（reactor 丢写**根因确证**：异步应答被记到错误的在途请求上；sync 侧对照为 0 失误）：
  1. **插桩手段（已全部回退，仓库不含诊断代码）**：
     * 给每个 AppendEntries 帧算**内容签名**（term + prevLogIndex/prevLogTerm + entries 数量 +
       首/末条目的 index/term），leader 发送时打 `[tx] sig=`、收到应答打 `[rx] sig=... ok=...`，
       follower 处理完打 `[ae] sig=... -> ok=...`。签名不含 leaderCommit，但足以判定"这一批条目"。
     * reactor 内部加**发送序号**：`[q]`（入队）、`[w]`（每次真正写出，含 wrote/want）、`[pop]`（收到
       应答后弹出队首）。
  2. **核心实验（可复现）**：对每个"被记功的应答"（leader 侧 `[rx] ok=1`）检查被记功的那个 peer
     是否真的处理过同签名帧：
     | 引擎 | 记功次数 | peer 从未处理该签名帧 | peer 只回过失败 |
     |---|---|---|---|
     | **sync** | 855 | **0** | **0** |
     | **reactor**（6 轮） | ~1230–1290 /轮 | **14 / 27 / 17 / 22 / 20 / 21** | **14 / 20 / 20 / 9 / 19 / 35** |
     → **sync 的"一次调用一个应答"归因是精确的；reactor 有 1–3% 的应答被记到了错误的在途请求上。**
       这与"sync 3/3 干净、reactor 1/6~2/3 丢写"的现象完全吻合。
  3. **独立的佐证**（同一轮故障日志）：node1 的 `[commit] … commit=4558 match={1:3438 2:4558 3:4530}`
     说明它按 `matchIndex_[2]=4558` 提交；而 node2 自己在 term 9 的投票行写着
     `myLast=(8,4530)` —— **node2 的日志从未到过 4558**。即那次"记功"是虚假的：
     leader 据此提交并 ack 了客户端，随后这些条目被更高任期的合法分歧覆盖 → 已 ack 的写丢失。
  4. **结论（阻断项性质）**：reactor 的请求/应答配对不满足"每个应答都对应它自己那一帧"这一前提；
     在"超时丢帧 + 关连接 + 重连 + 队列重排"的组合下会错配。**这是 reactor 侧的传输层缺陷，
     不是 Raft 层逻辑缺陷**（投票/提交/日志匹配规则经逐条核对均正确）。
  5. **处置**：
     * 出厂默认仍为 `sync`（v2.0 起），`--inflight-per-peer` 默认 1；reactor 明确标注**不可用于生产**。
     * 修复方向（下一轮）：让配对**可验证**或在歧义时**不可能错配**——最保守可行的方案是
       "每连接最多一个 Pending；任何超时/错误/部分写导致的不确定，一律关连接并**丢弃该 peer 全部
       Pending**（不重排、不重用）"，由 Raft 层按既有重试逻辑重发。改动集中在 `reactor.cpp`，
       不触碰 wire 格式（I14）。
  6. 本轮同时修掉一个**真实崩溃**（插桩期间暴露）：`main_raft_node.cpp` 里 `ticker` 线程是 joinable 的，
     而 socket/bind/listen 失败会直接 `return 1` → 析构 joinable thread → `std::terminate`
     （实测日志 `bind :40002 failed: Address already in use` + `terminate called without an active
     exception`）。已加 RAII 守卫（所有退出路径先停 ticker 再 join），并实测 bind 冲突时干净退出
     （rc=1，仅一行错误信息，无 terminate）。

- **v2.4**（reactor 丢写**已修复**：`flushLocked()` 在等待应答期间重发队首帧）：
  1. **根因（精确到行）**：`Reactor::Impl::flushLocked()` 的装填条件是
     `if (p.out.empty() && !p.queue.empty()) p.out = p.queue.front().frame;` ——
     队首帧**完整写出后 `p.out` 变空、但它仍在 queue 里等应答**，于是下一次 `flushLocked()`
     又把同一帧装进 `out` 再发一遍。而 `run()` 的事件循环尾部每次迭代都会对
     `!p.out.empty() || !p.queue.empty()` 的 peer 调用 `flushLocked()` —— **每一次循环迭代重发一次**。
     另一条出口：`onReadableLocked()` 收到应答后自己内联写下一帧（不走 `flushLocked`），
     同样不置"等待应答"标志，于是新队首也会被下一轮扫描重发。
  2. **实测证据**：给 reactor 加发送序号后按 (peer, seq) 统计"累计写出字节"：
     | 节点 | 入队且有写出的 seq | 恰好一次 | **超出（重复发送）** | 不足 |
     |---|---|---|---|---|
     | node1 | 4096 | 4021 | **75（全是 2.0 倍）** | 0 |
     | node2 | 6 | 3 | **3（2.0 倍）** | 0 |
     | node3 | 307 | 173 | **134（2.0 倍）** | 0 |
     对端因此对同一请求回两次；第二次答复会弹掉**下一批**的 Pending → ack 归因错位
     （v2.3 的对照实验：reactor 1–3% 记功错位、sync 0/855）→ `matchIndex_` 虚高 →
     leader 提交并 ack 了 follower 从未持有的条目 → 已 ack 的写被更高任期的合法分歧覆盖而丢失。
  3. **修复**（`src/raft/reactor.cpp`，不触碰 wire 格式，符合 I14）：给 `Peer` 增加
     `awaitingReply`（队首已完整写出、只等应答）；`flushLocked()` 仅在
     `!awaitingReply && out.empty()` 时装填队首，整帧写完后置 `awaitingReply=true`；
     收到应答弹出队首时、以及 `closePeerLocked()` 时复位；`onReadableLocked()` 的
     "应答后立刻发下一帧"改为**统一走 `flushLocked()`**，消除第二条重发出口。
  4. **验证**（同机、每轮 10000 写 + `verify`）：
     * 修复前：`win=1` 1/6 轮丢写（missing 29）；`win=4` ~2/3 轮丢写（missing 1…49）。
     * 修复后（各 10000 写/轮）：`win=4` **6/6 全部 missing 0**（修复前 ~2/3 轮丢写、最多 missing 49）；
       `win=1` **9/10 missing 0，1 轮 missing 1**（修复前 1/6 轮、missing 29）。
       → 占主导的丢写路径已消除；**仍残留一个极罕见项（约 1/10 万写）尚未归因**，
       已如实记录，未声称 reactor 已可用于生产。
     * reactor 全量脚本（修复后）**一次通过**：`snapshot_e2e` / `membership_e2e` /
       `raft_fault --repeat 50` / `raft_membership_fault --repeat 50` 全部 rc=0 PASS。
  5. **新增回归用例 R6**（`tests/raft_reactor_test.cpp`）：用一个"只收不回"的 FrameServer
     发一帧，等待 400ms（远大于一次事件循环），断言对端**只收到 1 帧**、回调不触发、
     `inflight()==1`。修复前该用例必失败（每轮扫描重发一次）。为此给测试用 `FrameServer`
     加了 `frames()` 计数（即使不回复也把帧读出来计数）。
     * **sync 对照组**（同一脚本、同一机器、8 轮 × 10000 写）：**8/8 全部 missing 0** ——
       即清洗后的残留（约 1/10 万写）仍是 **reactor 独有**，尚未归因；已如实入档。
  6. **出厂配置不变**：默认仍 `sync`（它在同口径下 3/3 + 全部 A/B 格子零丢写），
     `--inflight-per-peer` 默认 1；reactor 从"存在未关闭阻断项"改为
     "根因已定位并修复、有专项回归用例"，但**在安静机器上完成整套复测前仍不作为默认**。

- **v2.5**（残留丢写**根因与修复**：`awaitCommit` 把"index 已提交"当成"本次写已提交"）：
  1. **根因（Raft 层，非 reactor 专属）**：`awaitCommit()` 的两处成功判断原来只看
     `index <= commitIndex_`。真实序列（用 `[prop]/[app]/[commit]/[ack]` 插桩实测）：
     * leader（term T）把客户端条目**追加**到 index I，但还没拿到多数派（未提交）；
     * 它失去领导权；新 leader（term T+1）用**不同的条目**覆盖 I（合法：I 从未提交）；
     * 随后 I 被提交（提交的是新条目），本节点的 `commitIndex_` 也跟着推进到 I；
     * 于是 `awaitCommit(I, T)` 命中 `index <= commitIndex_` 直接回 `kOk` ——
       客户端被告知"写成功"，而**那次写从未被任何节点应用**。
     证据：残留复现轮里 `fill` 输出 `filled 10000`（rc=0、无 server error），
     `verify` 却 `missing 1`；`[app] node2 key=f816 idx=892 term=7` 说明它被追加在 892，
     而 `[commit] node1 ADVANCE commit=892 term=8`（多数派 {1,3}）提交的是**新 leader 的
     另一个条目**，全集群没有任何节点 APPLY 过 f816。
     reactor 因为选举更频繁而更易触发（实测 win=1 约 1/10 轮），**sync 同样有这个缺陷**，
     只是触发概率低（此前 8/8 干净）。
  2. **修复**（`raft_node.cpp`，不触碰 wire 格式）：两处成功判断都要求
     **`log_.termAt(index) == term`**（该 index 上仍是本次追加的那一条）；条目已被覆盖时
     回 `kNotLeader`，让客户端按既有重定向/重试逻辑重发（幂等，(clientId, requestId) 去重）。
     `cv_.wait_until` 的谓词同步收紧，条目被覆盖时不再空等。
  3. **回归用例 M5.A12**（`tests/raft_perf_test.cpp`）：用"只挡第一次 `sync()`"的
     `OnceBlockingLogStore` 把 propose 停在**锁外 fsync 窗口**里，趁这个窗口投递
     "更高任期 + 不同条目覆盖 I + leaderCommit=I"的 AE，然后放行 fsync，断言 propose
     返回 `kNotLeader` 而不是 `kOk`。
     **RED 证据**：临时去掉 term 校验后该用例失败（`status=0(kOk)` vs 期望 `3(kNotLeader)`），
     恢复后通过。
  4. **修复后实测（全部 10000 写/轮、`verify` 门禁）**：
     * reactor `win=1` ×10、`win=4` ×4、sync ×5 → **19/19 全部 `missing 0`**
       （修复前 reactor win=1 约 1/10 轮 missing 1）。
     * 9 次脚本运行**全 PASS**：默认(sync) 侧 `snapshot_e2e` / `membership_e2e` /
       `raft_fault(50)` / `raft_membership_fault(50)` / `snapshot_fault(20)`；
       reactor 侧 `snapshot_e2e` / `membership_e2e` / `raft_fault(50)` / `raft_membership_fault(50)`。
     * 单测 **86/86**；TSan **rc=0、0 warning、86/86**；M1 13/13。
  5. **至此 M5 的已知正确性阻断项全部关闭**（I9 锁内 fsync、ack 归因、reactor 重发、
     `awaitCommit` 误报成功）。仍未达成的是**绝对性能判据**（见 §3.0 机器状态声明：
     本机当前 p=1 为 28.7–41.5 ms/写、p=64 约 250–320 qps，而冻结基线期 M4 自身也只有
     16.4 ms/写、589 qps）——这一项取决于机器，需在安静机器上复测。

- **v2.6**（M5 收尾总结与 tag 说明）：
  * **已达成（均有本轮实测证据）**：
    | 验收项 | 状态 | 证据 |
    |---|---|---|
    | M1 回归 | ✅ 13/13 | `./build/bin/raftkv_tests` |
    | Raft 全量单测 | ✅ **86/86** | 含 M5.A1–A12、R1–R6 |
    | 4 个 e2e/fault 脚本 | ✅ PASS ×9 次运行 | sync 侧 5 个 + reactor 侧 4 个，全 rc=0 |
    | TSan | ✅ rc=0、**0 warning**、86/86 | `TSAN_OPTIONS=suppressions=tests/tsan.supp` + `setarch -R` |
    | 构建 0 warning | ✅ 干净重建 0 warning / 0 error | `build-final` 全量重建 |
    | `verify missing 0` | ✅ **19/19 轮**（reactor win=1×10、win=4×4、sync×5） | 各 10000 写 + verify |
    | 同机 A/B + 3 次中位数 | ✅ 两轮完成 | `docs/m5-bench.md` §3.1 |
  * **未达成（机器状态所致，已带原始数据入档）**：`pipeline=1 ≤8 ms/写`（本机 32.1 ms）
    与 `pipeline=64 ≥1200 qps`（本机 224 qps）。冻结基线期同一台机器的 **M4 自身**
    也只有 16.4 ms/写、589 qps，即该目标需要安静/更快的机器才能复测；同机交替口径下
    M5 比 M4 慢 1.4–1.8×（换来 I9 + 零丢写 + reactor + 可观测性）。
  * **设计偏差（未实现，已入档）**：§8.3 快照流式序列化（仍是全量缓冲、无 RSS 断言）、
    §8.4 断点续传仅进程内有效（`.recv` 无 header）。
  * **tag**：`m5-performance` 打在收尾提交上，**注释中明确写出**上述未达标项与偏差，
    不声称"性能目标达成"；它标记的是"M5 工程量完成、全部正确性门禁转绿、性能判据待安静机器复测"。
  * **默认引擎**：`sync`（reactor 已可用、故障注入与端到端全绿、丢写根因已修并有专项回归用例，
    但性能上并不优于 sync，且需在安静机器上再复测一轮）。

- **v2.7**（补 §8.3 流式快照与 §8.4 跨进程断点续传；并把绝对判据修订为**同机比值口径**）：
  1. **§8.3 快照流式序列化（已实现）**：
     * `state_machine.h` 新增 `SnapshotStream`，`SnapshotView::stream()` 有默认实现
       （把 `serialize()` 当单块吐出，旧实现零改动即可编译）。
     * `KvSnapshotStream`（`kv_state_machine.cpp`）按 `[lastApplied][kvCount][条目…][dedupCount][去重…]`
       逐条产出，**超大单条记录用 carry-over 拆到多次 next()**，因此 `next()` 的返回块严格
       ≤ `maxChunk`；有 B 组用例断言"分块流 == `serialize()` 逐字节相同"。
     * `FileSnapshotStore::saveStreaming()` **两趟走流**：第一趟只累计 `payloadLen` 与增量 CRC32
       （新增 `crc32Update`），第二趟逐块写盘 + `fsync` + `rename` + `fsyncDir`。落盘文件与
       `encodeSnapshotFile()` **逐字节一致**（用例实测），I14 不破。
     * 新增 `installedBytes()` / `readInstalled(offset,len)`：leader 发送 InstallSnapshot 分块时
       **按需读取文件切片**（O(块)），`RaftNode` 因此**删除了常驻的 `snapshotBytes_`**
       （启动加载、maybeSnapshot、onInstallSnapshot 三处都不再 materialize 整块编码）。
     * 仍为 O(状态) 的两处（已在此记录，不再算"未实现偏差"）：`snapshotView()` 在 `mu_` 下做的
       状态**拷贝**（L8 要求序列化在锁外，无法省），以及接收端安装时 `decodeSnapshotFile` 需要
       整块 payload 交给 `sm_.restore()`。即快照路径峰值从 ~3× 状态降到 ~1× 状态 + 1 块。
  2. **§8.4 跨进程断点续传（已实现）**：
     * `.recv` 的续传元数据放在**尾部**：`[payload: receivedLen] | "RKR1" | idx(8) | term(8) |
       receivedLen(8) | crcSoFar(4)`。每收一块就更新尾部（长度 + 增量 CRC），
       **载荷因此保持从偏移 0 连续**——安装时只需 `ftruncate` 掉尾部再 `rename`，
       不需要为"去掉头部"再复制一份整块 payload。
     * `FileSnapshotStore` 构造时即尝试从尾部恢复 `(idx, term, receivedLen, crc)`，
       `receiveChunk()` 因此能在**新进程**里从断点继续（`recvProgress()` 可读进度）；
       重传块仍是幂等成功；偏移对不上则拒绝并要求 leader 从 0 重来。
     * 用例 `RaftSnapshotResume.ContinuesAfterStoreRestart`：半个文件 → 析构（=进程重启）→
       新实例 `recvProgress()==半个文件` → 从断点续完 → `load()` 载荷完整；另有两条对照
       （重启后换更新的快照从头传仍可安装；错误偏移必须被拒）。
  3. **验收口径修订（用户决定）**：绝对判据 `p=1 ≤8 ms/写`、`p=64 ≥1200 qps` 改为**同机比值口径**：
     `p=1: M5 延迟 ≤ 1.2×M4`；`p=8/64: M5 吞吐 ≥ 0.8×M4`；每次 `verify missing 0` 为硬门禁。
     `scripts/bench_m5_ab.sh` 的判定逻辑已改（并输出 M5/M4 两个比值列），`docs/m5-bench.md` §3.7
     记录理由与对照（§3.6 的微基准：本机单次持久化提交 ≈8 ms，绝对目标隐含 flush≈1 ms 的硬件）。
  4. **本轮验证**：单测 **90/90**（原 86 + 4 条新增：流式保真 2、切片读取 1、跨重启续传 1）；
     `raft_snapshot_e2e` / `raft_snapshot_fault --repeat 20` / `raft_e2e` 全部 PASS（这三条脚本
     正好覆盖快照生成、分块传输、安装与重启路径）。

- **v2.8**（收尾实测：并行扇出的**负结果**与当前判定）：
  1. 按 §3.8 的诊断（"每批 70 ms 里 ~62 ms 是顺序等两个 follower"）实现了并行扇出
     （`SendExecutor` + 每 peer 一条常驻线程、同 peer 保序），同步引擎下注入 main，
     单测 90/90 不受影响。**实测更差**：p=8 14.83→22.28 ms/写、p=64 3.97→7.25 ms/写
     （252→138 qps）。节点退出正常，非死锁。**已整块回退**，仓库停在已验证状态。
     结论：差距**不是**"发送串行"造成的；扇出打断了"一批做完再下一批"的自然节流，
     放大了重复 AE 与 `mu_` 争用。详见 `docs/m5-bench.md` §3.9。
  2. **当前判定（同机比值口径）**：p=1 **达标**（M5/M4 延迟 1.11x ≤ 1.2x）；
     p=8/64 **未达标**（吞吐 0.60x / 0.57x < 0.8x）。同一轮 A/B 里 M4 基线出现
     `missing 5` / `missing 9`，而 M5 全部格子 `missing 0`。
  3. **下一步（先测再改）**：给"一批写"的各段打时间戳（append → 锁外 fsync 结束 →
     作业构建 → 收到 ack → 推进 commit → 唤醒客户端），把 70 ms 拆开定位，再决定是
     批大小、`mu_` 争用还是客户端唤醒路径；在此之前不再凭推测改复制路径。
  4. **里程碑口径**：M5.1/M5.2/M5.3/M5.5 与 M5.4 的 §8.1/§8.2/§8.3/§8.4 **全部落地并有用例守门**
     （单测 90/90、TSan 0 报告、9 次 e2e/fault 脚本 PASS、`missing 0` 19/19 轮）；
     唯一未达成的是**同机比值口径下的 p=8/64 吞吐**（0.57–0.60x vs ≥0.8x），
     已带完整诊断与负结果入档，`m5-performance` tag 的注释同样写明。

- **v2.9**（收尾：p=64 分段定量结论 + 复盘文档）：
  1. **分段插桩实测**（只打时间戳、复制路径未改，测完已回退；详见 `docs/m5-bench.md` §3.10）：
     单批内部 = 锁外 fsync **8 ms** + durable→commit（等 follower ack）**13 ms** +
     发送 **14 ms** + 间隙 3 ms ≈ **38 ms/批**（批≈30 条）；整轮 20000 写里 `fsync_ms` 只占墙钟 **~10%**。
     蓄批旋钮在静载机器上重测仍**无增益**（0/5ms/20ms → 241/244/235 qps；fsync 次数降 17% 但只占 10%）。
  2. **结论**：p=8/64 的差距**不在复制路径**（并行扇出实测更差、已回退），而在**批与批之间的
     唤醒 + 单把全局锁争用**（34+ proposer 抢 `mu_`、`cv_` 每批 notify_all）。M4 更快是因为它把
     fsync 关在锁内，等价于一个**全局串行点**；这是 I9（锁内不做 IO）与高并发吞吐之间的
     **结构性张力**，需要改唤醒机制（按批条件变量/事件计数，把"批成形"从抢锁里拆出来），
     属下一阶段架构工作——不是调参能解决。
  3. 新增 **`docs/m5-review.md`**：M5 复盘（面试口径）——六个真 bug 的现象/定位手段/根因/修复与回归、
     两次负结果、四条可迁移方法论、性能口径为何改为比值、下一步方向。
  4. **判定不变**：p=1 比值达标（1.11×）；p=8/64 未达标（0.60×/0.57×）；正确性门禁全绿
     （单测 90/90、TSan 0 报告、`missing 0` 19/19 轮、9 次脚本 PASS、0 warning）。

- **v2.10**（P2a：p=8/64 比值达标；**修正 v2.9 的归因**）：
  1. **归因修正**：v2.9（= `m5-bench.md` §3.10）把 p=8/64 差距归为"批间唤醒 + 单把全局锁争用"，
     **实测不成立**。分段插桩（`awaitCommit` 四段 + `TcpTransport::roundTrip` 把"等锁"与"往返"分开）
     显示瓶颈是**复制发送段排队**：`syncInFlight_` 在 fsync 返回即放开 → 上一批还在阻塞发送、
     下一批已开始 fsync，多个 flusher 在唯一那把 `mu_` 上排队（peer=2 的发送"拿到锁之前"中位
     **11.5 ms**，而一次真正往返 **<1 ms**；每 flush 平均 4.4 次往返）。
  2. **修复（P2a）**：flusher 等本批两次 `sendAppendEntries` 都发出后再放开 flush 窗口
     （fsync 失败仍立刻放开，I5），并在仍有未 durable 条目时 `notify_one` 交接。回归 **M5.A16**
     （RED：窗口内 7 次新 flush / 总 sync 9 → GREEN：0 次 / ≤4）。
  3. **复测**（同机静载、3 节点回环、每格 `missing 0`）：p=1 延迟 **1.03×**；p=8 n=20000 **1.34×**
     （修前 0.47×）；p=64 n=2000 **2.20×**（修前 ~1.06×）；副产物 leader `batch_avg` p=8 1→4~6、
     p=64 4→8（max 63）。
  4. **引擎重测**（P2a 后）：p=1 reactor 快 **1.17×**、p=8 **打平**、p=64 sync 快 **1.38×** ⇒ 默认仍
     `sync`，但理由从"正确性（reactor 曾丢写，已修）"改为"**高并发吞吐**"（§3.3 的 p=8 结论作废）。
  5. **门禁**：单测 **94/94**（新增 A13/A14/A15/A16，均先 RED 后 GREEN）、TSan **94/94 且 0 报告**
     （`tsan.supp` 按同一 libstdc++ 论证补 `condition_variable_any::wait_until` 族）、
     `raft_e2e` 与三个 fault 脚本各 10 轮 PASS。§3.9 的"并行扇出更差"、§3.10 的"改唤醒机制"
     结论据此作废（原文保留为当时的记录）。
