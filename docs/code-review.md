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

---

# M3 + group commit 代码评审报告（#4，第二轮）

> 评审方式：**独立评审子代理**（只读，未改代码），产出 15 个阻断项 + 17 个优化建议。
> 评审范围：`src/raft/*`（含新增 M3 文件）、`src/kv/kv_state_machine.*`、`src/main_raft_*.cpp`、
> `tests/raft_snapshot_test.cpp`、`scripts/bench_group_commit.sh`。
> 评审结论：**M3 快照主体可上简历**（`RKS1` 格式、CRC、tmp+rename+fsync(dir)、D3 基址模型、两段式生成、
> 五步恢复）；但**新增的 group commit 有多处阻断性正确性/数据丢失问题**，`28/28` 绿灯并未覆盖这些路径
> （M3 测试直接调 `follower->onInstallSnapshot`，绕过了 leader 的 offset/chunk 状态机；也没有单节点用例）。

## 结论：15 个阻断项已全部修复 ✅

修复决策与实现细节见 [m3-design.md](m3-design.md) **v1.3** §12。摘要：

| 编号 | 问题 | 修复 |
|---|---|---|
| B1 | 单节点 cluster 组提交后无人推进 commit | flusher 回锁段内 `advanceCommitAndApply()` |
| B2 | `sync()` 返回值被忽略，fsync 失败仍计自持久 | 失败不推进 `syncedIndex_`，`propose` 返回 `kErr` |
| B3 | `flushTarget` 与并发 truncate 竞争 | 回锁后夹紧 `min(flushTarget, lastIndex())` |
| B4 | 锁外 `sync()` 与锁内 `compact()` 争 fd（D5） | **L9**：fd 生命周期收进 `FileLogStore::mu_` |
| B5 | 锁外 `save` 覆盖更新的 InstallSnapshot | store 拒绝回退边界 + `installEpoch_` |
| B6 | 靠安装追平的节点当选后无法再服务快照 | 安装后刷新 `snapshotBytes_` |
| B7 | 丢回复 → 重复追加 / 永久 offset 不匹配 | 接收端幂等 + 失败即重置发送偏移 |
| B8 | 后缀未校验 term；commit 公式用 `lastIndex` | 后缀 term 校验；`min(leaderCommit, lastMatched)`、只增不减 |
| B9 | 安装快照无条件回退 applied/commit（违反 I4） | `commitIndex = max(...)` + 同锁段内重放追平 |
| B10 | `restore` 失败被当 torn tail → 清空日志 | 视为不可恢复，拒绝启动 |
| B11 | 构造器忽略 `log_.load()` 返回值 | load 失败抛异常 |
| B12 | `compact` 不 fsync 目录 / reopen 失败半提交 / 忽略返回值 | `fsyncDir` + 失败 fatal |
| B13 | 缺压缩区守卫；组提交路径绕过快照路由 | 边界守卫 + fast-backup 夹紧 + 组提交跳过落后 peer |
| B14 | 并发 InstallSnapshot 并发写无锁 store | store 内部互斥 + (index,term) 传输标识 |
| B15 | `fill --pipeline` 共享 clientId → 静默丢写 | 每 worker 独立 clientId + `verify <n>` 写回校验 |

## 评审意见的采纳与反驳

- **采纳**：上表 15 项；优化项 O1（已存在条目也要 fsync 才能 ack）、O5（count 上界校验）、O6（compact no-op）、
  O7（setBoundary 单次范围 erase）、O9（persistMeta 补 fsyncDir）、O10（truncateSuffix 缺 offset 不再退化成
  `ftruncate(0)`）。
- **部分反驳**：B9 建议 `lastApplied_ = max(lastApplied_, installed.index)`。直接在回锁段里对 `lastApplied_`
  取 max 会与刚 `restore()` 出来的状态机**不一致**（状态机已被倒回边界）。实际实现改为
  `lastApplied_ = installed.index` 后**在同一锁段内** `advanceCommitAndApply()` 重放已提交后缀：
  对外（`mu_` 保护）观察不到回退，状态机与索引也保持一致。
- **保留意见**：O2（cv 谓词缺 `syncedIndex_`）。现有谓词是"已提交/已降级"，所有 `notify` 都伴随状态变化
  （写入 `syncedIndex_`、推进 `commitIndex_`、降级），未发现丢唤醒路径；本轮不改为条件谓词。
  > **后续（M5 P2a 阶段，已被证伪）**：O2 指出的正是真问题。`awaitCommit` 的"开头能否接手当 flusher"
  判定与 `cv_.wait_until` 之间有一段放锁窗口，flush 完成的通知会丢，最后一个待写者睡满 propose 超时
  —— 用例 **M5.A15** 修前约 8/10 轮命中（`commit=64/lastIndex=65`）。修法：谓词补上
  `(!syncInFlight_ && syncedIndex_ < index)`（见 `raft_node.cpp` awaitCommit 的谓词注释）。
- **推迟到 M5**：O3、O4、O8、O11、O13、O15、O17（见 m3-design.md §12 末段）。

## 修复过程中的额外发现（评审未提及）

1. **测试自身越界崩溃**：`tests/raft_snapshot_test.cpp::InstallSnapshotCatchesUpLaggingFollower` 把某个
   follower 隔离后**仍然 tick 它**，该节点会不断竞选 → 任期抬升 → leader 被逼下台，随后用例用
   `leader->leaderId() - 1` 索引节点（此时 `leader_id = -1`）→ 堆越界（ASan 报 heap-buffer-overflow）。
   修复：新增 `tickOnly()` 只驱动可达节点，并在索引前断言仍是 leader。
2. **B13 守卫的边界写错会引入死循环**：`prevLogIndex == lastIncludedIndex` 时边界条目的 term 是**可验证**的
   （`termAt` 返回答），守卫必须用 `< lastIncludedIndex()` 而不是 `< firstIndex()`；否则 leader 会与
   follower 在"重传快照 ↔ 回退 prevLogIndex"之间无限循环，集群无法提交（本轮实测复现并修复）。
3. **fast-backup 的收缩方向**：peer 报出的冲突提示可能**高于**本方日志末尾（对方压缩边界更靠前），
   此时必须接受该提示（下一步自然由 tick 的快照/追加路由处理），逐格回退只会造成空转。
4. **benchmark 数字的可信度**：`--pipeline` 64 的吞吐必须与 `verify` 的"零丢写"一起看。修复后干净环境下重测为
   **pipeline=1 → 129 qps、8 → 746 qps、64 → 2840 qps（≈22×）**，三次 `verify` 均 `missing 0`；
   旧记录的 2623 qps 对应的是"共享 clientId 静默丢写"的实现，因此 README/roadmap 已改用新数字。

## 验证证据（修复后，全部在 ubuntu-vm 实测）

| 检查 | 结果 |
|---|---|
| `raftkv_raft_tests` | **35/35 PASS**（M2 14 + M3 14 + 评审回归 7） |
| `raftkv_tests`（M1） | **13/13 PASS** |
| ASan（`-fsanitize=address` 全套 raft 用例） | **35/35 PASS，无内存错误** |
| `scripts/e2e.sh` / `raft_e2e.sh` | PASS |
| `scripts/raft_fault.sh --repeat 50` | PASS |
| `scripts/raft_snapshot_e2e.sh` | PASS |
| `scripts/raft_snapshot_fault.sh --repeat 50` | PASS |
| `scripts/bench_group_commit.sh`（含每轮 `verify`） | PASS，`missing 0` |

## 新增回归用例（评审要求 4 项 + 本轮补充）

1. `RaftSnapshot.SingleNodeProposeCommitsWithoutPeers` —— 单节点 propose 必须提交（B1）。
2. `RaftSnapshot.InstallSnapshotRetransmitAfterLostReplies` —— 经 **leader 的 offset/chunk 状态机**传输，
   丢掉**中间块**的回复后仍能续传（B7）；修复前该用例确定性失败（follower 停在 `last_applied=1`）。
3. `RaftSnapshot.InstalledNodeCanServeSnapshotAsLeader` —— 靠安装追平的节点当选 leader 后，
   仍能向新的落后节点发快照（B6）。
4. `RaftSnapshotStore.StaleSaveNeverOverwritesNewerInstall` / `...ReceiveChunkIsIdempotentForRetransmits` /
   `RaftSnapshotDisk.ConcurrentSaveAndInstallKeepNewestSnapshot` —— 快照边界不回退、chunk 幂等、并发 save+install（B5/B7/B14）。
5. `RaftSnapshotDisk.FileStoreClusterKeepsCommittingAfterCompaction` —— 真实文件存储集群在压缩后**持续提交**
   （修复前该用例复现了压缩后提交停滞，是定位 B13 边界错误的关键用例）。


---

# M4 代码评审报告（#4，第三轮）

> 评审方式：**独立评审子代理**（只读，未改仓库；基线 HEAD `28e1e37`），产出 7 个阻断项 + 9 条优化建议，
> 并用 `/tmp/m4probe*.cpp` 临时探针**实际复现了 B1 / B2 / B3 / B4 / B5 / B7** 的关键路径。
> 评审范围：`docs/m4-design.md`、`docs/m4-prerequisites.md`、`src/raft/*`、`src/kv/*`、
> `src/main_raft_*.cpp`、`tests/raft_membership_test.cpp`、`scripts/raft_membership_*.sh`，
> 并用 `git diff` 核对了 M1/M2/M3 的存量改动。
> 评审结论：设计文档、测试工程化、RKS1 v2、两个故障注入脚本扎实；**但 `58/58` 全绿覆盖不到 J2 越过提交、
> J4 非成员投票、新 Leader 陈旧 ReadIndex、在途配置快照、重启丢在途配置等安全关键路径**，
> 因此**当时不可判定 M4 完成**。

## 结论：7 个阻断项 + 9 条优化项全部处理完毕 ✅

修复分四批落地，每批**先写"守它的用例"（A20–A28），再在"移除修复"的状态下确认用例确实 RED**——
这既证明了缺陷真实存在，也保证用例不会再退化成"绿灯噪声"。设计侧的对应修订见
[m4-design.md](m4-design.md) **v1.5** §12。

| 批次 | commit | 覆盖 |
|---|---|---|
| 批 1 | `c3f42ad` | **B2**（J2 不得被后一条条目绕过）、**B5**（ReadIndex 同任期屏障）、**B7**（count 炸弹 + 连接级 `try/catch`）；O1/O6/O7/O8/O9 的廉价部分 |
| 批 2 | `c4e42b5` | **B1**（J4 投票资格）、**B3**（在途配置不得进快照）、**B4**（重启重建在途/prev/base + 安装快照基线）、O3 |
| 批 3 | `a9270b0` | **B6**（`changeMembership` 串行化 + `version == index` 不变量；`propose()` 拆出 `appendEntryLocked()` / `awaitCommit()`） |
| 批 4 | `61fd1f2` | O2（被移除 peer 的每 peer 状态 + 地址簿回收，`removablePeersLocked()` 单一入口）、O4（`readAcks_` 回收） |
| 批 4b | 本轮 | O2 强化（设计 v1.4(a)）：**"提交"不等于"送达完成"**——送达目标改为 `{until, deadlineMs}`，被移除节点确认收到移除条目（或超预算）才移出复制目标并回收；新增 `purgeDrainsLocked()` |
| O5 | `3088814` | 客户端拓扑缓存 + 失效重取（决策⑦ 的验收目标），e2e 新增第 6/7 步 |

## 【阻断项】逐条处置

| 编号 | 缺陷（评审要点） | 处置 | 守它的用例（RED 证据） |
|---|---|---|---|
| B1 | **J4 未落实**：`onRequestVote` 只比 term/日志新旧，给非成员与退役节点投票 → 已被移除但仍持 C_old 的分区节点可拿 C_old 多数派当选，用更高任期截断已提交条目 | ✅ 接受：term 处理之后先判 `retiredLocked() \|\| !currConfig_.isVoting(candidateId)` → 拒票 | A23（RED：三处 `voteGranted=true`） |
| B2 | **J2 可被绕过**：提交循环只在 `n == inFlightConfigIndex_` 时校验 C_old 多数派，后一条普通条目先拿到 C_new 多数派即可越过 | ✅ 接受：凡 `n >= inFlightConfigIndex_` 都必须先满足 `hasMajorityLocked(prevConfig_, inFlightConfigIndex_)` | A20（RED：`commitIndex 2 > cfgIdx 1`） |
| B3 | **在途配置写进快照**：`snapIndex = lastApplied_` 却写 `currConfig_` → 重启重放同一条目抛 `config version regressed`（节点起不来），或条目被截断后拓扑泄漏 | ✅ 接受：按边界取配置（在途取 `prevConfig_`）+ `snapConfig.version <= snapIndex` 兜底 | A24（RED：`sc.version 2 > 边界 1` 且重启抛异常） |
| B4 | **重启不重建在途/prev/base**，安装快照的基线更新被 `sc.version` 门控 | ✅ 接受：重启时把快照边界之上最后一条配置条目重新标记为在途（并重建 C_old 与送达集合）；安装快照**无条件**重置基线后按「基线 + 剩余日志」重算 | A25（RED：重启后追加了第二条配置条目）、A26（RED：保留了已不在日志里的 v5 拓扑） |
| B5 | **线性一致读可返回陈旧值**：新 Leader 未提交本任期条目时 `commitIndex_` 可能落后于真实提交点 | ✅ 接受：读前要求 `log_.termAt(commitIndex_) == currentTerm_`（§8 屏障），超时 `kErr`，绝不返回可能陈旧的值 | A21（RED：返回 `kNotFound`） |
| B6 | **`changeMembership` check-then-act 竞态**：两个线程可同时通过 in-flight 检查；`next.version` 靠猜 index | ✅ 接受：`membershipMu_` 串行化整个变更；`index/term/version` 在同一 `mu_` 临界区内确定并立即追加 | A27（RED：两个变更同时进入 CatchUp、日志出现 2 条配置条目） |
| B7 | **`decodeAppendEntries` 未校验 count 就 reserve**（40 字节帧 → `bad_alloc`），连接线程无 `try/catch` → `terminate` | ✅ 接受（存量缺陷，M4 未修且同端口可达，一并修）：`count <= (n-40)/41`；连接处理拆出 `serveConnection()` 并整段 `try/catch` | A22（RED：`std::bad_alloc`） |

## 【优化项】逐条处置

| 编号 | 建议 | 处置 |
|---|---|---|
| O1 | ReadIndex 在途变更时按设计做双重多数派 | ✅ 采纳：`readQuorumLocked` 在 `inFlightConfigIndex_ != kNoIndex` 时同时要求 C_old 多数派 |
| O2 | 配置提交后清理被移除 peer（连接/每 peer map 泄漏） | ✅ 采纳并强化：批 4 起 `removablePeersLocked()` 单一入口 + `erasePeerStateLocked` + 锁外 `transport_.removePeer`；批 4b 把清理时机从"配置条目提交"推迟到"被移除节点确认收到该条目（或超 `catchUpTimeoutMs` 预算）"，见下方额外发现 4 |
| O3 | `recomputeConfigLocked` 未重算 `drainingPeers_`/`prevConfig_` | ✅ 采纳：批 2，`prevConfig_ = 最后一条在途配置的 C_old`，并按同一规则重算送达集合 |
| O4 | `readAcks_` 不清理 | ✅ 采纳：批 4，随 `erasePeerStateLocked` 一并回收 |
| O5 | **客户端拓扑缓存/失效重取缺失（验收目标未达成，非"取舍"）** | ✅ 采纳：`3088814` 实现缓存 + `kConfigRequest(get)` 失效重取；e2e 新增第 6/7 步直接守这条目标 |
| O6 | 超时配置不一致 / `readIndexTimeoutMs` 是死配置 | ✅ 部分采纳：`readIndexTimeoutMs` 接入为探针阶段预算（`min(调用方超时, 500ms)`）。**服务端 catch-up 预算（`max(timeoutMs, 30s)`）与客户端 15s 读超时的不一致保留**：`add/remove` 的结果是"提交后才算成功"，客户端超时后重试 `config` 即可看到真实结果，改变默认值会让慢网络下更容易误报失败。**推迟**：`add/remove` 的幂等状态查询 |
| O7 | 多数派未完全单一入口 / CatchUp 判据偏严 | ✅ 采纳：CatchUp 回到设计冻结判据（`matchIndex >= commitIndex` **且**本任期已应答，新增 `ackedTerm_`）；清理逻辑收敛为 `removablePeersLocked()`。多数派计算仍散落 `hasMajorityLocked` 派生的少数几处（已复核全部带配置语义），**推迟**纯重构 |
| O8 | 新 wire codec 健壮性（addrLen 截断、未知 action、重复 id） | ✅ 采纳：编码器保证 `addrLen` 与插入字节数一致；`decodeConfigRequest` 拒绝 `action ∉ {0,1,2}`；配置编解码拒绝重复 id；`changeMembership` 拒绝 >64KiB 地址。**推迟**："地址非空"校验——M2/M3 风格的退化配置（无地址，仅 peerIds）是合法内部状态，快照里会携带它，强制非空会让既有重启路径解不出来 |
| O9 | SM 的 `kConfig` no-op 分支 + 锁内 fsync | ✅ 部分采纳：`KvStateMachine::apply` 把 `kConfig` 当推进 `lastApplied_` 的 no-op，`RaftNode` 不再跳过配置条目（保证 SM applied 与快照边界一致）。**锁内 fsync 保留**：这是 M2 既有 L2 偏差（`onRequestVote`/`onAppendEntries`），非正确性问题；修它要动 M2 选主/复制路径的两段式持久化，与"最小改动存量代码"冲突，且 M3→M4 的 A/B 基准显示**无回归**（见下表），随 M5 的两段式持久化一起做 |

## 修复过程中的额外发现（评审未提及）

1. **J4 与 M2 用例前提冲突**：`RaftElection.VoteGrantedToUpToDateCandidate`（`makeCluster(1)`）与
   `TermAndVotePersistedBeforeReply`（无 `peerIds`）用**配置外的 `candidateId=2`** 充当候选人，
   J4 落实后必然拒票。这两条用例想验证的是"日志同样新的候选人会拿到票"与"授权前先落盘"，
   因此改为把候选人放进配置（`makeCluster(2)` / `cfg.peerIds = {2}`）——**只修正测试前提，被验证语义不变**。
2. **A19 依赖"旧配置节点仍会给被移除节点投票"**：J4 之后 l1（已切到 C_new）不再给 victim 投票，
   但 l1 之外的节点仍持 C_old，`votesGranted_ = 1 + self = 2 = majority(C_old)` 依然够，用例语义未变
   （实测通过）。
3. **"提交"不等于"送达完成"（A29 的由来）**：`raft_membership_fault.sh --repeat 50` 在**收尾**步骤偶发
   "remove 未收敛"。50 轮故障全部通过，说明不是故障窗口的问题，而是：被移除的节点**没有被保证收到**
   移除它的配置条目。J2 只要求 `majority(C_old) ∧ majority(C_new)` 确认该条目，被移除的那个节点本身
   可以不在任何多数派里；旧实现在条目**一提交**就清空送达集合、随即摘除它的连接——于是"错过了那一
   次发送"的节点永远停在旧配置里，会不断竞选（J4 让它拿不到票，但每次竞选都会抬高任期，把健康
   Leader 逼下台，正是设计 v1.4(a) 想消除的搅动）。修复：送达目标带 `{until, deadlineMs}`，确认收到
   （`matchIndex >= until`）或超预算才收尾；`purgeDrainsLocked()` 在 Leader 每 tick 兜底。守它的用例是
   **A29**（丢包面下必须继续送达、不得提前摘除；无修复时 `sendsTo(victim)` 提交后立刻冻结、地址簿被摘）。
4. **故障脚本自身的静默失败**："`x="$(field ...)"` 在节点瞬时不可达时会让整条管道失败，而脚本开了
   `set -e + pipefail` → **无任何诊断地退出**（`raft_snapshot_fault.sh` 就是在 SIGSTOP/SIGCONT 边界上
   命中它，表现为"跑到一半无声失败"）。M3 起就存在（脚本里为 `require_leader` 打过补丁，但 `field`
   没打）。已把 `field()` 统一改成返回空串，并给收敛检查加上超时后的逐节点视图 dump。
5. **`version == 配置条目 index` 是隐性跨模块不变量**：它同时被 `maybeSnapshot` 的边界保护
   （`snapConfig.version <= snapIndex`）与重启期 J3 校验依赖。B6 的"猜 index"会同时打穿这两处，
   因此批 3 把"确定 index/version + 追加"合并进同一个临界区，而不是只加一把互斥锁。

## 已确认无问题（抽查复核）

- 配置条目字节布局为**纯追加**：M2 `LogEntry` 落盘布局、`decodeLogEntry`/`FileLogStore::decodeEntry`
  仅扩白名单；M1 `codec.*`、`decodeClientRequest` 未动 → 客户端仍无法伪造配置条目。
- RKS1 v1 解码路径保留（B4 用例从 #2 起即通过）；快照五步恢复顺序未变，只在尾部追加 config 段。
- M1 源码零行为改动（仅 `common.h` 新增 `OpCode::kConfig = 4`）；`tests/test_*.cpp`、`scripts/e2e.sh` 在 diff 中为空。
- §5.4.2 提交规则、fast backup、D2/D3 语义、组提交 `syncedIndex_` gating 均未被 M4 改动（既有用例全绿）。
- Leader 自我移除（A16）、退役节点继续接收复制（A10）、J5（C_old ∩ C_new ≠ ∅，one-at-a-time 保证）复核无误。
- L10（`transport_.addPeer/removePeer` 只在锁外调用）、L11（锁序 `mu_ → transport → log`）复核无嵌套违规；
  新增的 `membershipMu_` 是叶子锁，锁序 `membershipMu_ → mu_`，不与既有路径交叉。

## 验证证据（修复后，全部在 ubuntu-vm 实测）

| 检查 | 结果 |
|---|---|
| `raftkv_raft_tests` | **68/68 PASS**（58 → +A20–A29 共 10 例） |
| `raftkv_tests`（M1） | **13/13 PASS** |
| `scripts/e2e.sh`（M1） | PASS |
| `scripts/raft_e2e.sh` | PASS |
| `scripts/raft_snapshot_e2e.sh` | PASS |
| `scripts/raft_membership_e2e.sh`（含新增第 6/7 步） | PASS |
| `scripts/raft_fault.sh --repeat 50` | PASS |
| `scripts/raft_snapshot_fault.sh --repeat 50` | PASS |
| `scripts/raft_membership_fault.sh --repeat 50` | PASS |
| M3(`b747c70`) vs M4 A/B 基准（同 `--snapshot-threshold`） | **13.2 ms/write vs 13.2 ms/write**——成员变更与读路径未引入回归 |

**RED 证据**（临时移除修复后重跑，逐条复现评审探针现象）：

| 用例 | 无修复时的失败信息（节选） |
|---|---|
| A20 | `Expected: (leader->commitIndex()) < (cfgIdx), actual: 2 vs 1` |
| A21 | `Expected: (rd.status) != (ClientStatus::kNotFound), actual: 1-byte object <01>` |
| A22 | `Actual: it throws std::bad_alloc` |
| A23 | `Value of: on->onRequestVote(stale).voteGranted  Actual: true`（退役/非成员同样 `true`） |
| A24 | `(sc.version) <= (snap.lastIncludedIndex) actual: 2 vs 1`；重启 `throws std::runtime_error "raft: config version regressed at index 2"` |
| A25 | 重启后第二条配置条目被追加（`log->lastIndex()` 增长、`config_version` 被推进） |
| A26 | 安装快照后仍保留 `v5`（`contains(3) == false`） |
| A27 | `blocked == 2`（两个变更同时进入 CatchUp）、日志出现 2 条配置条目 |
| A28 | `transport->wasRemoved(victim) == false`（地址簿从不回收） |
| A29 | `sendsTo(victim)` 提交后冻结（10 vs 10）、`wasRemoved(victim) == true`（送达未确认就摘除） |

## 建议执行顺序（后续）

1. ✅ 7 个阻断项 + O1–O5/O8 已全部落地，`m4-membership` 快照随之可打。
2. 推迟项：M5 两段式持久化（消除锁内 fsync，同时解决 O9 的锁内 IO）、异步 transport、
   `LogEntry` 编解码去重（M3 O4）、`add/remove` 幂等状态查询。
3. 评审提出的**未覆盖路径**已记录为后续工作：真实 TCP 下的恶意帧/并发 `add`（单测走 MemoryTransport）、
   M3 v1 数据目录与 M4 日志混合的完整恢复组合、50 轮以上长时间故障下的拓扑收敛断言增强。
