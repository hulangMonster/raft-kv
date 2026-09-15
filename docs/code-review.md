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

