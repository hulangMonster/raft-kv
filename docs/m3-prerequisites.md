# M3 前置校验（Phase #1）

> 阶段：#1 / verification-before-completion（不写业务实现代码）
> 依据：`docs/m3-design.md` **v1.1**（已批准）、`docs/roadmap.md`、M1/M2 现有代码与测试
> 结论：设计可行，**发现并已修订 1 处设计缺陷（D3）**；无其它阻断项

---

## 0. 基线状态与验证证据（本轮已在 ubuntu-vm 实测）

M3 起点基线**已在本轮重新执行、全部通过**：

| 检查 | 命令 | 结果 |
|---|---|---|
| 构建 | `cmake --build build -j4` | exit 0 |
| M1 单测 | `./build/bin/raftkv_tests` | `all tests passed (13)` |
| M2 Raft 单测 | `./build/bin/raftkv_raft_tests` | `[  PASSED  ] 14 tests` |
| M1 e2e | `./scripts/e2e.sh` | `e2e: PASS` |
| 集群 e2e | `./scripts/raft_e2e.sh` | `raft_e2e: PASS` |
| 故障注入 | `./scripts/raft_fault.sh --repeat 50` | `raft_fault: PASS (50 iterations)` |

- **本地 ↔ 虚拟机同步通道已打通**：公钥免密登录；`docs/m3-design.md` 与 `docs/m3-prerequisites.md` 的 md5 本地与 VM 完全一致。
- 复验期间**未触碰任何 M1/M2 源码**（本轮仅改动 `docs/`，不影响基线）。

---

## 1. M2 → M3 边界不变量

### 1.1 组件处置总表

| M2 组件 | 处置 | 说明 |
|---|---|---|
| `RaftNode` 选主/投票/任期/心跳（`tick` 选举分支、`onRequestVote*`、`startElection`、`become*`） | **复用，零语义改动** | M3 只在 `tick()` 顶部插入 `maybeSnapshot()`、在复制路径插入"`nextIndex ≤ lastIncluded` → 发快照"分支 |
| `RaftNode` 复制/提交（`onAppendEntries`、`onAppendEntriesReply`、`buildAppendEntries`、`advanceCommitAndApply`、§5.4.2 规则） | **复用，零语义改动** | 仅在 `onAppendEntries` 增加"落在边界以下 → `conflictIndex = firstIndex()`" |
| `Transport` / `TcpTransport` / `MemoryTransport` / `Clock` / `SteadyClock` / `FakeClock` | **复用，零改动** | InstallSnapshot 复用同一 `Transport` 接口与帧 |
| `LogStore`（8 个既有虚函数） | **扩展** | 新增 `setBoundary/compact/firstIndex/lastIncluded*`；**既有 8 个方法的对外语义不变** |
| `MemoryLogStore` / `FileLogStore` | **扩展（含 D3 修订）** | 内部改为"边界 + 基址"模型；`load` 连续性校验相对 `firstIndex_` |
| `StateMachine`（`apply/get/lastApplied`） | **扩展** | 新增 `snapshotView()/restore()`（带默认实现 → M2 实现类零改动可编译） |
| `KvStateMachine` | **扩展** | 实现视图/恢复；`apply` 的 `kGet` no-op 分支保留 |
| `message.{h,cpp}`（msgType 1–4、10–12） | **扩展** | 仅追加 5/6；既有编解码零改动 |
| `types.h::RaftConfig` | **扩展** | +`snapshotThresholdEntries` / `snapshotChunkBytes` |
| `main_raft_node.cpp` / `main_raft_client.cpp` | **最小修改** | 注入 `FileSnapshotStore`、status 扩展、手动触发 |
| `tests/raft_test_harness.h` | **仅追加** | 新增快照版 cluster 构造辅助；既有 `makeCluster` 与 M2 用例不动 |
| M1 全部（`common.h`/`codec`/`wal`/`store`/`server`/`thread_pool`/`main_server`/`main_client`/`tests/test_*`/`scripts/e2e.sh`） | **禁止改动** | M3 完全不触碰 |

### 1.2 决策 D2 的落地（复用 / 扩展 / 重构）

- **复用**：M2 的 D1（日志为持久化真相源）→ M3 扩展为"**最新快照 + 快照点之后的 raft.log**"共同构成真相源。
- **扩展**：`LogStore` 获得前缀物理压缩（`compact`）与边界语义；新增 `SnapshotStore` seam。
- **重构**：**无**（不做结构性重构）。
- **D2 铁律**：快照只覆盖 `≤ lastApplied`；**先 durable 快照，后 compact**；顺序不可颠倒。

### 1.3 不变量（M3 代码必须维持）

沿用 `m2-prerequisites.md` 的 **I1–I8**（日志 1-based、term 单调、单一 votedFor、commitIndex/lastApplied 单调、投票先持久化、只 Leader 接受写、apply 幂等、RaftNode 不直接依赖 socket/fs），新增：

- **I9**：`lastIncludedIndex ≤ lastApplied ≤ commitIndex`
- **I10**：`firstIndex = lastIncludedIndex + 1`
- **I11**：`snapshot.dat` durable 之后才允许 `compact`；任意时刻快照边界 ≤ 已删日志前缀末端
- **I12**：`restore` 只接受 `lastIncludedIndex ≥ 当前 lastIncludedIndex` 的快照，**禁止状态回退**
- **I13**（D3）：LogStore 的位置访问一律以 `firstIndex_` 为基址；完全压缩后 `lastIndex()==lastIncludedIndex`、`lastTerm()==lastIncludedTerm`

---

## 2. 线程安全契约与锁纪律

### 2.1 沿用 M2 的 L1–L7

- **L1**：`RaftNode` 单一 `mu_` 保护全部共识状态。
- **L2**：持锁期间禁止 fsync / socket send/recv / `Transport::send*`。
- **L3**：日志持久化两段式（锁内改状态 → 解锁落盘 → 回锁确认）。
- **L4**：出站队列：锁内只构造任务，解锁后发送；回调再进锁。
- **L5**：`propose` 在 `cv_` 上等待（提交/降级/超时），等待过程不持锁。
- **L6**：ticker 每 10ms 调 `tick()`；单测手动 `tick()`。
- **L7**：`tick()`/RPC 入口全部在锁内；出站发送在锁外。

### 2.2 新增 L8：快照专项

- **L8**：锁内**只允许**"取快照边界 + 取一致性视图（`sm_.snapshotView()`，内存拷贝）"；**禁止**锁内 `view->serialize()`、禁止锁内 `SnapshotStore::save/receiveChunk`（磁盘 IO）、禁止锁内 `LogStore::compact`（重写文件）。
- **允许的例外**：`sm_.restore(payload)` 为**纯内存反序列化**（无 IO），允许在锁内执行（InstallSnapshot 安装路径）。
- **出站队列适配**：InstallSnapshot 分块发送同样遵守 L4（锁内只选 peer + 组装该 chunk 的 args，锁外 `transport_.sendInstallSnapshot`）。
- **ticker 串行**：`maybeSnapshot()` 只在 ticker 线程调用，天然串行（无并发快照）。

---

## 3. 风险清单

| # | 风险 | 后果 | 缓解 | 覆盖测试 |
|---|---|---|---|---|
| R1 | 快照覆盖未 apply 前缀 | 提交语义破坏 | D2 + `snapIndex = lastApplied_` | A2 |
| R2 | compact 后 `termAt/slice/lastIndex` 边界语义破坏 | 复制/选主误判 | §4.3 不变量 + 以 `firstIndex_` 为基址 | A3/A4 |
| R3 | **compact 失败/部分成功后重启，`load` 误判损坏截断整份日志**（D3） | 丢数据 | `load` 按 `firstIndex_` 校验、跳过 `index < firstIndex_` 的旧记录 | B13/B14 |
| R4 | 旧快照覆盖新状态 | 状态回退 | `lastIncludedIndex ≤ 自己边界 → 忽略` | A7 |
| R5 | 先删日志后落快照 | 崩溃丢数据 | §5.4 顺序 + 回锁校验 | B11/B12 |
| R6 | torn-snapshot | 启动失败 | 魔数/版本/CRC 校验 → 丢弃回落全量日志 | B12 |
| R7 | 去重表未随快照序列化 | 重启后二次 apply | 视图/恢复包含 `lastRequest_` | A10 |
| R8 | 快照传输期间 propose 阻塞 | 可用性下降 | 每 tick 每 peer 1 chunk | A5、e2e |
| R9 | InstallSnapshot 单包超 64 MiB | 传输失败 | 1 MiB 分块 + 帧上限校验 | A5 |
| R10 | 分块 offset 不连续 | 快照损坏 | `offset != 当前大小 → 重置重传` | A5（可注入） |
| R11 | 锁内序列化大 KV / 锁内 IO | ticker/propose 卡死 | L8 | 评审 + A 组时序检查 |
| R12 | `restore` 期间并发读 sm_ | 数据竞争 | `restore` 在 `mu_` 锁内执行 | 评审 |
| R13 | 阈值默认值不合理（10000 条） | 测试过慢/过快 | 测试显式设小阈值（如 8）；默认值仅生产用 | 全部 A 组 |

---

## 4. M1/M2 存量代码修改点清单

### 【必须改】

1. `src/raft/state_machine.h` — 追加 `SnapshotView` 与 `snapshotView()/restore()`（带默认实现）。
2. `src/raft/log_store.h` — 追加 `setBoundary/compact/firstIndex/lastIncludedIndex/lastIncludedTerm`。
3. `src/raft/memory_adapters.cpp` — `MemoryLogStore` 实现上述接口 + D3 基址模型。
4. `src/raft/file_log_store.cpp` — `FileLogStore` 实现上述接口 + D3 基址模型 + `load` 连续性相对 `firstIndex_`。
5. `src/raft/message.{h,cpp}` — `MsgType` 追加 5/6 + `InstallSnapshotArgs/Reply` 编解码。
6. `src/raft/types.h` — `RaftConfig` 追加 `snapshotThresholdEntries`、`snapshotChunkBytes`。
7. `src/kv/kv_state_machine.{h,cpp}` — 实现 `snapshotView()/restore()`（`data_ + lastRequest_ + lastApplied_`）。
8. `src/raft/raft_node.{h,cpp}` — 可选注入 `SnapshotStore*`（默认 `nullptr`）、`maybeSnapshot()`、`triggerSnapshot()`、InstallSnapshot 收发、边界状态、`lastIncludedIndex()`。
9. `src/main_raft_node.cpp` — 注入 `FileSnapshotStore`、`status` 扩展快照字段、手动触发入口。
10. `src/main_raft_client.cpp` — `status` 显示快照信息。
11. `CMakeLists.txt` — `raftkv_raft` 追加 `memory_snapshot_store.cpp`/`file_snapshot_store.cpp`；`raftkv_raft_tests` 追加 `tests/raft_snapshot_test.cpp`。
12. `tests/raft_test_harness.h` — **仅追加**快照版 cluster 构造辅助。

### 【可选扩展】（本次不做）

- 日志字节阈值触发（M5）
- InstallSnapshot 断点续传（M5，`nextOffset` 已预留）
- 保留多份快照（M5）

### 【禁止改动】

- M1 全部：`src/common.h`、`src/codec.{h,cpp}`、`src/thread_pool.h`、`src/wal.{h,cpp}`、`src/store.{h,cpp}`、`src/server.{h,cpp}`、`src/main_server.cpp`、`src/main_client.cpp`、`tests/test_*.cpp`、`scripts/e2e.sh`
- M2 算法路径：`RaftNode` 选主/复制/§5.4.2 提交规则；既有 `tests/raft_*_test.cpp` 用例；`scripts/raft_e2e.sh`、`scripts/raft_fault.sh`

---

## 5. 边界 case 全集、未定义行为、测试前置假设

### 5.1 快照生成与阈值

- 空 KV（`payloadLen == 0`，CRC 为空串）
- `lastApplied - lastIncluded < threshold`（不触发）
- 恰好达到阈值（`>=` 触发）
- 一次跨越多个阈值（只生成一个快照）
- 手动触发：leader / follower / 非 leader
- 生成中 apply 前进（`snapIndex < 新 lastApplied`，快照允许"落后"，下次再快照）
- 生成中降级或任期变化（回锁校验失败 → **放弃 compact**，边界不变）
- `SnapshotStore::save` 失败（**不 compact**、边界不变、下次重试）
- `compact` 失败或部分成功（快照已 durable；日志可能仍含 `index < firstIndex_` 的旧记录）

### 5.2 压缩与边界语义（D3）

- `compact` 到 `lastIndex`（日志全空）→ `lastIndex()==lastIncludedIndex`、`lastTerm()==lastIncludedTerm`、`slice` 返回空、`termAt(lastIncluded)` 正确
- 重复 `compact(upTo ≤ lastIncluded)` → no-op
- `compact` 后 `append`：要求 `e.index == lastIndex()+1`
- `compact` 后 `termAt(i < lastIncludedIndex) == kNoTerm`（调用方不得查询）
- `compact` 后 `slice(from < firstIndex)` → clamp 到 `firstIndex_`
- **`load` 遇到 `index < firstIndex_` 的旧记录 → 跳过（等价补做 compact），不得截断整份日志**；随后首条保留记录 index 必须 `== firstIndex_`，否则视为 torn-tail
- 完全压缩后 `load`（日志文件为空）→ 合法，`lastIndex==lastIncluded`

### 5.3 InstallSnapshot

- 单块（快照 < chunk 大小）与多块（`> chunk`）
- `offset` 不连续 → 重置接收为 0 重新传
- `done` 前连接断开（`.recv` 残留，下次重传覆盖）
- 收到 `lastIncludedIndex ≤ 自己边界` 的旧快照 → 忽略（回 `success=false` + 自己边界）
- 快照边界 < follower 已有后缀 → **保留 `> lastIncludedIndex` 的后缀**
- 快照边界 ≥ follower `lastIndex` → 日志清空
- `term` 更旧的 InstallSnapshot → 拒绝
- 安装完成后重置选举超时
- 与在途 AppendEntries 交错：AppendEntries `prevLogIndex < firstIndex` → 回 `conflictIndex = firstIndex()`，leader 转快照路径
- `restore` 失败（payload 损坏）→ 返回 false，**不更新边界**
- Leader 发快照中途下台 → 停止发送

### 5.4 恢复与持久化

- torn-snapshot（魔数/版本/CRC 失败）→ 丢弃 → 全量日志重放
- 快照 + 日志组合恢复
- 仅快照（日志已被完全压缩）
- 仅日志（无快照）
- 两者皆无（全新节点）
- `compact` 后日志 torn-tail → 截断
- 顺序：**`setBoundary` → `load`**（D3）

### 5.5 未定义行为 / 禁止

- 锁内 `serialize()` / `save()` / `receiveChunk()` / `compact()`（违反 L8）
- 查询 `termAt(i < lastIncludedIndex)`（调用方保证不查）
- 快照覆盖未 apply 条目（违反 I9）
- 先 `compact` 后落快照（违反 I11）
- `restore` 到更小的 `lastIncludedIndex`（违反 I12）
- `append` 非连续 index（LogStore 必须拒绝）

### 5.6 测试前置假设

- **A 组**：`FakeClock` + `MemoryTransport` + `MemoryLogStore` + `MemorySnapshotStore`，无真实网络/磁盘 → 零 flaky；`tick()` 手动驱动；阈值/块大小测试内显式设置（如 threshold=8、chunk=64B），不依赖默认值。
- **B 组**：`FileLogStore` + `FileSnapshotStore` + 真实临时目录；测试结束清理。
- **既有用例不动**：M1 `tests/test_*.cpp`、M2 `tests/raft_*_test.cpp` 全部保持；`raft_test_harness.h` 只追加、不修改既有 `makeCluster`。
- **`RaftNode` 构造默认 `SnapshotStore* = nullptr`** → M2 全部调用点零改动可编译，快照关闭。

---

## 6. 前置待办状态

1. ✅ **VM 通道**：已配置公钥免密，`本地 ↔ 虚拟机` 同步可用（tar/scp 同步 + md5 校验一致）。
2. ✅ **基线复验**：§0 的 6 条命令已在本轮 VM 实测全部通过。
3. ⏭️ **下一步 #2（TDD 测试先行）**：新增 `tests/raft_snapshot_test.cpp`（A 组 10 例 + B 组 4 例），编译并**跑出 RED**。
