# M3 设计文档：快照与日志压缩

> 状态：**设计定稿 v1.3（已批准基线 + #4 评审修订）**
> 关联：[roadmap.md](roadmap.md)（M3 验收口径）· [m2-design.md](m2-design.md) · [m2-prerequisites.md](m2-prerequisites.md) · [protocol.md](protocol.md) · M1/M2 源码
> 冻结日期：本轮 #0；**v1.1**：#1 发现 **D3**（compact 后日志索引基址未定义），修订 §4.3 边界不变量与 §5.3 恢复顺序；**v1.2**：#2 发现 **D4**（§10 漏列 Transport 的 `sendInstallSnapshot` 扩展），补齐 §10 文件清单；**v1.3**：#4 独立评审发现 15 个阻断项，按 §12 修订（group commit 持久性语义、SnapshotStore 边界不回退、InstallSnapshot 幂等续传与后缀 term 校验、commit/applied 单调性、LogStore fd 锁下沉、恢复路径致命化）。均已获用户批准；批准后不得擅自偏离本文档

---

## 1. 目标与非目标

### 1.1 目标（每条可验收）

| 编号 | 目标 | 验收方式 |
|---|---|---|
| G1 | 已 apply 前缀生成快照（`≤ lastApplied`），durable 后 compact 该前缀 | A 组单测 1/2 |
| G2 | 连续写入 **10w+ 条**后 `raft.log` **有界**（不再随写入线性增长） | M3.5 脚本断言文件大小上界 |
| G3 | 落后节点经 **InstallSnapshot** 追赶（不回放全量日志） | A 组 5；`raft_snapshot_e2e.sh` |
| G4 | 重启**先加载快照、再重放 raft.log 尾部**，状态完整 | B 组 11/14 |
| G5 | 幂等去重表**随快照序列化**（否则重启后重复请求被二次 apply） | A 组 10 |

### 1.2 非目标（明确推迟）

- 成员变更、客户端任意节点路由（**M4**）
- 增量快照、并发/后台快照线程、压缩算法（ZSTD）、多个快照副本（**M5**）
- gRPC、异步 transport（**M5**）
- 断点续传式 InstallSnapshot（M5 可选，字段已预留）

---

## 2. 与 M2 的关系：决策 D2 与不变量

### 2.1 决策 D2（边界铁律）

- 快照**只允许覆盖"已提交且已 apply"（`≤ lastApplied`）**的日志前缀；**绝不快照未 apply 条目**。
- 持久化真相源 = **最新快照 + 快照点之后的 raft.log**（二者共同构成，缺一不可）。
- **顺序铁律**：必须先 **durable 快照文件**，再 **compact 删日志**；顺序颠倒 → 崩溃丢数据。

### 2.2 不变量

沿用 `m2-prerequisites.md` 的 I1–I8，新增：

- **I9**：`lastIncludedIndex ≤ lastApplied ≤ commitIndex`
- **I10**：日志首个可用下标 `firstIndex = lastIncludedIndex + 1`

---

## 3. 决策记录（①–⑥）

### 决策① 快照触发策略 —— 选定 A 为主 + C 为辅

- A（选定）日志条数阈值：`lastApplied - lastIncludedIndex ≥ snapshotThresholdEntries`（默认 10000）。
- B（推迟 M5）日志字节阈值：需读文件大小，小 value 下行为怪。
- C（选定为辅）手动触发：admin/CLI 触发一次快照，e2e 演示与运维刚需。

理由：A 确定性强、单测易构造、直接达成 G2；C 成本极低。

### 决策② 快照文件布局与原子性 —— 选定 A

- A（选定）临时文件 + fsync + rename + fsync 目录；`<dataDir>/raft/snapshot.dat`，**只留最新一份**。
- B（否）直接覆盖 + CRC：崩溃窗口内旧快照已毁，可能无快照可用。
- C（否）双缓冲 A/B 保留两份：单机演示 YAGNI，且需管理两份清理。

### 决策⑤ 快照生成路径与锁纪律 —— 选定 B（两段式）

- A（否）锁内直接序列化：**持锁序列化整份 KV**，违反新纪律、大 KV 阻塞 ticker/propose。
- **B（选定）两段式**：锁内取一致性视图（内存拷贝，无序列化/IO）→ 锁外序列化 + 落盘 → 回锁校验后 compact 并更新边界。
- C（否）后台线程 + 双缓冲：锁内 O(1) 但需 active 从 frozen 重建（仍 O(n)）或 COW 结构，复杂度高。

生成时机：**ticker 线程**（`tick()` 顶部，单线程串行）调用 `maybeSnapshot()`。

### 决策④ LogStore 前缀压缩方式 —— 选定 A

- A（选定）原地重写：把 `(upTo, lastIndex]` 记录重写 → `raft.log.tmp` → fsync → rename 覆盖 `raft.log`；内存 `entries_/offsetOf_` 前移。
- B（否）段文件：需段索引/跨段 slice/段 GC，超 M3 范围。
- C（否）纯逻辑压缩：日志仍无界，违反 G2。

**compact 后边界语义（冻结）**：

| API | 语义 |
|---|---|
| `firstIndex()`（新增） | `lastIncludedIndex + 1`；无快照时 = 1 |
| `lastIncludedIndex()/lastIncludedTerm()`（新增） | 压缩边界 |
| `termAt(i)` | `i == lastIncludedIndex` → `lastIncludedTerm`；`i < lastIncludedIndex` → `kNoTerm`（调用方不得查询）；`i > lastIndex` → `kNoTerm` |
| `slice(from,…)` | `from` 先 clamp 到 `max(from, firstIndex)` 再取 |
| `lastIndex()/lastTerm()` | 不变 |
| 边界来源 | **快照文件权威携带**；启动先加载快照再 `log_.setBoundary(idx,term)`；**不改 `meta.dat` 格式** |

### 决策③ InstallSnapshot —— 选定 B（分块，不做断点续传）

- A（否）单包：超 `TcpTransport` 64 MiB 帧上限即失败，内存峰值 = 整份快照。
- **B（选定）分块**：`offset + data + done`，chunk 默认 1 MiB；中断则下次从头重传。
- C（推迟 M5）分块 + 断点续传：`nextOffset` 字段已预留。

### 决策⑥ 启动恢复顺序 —— 选定 A（RaftNode 构造器内聚）

- **A（选定）**：`RaftNode` 构造器内按五步执行（见 §5.3）。
- B（否）：把恢复顺序放在 `main_raft_node`，会把 Raft 一致性知识外泄到进程入口。

---

## 4. 接口定义

### 4.1 StateMachine 最小扩展（`src/raft/state_machine.h`，新增 2 个带默认实现的虚接口）

```cpp
// 不可变一致性视图：由 snapshotView() 在锁内产生，只允许锁外 serialize()
class SnapshotView {
 public:
  virtual ~SnapshotView() = default;
  virtual Bytes serialize() const = 0;
};

class StateMachine {
 public:
  virtual ~StateMachine() = default;
  virtual void apply(const LogEntry& entry) = 0;
  virtual bool get(const std::string& key, std::string& out) const = 0;
  virtual Index lastApplied() const = 0;

  // 锁内调用：取一致视图（内存拷贝；禁止序列化 / IO）。nullptr = 不支持快照。
  virtual std::shared_ptr<const SnapshotView> snapshotView() const { return nullptr; }
  // 锁内调用：用 payload 整体替换状态（纯内存反序列化，无 IO）。
  virtual bool restore(const Bytes& payload) { (void)payload; return false; }
};
```

`KvStateMachine` 实现：视图拷贝 `data_ + lastRequest_ + lastApplied_`；`restore()` 反序列化并整体替换三者（G5）。

### 4.2 SnapshotStore（新增 `src/raft/snapshot_store.h`）

```cpp
struct SnapshotData {
  Index lastIncludedIndex = kNoIndex;
  Term  lastIncludedTerm  = kNoTerm;
  Bytes payload;                       // StateMachine 序列化结果
};

class SnapshotStore {
 public:
  virtual ~SnapshotStore() = default;
  // 无快照 / 损坏（魔数/版本/CRC 失败）→ false（调用方回落全量日志）
  virtual bool load(SnapshotData& out) = 0;
  // 原子持久化：tmp → fsync → rename → fsync(dir)；返回 true 表示已 durable。
  // 禁止在 RaftNode::mu_ 锁内调用。
  virtual bool save(const SnapshotData& data) = 0;
  // Follower 分块接收：顺序写 snapshot.recv；done=true 时校验并原子安装为 snapshot.dat。
  virtual bool receiveChunk(Index lastIncludedIndex, Term lastIncludedTerm,
                            uint64_t offset, const Bytes& data, bool done) = 0;
};
```

适配器：`MemorySnapshotStore`（A 组，无 IO）、`FileSnapshotStore`（B 组，`<dir>/raft/snapshot.dat` + `.recv`）。

### 4.3 LogStore 扩展（`src/raft/log_store.h`）

```cpp
  // 设置压缩边界（启动恢复时由快照恢复；或 compact 时写入）
  virtual void setBoundary(Index lastIncludedIndex, Term lastIncludedTerm) = 0;
  // 物理丢弃 [firstIndex, upTo] 前缀（upTo 必须 <= lastIndex；由 RaftNode 保证 <= lastApplied）
  virtual bool compact(Index upTo, Term termAtUpTo) = 0;
  virtual Index firstIndex() const = 0;
  virtual Index lastIncludedIndex() const = 0;
  virtual Term  lastIncludedTerm() const = 0;
```

`MemoryLogStore` 与 `FileLogStore` 同步实现；`FileLogStore::compact` 用 §3 决策④ 的重写 + rename。

**D3 修订：索引基址不变量（必须维持）** —— 压缩后日志不再从 index 1 开始，LogStore 必须显式维护边界与基址：

- 内部维护 `lastIncludedIndex_` / `lastIncludedTerm_` 与基址 `firstIndex_ = lastIncludedIndex_ + 1`（无快照时 `lastIncludedIndex_ = 0, firstIndex_ = 1`）。
- 按位置的访问一律用 `entries_[i - firstIndex_]`，**禁止硬编码 `i - 1`**。
- `lastIndex() = entries_.empty() ? lastIncludedIndex_ : entries_.back().index`。
- `lastTerm()  = entries_.empty() ? lastIncludedTerm_  : entries_.back().term`（**完全压缩后不得返回 `kNoTerm`**）。
- `append(entries)` 要求 `entries.front().index == lastIndex() + 1`，否则返回 false。
- `load()` 的连续性校验相对 `firstIndex_`：首条记录 index 必须等于 `firstIndex_`，其后逐条 +1；**不得硬编码从 1 开始**（否则 compact 后重启会误判损坏并截断整份日志）。
- `compact(upTo, termAtUpTo)`：先 `setBoundary(upTo, termAtUpTo)`，再物理丢弃 `[firstIndex_, upTo]`（`MemoryLogStore` 用 `erase` 后重置基址；`FileLogStore` 重写保留段 + rename）。

### 4.4 RaftNode 扩展（`src/raft/raft_node.h/.cpp`）

```cpp
  // SnapshotStore 可选：nullptr = 关闭快照（M2 调用点零改动可编译）
  RaftNode(RaftConfig cfg, LogStore& log, StateMachine& sm, Transport& transport,
           Clock& clock, SnapshotStore* snapshots = nullptr);
  ...
  void triggerSnapshot();            // 手动触发（置位，由下一次 tick 消费）
  Index lastIncludedIndex() const;   // status/测试用
  Term  lastIncludedTerm() const;
```

`RaftConfig` 新增：

```cpp
  size_t snapshotThresholdEntries = 10000;   // 触发阈值（条数）
  size_t snapshotChunkBytes = 1u << 20;      // InstallSnapshot 分块大小（1 MiB）
```

### 4.5 消息扩展（`src/raft/message.h/.cpp`，复用 M2 长度前缀帧）

新增 `MsgType::kInstallSnapshot = 5`、`MsgType::kInstallSnapshotReply = 6`，以及
`MsgType::kSnapshotTrigger = 13`（客户端 `snapshot` 命令手动触发一次快照，服务端仅置位 `snapshotRequested_`，
由下一个 tick 消费；v1.3 补记）。

```cpp
struct InstallSnapshotArgs {
  Term term = kNoTerm; int leaderId = 0;
  Index lastIncludedIndex = kNoIndex; Term lastIncludedTerm = kNoTerm;
  uint64_t offset = 0;                 // 本块在快照文件中的偏移
  bool done = false;
  Bytes data;                          // 原始快照文件字节 [offset, offset+len)
};
struct InstallSnapshotReply {
  Term term = kNoTerm;
  bool success = false;
  uint64_t nextOffset = 0;             // M5 断点续传预留；M3 恒为 0
};
```

字节布局（BE）：

```
InstallSnapshot      : term(8) leaderId(4) lastIncludedIndex(8) lastIncludedTerm(8)
                       offset(8) dataLen(4) done(1) data[dataLen]      -> 41 + dataLen
InstallSnapshotReply : term(8) success(1) nextOffset(8)               -> 17
```

---

## 5. 快照文件格式、持久化顺序与算法规则

### 5.1 快照文件格式（冻结）

```
0   magic(4)  = "RKS1"
4   version(1)= 0x01
5   lastIncludedIndex(8, BE)
13  lastIncludedTerm(8, BE)
21  payloadLen(4, BE)
25  crc32(4, BE)  —— 覆盖 payload
29  payload[payloadLen]
```
CRC32 与 M2 同多项式。`payloadLen == 0` 合法（空状态机），此时 CRC 为空串 CRC。

### 5.2 写入与原子性

`FileSnapshotStore::save`：写 `snapshot.dat.tmp` → `fsync(fd)` → `rename` 为 `snapshot.dat` → `fsync(dir)`；返回 true 才算 durable。只保留最新一份。
`receiveChunk`：顺序追加 `snapshot.recv`（要求 `offset == 当前文件大小`，否则重置为 0 重新接收）；`done` 时校验魔数/版本/CRC → `rename` 为 `snapshot.dat`（并 fsync 目录）。

### 5.3 启动恢复顺序（冻结五步）

1. `SnapshotStore::load()` —— 成功则得 `(lastIncludedIndex, lastIncludedTerm, payload)`；失败/损坏（magic/version/CRC）→ 视为无快照。
2. `sm_.restore(payload)`（有快照时）→ `lastApplied_ = lastIncludedIndex`。**v1.3：若 decode 成功但 `restore` 失败（payload/版本不匹配）→ 视为不可恢复，构造器抛异常拒绝启动**——此时 compact 已删掉被覆盖的前缀，回退全量重放会把日志当成 torn tail 清空（见 §12-B10）。
3. **`log_.setBoundary(lastIncludedIndex, lastIncludedTerm)`（有快照时）** —— **必须早于第 4 步 `log_.load()`**：D3 修订要求 `load()` 按 `firstIndex_` 校验连续性，边界需先就位。
4. `log_.load(term, votedFor, lastIndex)` —— 含 torn-tail 截断；按 `firstIndex_` 校验首条记录。**v1.3：首条有效记录若 `index > firstIndex_`（缺前缀）→ 返回 false，构造器拒绝启动**，不再静默 `ftruncate(0)`。load 失败同样拒绝启动（不得带着未验证的日志参与选举）。
5. `lastApplied_ = commitIndex_ = lastIncludedIndex`；`syncedIndex_ = log_.lastIndex()`（磁盘上恢复出来的条目都是持久的）。

**崩溃一致性论证**：D2 保证"先 durable 快照、后 compact"，所以 **torn-snapshot 只可能出现在从未 compact 的场景**，此时日志前缀仍在 → 回退全量日志重放，不丢数据。

### 5.4 生成流程（两段式，`maybeSnapshot()`）

```
锁内：若 lastApplied_ - lastIncluded_ >= threshold（或手动触发位）：
        snapshotRequested_ = false;
        snapIndex = lastApplied_; snapTerm = log_.termAt(snapIndex);
        若无有效边界（snapIndex==0 或 snapTerm==kNoTerm）→ 返回；
        epoch = installEpoch_;                     // v1.3（B5）
        view = sm_.snapshotView();                 // 内存拷贝，无 IO
解锁：payload = view->serialize();                 // 序列化（锁外）
        ok = snapshots_->save({snapIndex, snapTerm, payload});   // 落盘（锁外，且 store 内部拒绝回退边界）
锁内（回锁）：若 epoch != installEpoch_ → 放弃（期间装了更新的快照）；
        若 snapIndex <= commitIndex_ 且 snapIndex > lastIncluded_：
          log_.compact(snapIndex, snapTerm)        // 先快照后 compact；返回 false → 视为存储故障抛异常
          lastIncluded_ = snapIndex; lastIncludedTerm_ = snapTerm;
          snapshotBytes_ = encode(...); 各 peer 的 snapshotSendOffset_ 归零
```

**v1.3 说明**：`compact` 成功后才推进边界（B12）；`installEpoch_` 在每次接受 InstallSnapshot 时自增，
使"落盘完成但期间被更新的快照覆盖"这一次生成被丢弃（B5）。

### 5.5 复制与安装规则

- **Leader**：若 `nextIndex_[peer] <= lastIncluded_` → 发 **InstallSnapshot**（每 tick 每 peer 至多 1 个 chunk，避免阻塞 ticker）；`onInstallSnapshotReply(success)` → `snapshotSendOffset_ = snapshotChunkEnd_`；`done` 后 `nextIndex_[peer] = lastIncluded_ + 1`。**v1.3（B7）**：回复失败/超时（丢回复）时把 `snapshotSendOffset_[peer]` 归零重传；接收端对重复 chunk 幂等，因此"重发同一 offset"不会破坏传输。
- **Follower**：
  - 若 `args.term < currentTerm_` → 拒绝。
  - 若 `args.lastIncludedIndex <= 自己 lastIncluded_` → **忽略**（旧快照，不得回退状态），回 `success=false` + 自己的边界+1。
  - 否则 `snapshots_->receiveChunk(...)`（store 内部串行；**v1.3（B7/B14）**：只接受 `offset == writtenEnd` 追加，已被覆盖的 chunk 视为幂等成功，(index,term) 变化或 offset 不连续则以 `success=false` 让 leader 从 0 重传）。
  - `done` 后：`sm_.restore(payload)`、**v1.3（B8）** 仅当 `log_.termAt(lastIncludedIndex) == lastIncludedTerm` 才保留后缀，否则丢弃整段后缀、`log_.compact(...)`（失败 → 抛异常）、`++installEpoch_`、**v1.3（B6）** `snapshotBytes_ = encodeSnapshotFile(installed)`、**v1.3（B9）** `lastApplied_ = lastIncludedIndex` 且 `commitIndex_ = max(commitIndex_, lastIncludedIndex)`、`syncedIndex_ = max(...)`，最后 `advanceCommitAndApply()` 立刻重放仍已提交的后缀、重置选举超时、回 `success=true`。
- **AppendEntries 落在边界以下**：Follower 回 `conflictIndex = firstIndex()`；Leader 见 `nextIndex <= lastIncluded_` → 转快照路径（§5.5 第一条）。**v1.3（B13）**：Leader 的 fast-backup 回退值夹紧到 `>= lastIncluded_`（不得再把 AppendEntries 发进压缩区）；组提交（`propose`）的复制路径对 `nextIndex_ <= lastIncluded_` 的 peer 直接跳过，交由 tick 的快照路由。

---

## 6. 线程安全与锁纪律

沿用 M2 的 L1–L7（单锁 `mu_`；锁内禁 fsync/网络 IO；出站队列；propose 用 cv）。**新增 L8**：

- **L8**：锁内**只允许**"取快照边界 + 取一致性视图（内存拷贝）"；**禁止**在锁内序列化整份 KV、禁止锁内 `SnapshotStore::save/receiveChunk`（磁盘 IO）。
- 允许的例外：`sm_.restore(payload)` 为**纯内存反序列化**（无 IO），在锁内执行（InstallSnapshot 安装路径）。
- **L9（v1.3，D5 正解）**：组提交把 `LogStore::sync()` 放到了 `mu_` **之外**（锁内 fsync 会阻塞心跳/入站 RPC），
  因此 fd 生命周期必须由 **LogStore 内部锁**保护：`FileLogStore` 新增 `mu_`，`sync/load/persistMeta/truncateSuffix/compact`
  （即所有触碰 `logFd_` 或重建/改名日志文件的操作）都在其内；`appendNoSync` 不加该锁，因为它只由 `mu_` 串行调用
  （与所有 fd 变更互斥）。锁序固定为 **`RaftNode::mu_` → `LogStore::mu_`**，反向获取不存在，故无死锁。
- **SnapshotStore 内部锁（v1.3）**：`save/receiveChunk/load` 由 store 自己的互斥量串行（多连接并发 InstallSnapshot
  与 `maybeSnapshot` 的锁外 `save` 可能并发），且**任何写路径都不得把已落盘边界往回退**。

---

## 7. 风险与对策

| 风险 | 后果 | 对策 |
|---|---|---|
| 快照覆盖未 apply 前缀 | 提交语义破坏 | D2 + `snapIndex = lastApplied_`；A 组测试 2 |
| compact 后 `termAt/slice` 边界语义破坏 | 复制/选主误判 | §3 边界语义表 + A 组测试 4 |
| 旧快照覆盖新状态 | 数据回退 | Follower 忽略 `lastIncludedIndex ≤ 自己边界` 的快照；A 组测试 7 |
| 先删日志后落快照 | 崩溃丢数据 | §5.4 顺序 + B 组测试 11/12 |
| torn-snapshot | 启动异常 | 魔数/版本/CRC 校验 → 丢弃回落全量日志；B 组测试 12 |
| 去重表未随快照序列化 | 重启后二次 apply | §4.1 + A 组测试 10 |
| 快照传输期间 propose 阻塞 | 可用性下降 | 每 tick 每 peer 1 chunk；M5 异步化 |
| InstallSnapshot 单包超 64 MiB | 传输失败 | 分块（1 MiB）+ 帧上限校验；A 组测试 5 |
| 压缩后日志"空洞" | slice 越界 | `firstIndex` clamp；A 组测试 3/4 |

---

## 8. 测试矩阵（新增 `tests/raft_snapshot_test.cpp`）

**A 组（FakeClock + MemoryTransport + MemoryLogStore + MemorySnapshotStore，无 IO/无 flaky）**

| # | 用例 | 断言要点 |
|---|---|---|
| 1 | `test_snapshot_created_at_threshold` | 超过阈值后生成快照；`lastIncludedIndex ≤ lastApplied` |
| 2 | `test_snapshot_only_covers_applied_prefix` | 快照边界恒 `≤ lastApplied`（构造未 apply 条目不得被覆盖） |
| 3 | `test_compact_truncates_log_and_first_index` | compact 后 `firstIndex == lastIncluded + 1`，日志条数下降 |
| 4 | `test_term_at_compacted_boundary_semantics` | `termAt(lastIncluded) == lastIncludedTerm`；`termAt(<lastIncluded) == kNoTerm`；`slice` clamp |
| 5 | `test_install_snapshot_catches_up_lagging_follower` | 落后 follower 收快照后 `lastApplied/commitIndex` 追平；`nextIndex` 复位 |
| 6 | `test_follower_keeps_suffix_beyond_snapshot` | 安装快照后保留 `> lastIncludedIndex` 的后缀日志 |
| 7 | `test_stale_snapshot_ignored` | `lastIncludedIndex ≤ 自己边界` 的旧快照被忽略，状态不回退 |
| 8 | `test_sync_542_still_holds_across_compaction` | 压缩边界前后的旧 term 条目仍不得"仅凭多数派"提交 |
| 9 | `test_propose_not_leader_unaffected_by_snapshot` | 快照路径不影响降级后 `propose → kNotLeader` |
| 10 | `test_dedup_table_survives_snapshot` | restore 后同一 `(clientId,requestId)` 仍被去重 |

**B 组（FileLogStore + FileSnapshotStore + 真实临时目录）**

| # | 用例 | 断言要点 |
|---|---|---|
| 11 | `test_restart_loads_snapshot_then_replays_tail` | 重启后状态 = 快照 + 日志尾部重放 |
| 12 | `test_torn_snapshot_discarded` | 损坏 `snapshot.dat` 被丢弃并回落全量日志重放 |
| 13 | `test_torn_tail_after_compact` | compact 后日志 torn-tail 正确截断 |
| 14 | `test_snapshot_and_log_combined_recovery` | 快照 + 日志组合恢复出完整 KV |

**既有用例保持不变**：`raftkv_tests`（M1 13 例）、`tests/raft_*_test.cpp`（M2 14 例）、`scripts/e2e.sh`、`scripts/raft_e2e.sh`、`scripts/raft_fault.sh` 断言全部保持。

---

## 9. 里程碑拆分（M3.1–M3.5）

| 阶段 | 交付物 | 验收 |
|---|---|---|
| **M3.1** | `StateMachine` 快照接口、`KvStateMachine` 序列化（含去重表）、`SnapshotStore` + `MemorySnapshotStore`、`RaftNode` 按 `lastApplied` 生成快照 + `lastIncluded*`、`LogStore::compact` 前缀截断与边界 | A 组 1/2/10 绿 |
| **M3.2** | 触发策略（`snapshotThresholdEntries` + 手动触发）、两段式锁纪律、compact 后 `slice/termAt/lastIndex/firstIndex` 边界语义 | A 组 1–4、10 全绿 |
| **M3.3** | InstallSnapshot(5/6) 编解码、Leader `nextIndex ≤ lastIncluded` 改发快照（分块）、Follower 安装/保留后缀/忽略旧快照 | A 组 5–9 绿；`raft_snapshot_e2e.sh` 前半 |
| **M3.4** | `FileSnapshotStore`（tmp+rename+fsync、CRC、torn-snapshot 丢弃、只留最新）、启动恢复（先快照后日志） | B 组 11–14 绿；`raft_snapshot_e2e.sh` 全绿 |
| **M3.5** | 10w+ 条后 `raft.log` 有界、空节点经 InstallSnapshot 快速追平、`raft_snapshot_fault.sh`（生成中 kill -9、传输中 SIGSTOP） | 有界断言 + 追平耗时断言 + `--repeat 50` |

---

## 10. 文件清单（新增 / 最小修改 / 禁止改动）

**新增**
- `src/raft/snapshot_store.h`（`SnapshotData` / `SnapshotStore` / `MemorySnapshotStore` / `FileSnapshotStore` 声明）
- `src/raft/memory_snapshot_store.cpp`
- `src/raft/file_snapshot_store.cpp`
- `tests/raft_snapshot_test.cpp`
- `scripts/raft_snapshot_e2e.sh`、`scripts/raft_snapshot_fault.sh`
- `docs/m3-design.md`（本文档）

**最小修改**
- `src/raft/state_machine.h`：+`SnapshotView` / `snapshotView()` / `restore()`（带默认实现）
- `src/raft/log_store.h`、`src/raft/memory_adapters.cpp`、`src/raft/file_log_store.cpp`：`compact/setBoundary/firstIndex/lastIncluded*` 与边界语义
- `src/raft/message.h/.cpp`：`MsgType::kInstallSnapshot = 5` / `kInstallSnapshotReply = 6` + 编解码
- `src/raft/transport.h`、`src/raft/memory_adapters.cpp`、`src/raft/transport_tcp.{h,cpp}`：**追加** `sendInstallSnapshot(peerId, args, cb)`（**D4**；既有 3 个方法语义不变）
- `src/raft/types.h`：`RaftConfig` +`snapshotThresholdEntries` / `snapshotChunkBytes`
- `src/kv/kv_state_machine.h/.cpp`：`snapshotView()` / `restore()`（KV map + 去重表 + lastApplied）
- `src/raft/raft_node.h/.cpp`：可选注入 `SnapshotStore*`、`maybeSnapshot()`、InstallSnapshot 收发、边界状态
- `src/main_raft_node.cpp`：注入 `FileSnapshotStore`、status 扩展快照信息、手动触发
- `src/main_raft_client.cpp`：`status` 显示快照信息
- `CMakeLists.txt`：`raftkv_raft` 追加 M3 源文件；`raftkv_raft_tests` 追加 `tests/raft_snapshot_test.cpp`
- `tests/raft_test_harness.h`：**仅追加**一个快照版 cluster 构造辅助（既有 `makeCluster` 与全部 M2 用例不动）

**禁止改动**
- M1 全部：`src/common.h`、`codec.*`、`thread_pool.h`、`wal.*`、`store.*`、`server.*`、`main_server.cpp`、`main_client.cpp`、`tests/test_*.cpp`、`scripts/e2e.sh`
- M2 算法路径：`RaftNode` 的选主/日志复制/§5.4.2 提交规则逻辑、既有 `tests/raft_*_test.cpp` 用例、`scripts/raft_e2e.sh`、`scripts/raft_fault.sh`

---

## 11. 自检记录（占位符 / 矛盾 / 歧义 / 范围）

1. **占位符扫描**：无 `TBD/TODO/待定`；所有阈值（10000 条、1 MiB）、格式（快照 29B 头、消息 41+dataLen / 17B）均为具体值。
2. **内部一致性**：D2（先快照后 compact）与 §5.4 生成流程一致；§4.3 边界语义与 §7 风险对策一致；§8 用例编号与 §9 里程碑验收一一对应。
3. **歧义消除**：明确"锁内允许取视图（内存拷贝）、禁止锁内序列化/IO"（L8）；明确 `termAt(lastIncludedIndex)` 必须返回答（不是 `kNoTerm`）；明确旧快照判定为 `lastIncludedIndex ≤ 自己边界`。
4. **范围检查**：单一子系统（快照+压缩），M3.1–M3.5 每步可独立测试；成员变更/异步 transport/增量快照已明确划入非目标。
5. **接口稳定性**：`SnapshotStore*` 以可选指针注入（默认 `nullptr`），M2 全部既有调用点（harness / `main_raft_node`）零改动即可编译，快照功能在未注入时关闭。
6. **修订记录**：**v1.1**（#1 发现 D3）——§4.3 增加"索引基址不变量"、§5.3 把 `setBoundary` 提到 `load` 之前；**v1.2**（#2 发现 D4）——§10 补齐 `transport.h` / `transport_tcp.{h,cpp}` 的 `sendInstallSnapshot` 追加项。其余内容与已批准的 v1.0 一致。
7. **v1.3 修订记录**（#4 独立评审，15 个阻断项）：§4.5 补 MsgType 13；§5.3 澄清"`setBoundary` 早于 `log_.load()`"的措辞并新增恢复失败的致命化规则；§5.4 补 epoch/compact-先行；§5.5 重写 Leader 重传与 Follower 安装规则；§6 新增 L9（LogStore fd 锁下沉）与 SnapshotStore 内部锁。详见 §12。

---

## 12. #4 评审修订（v1.3 决策表）

| 编号 | 问题 | 决策 / 实现 |
|---|---|---|
| B1 | 单节点 cluster 的 `propose` 组提交后无人推进 commit | flusher 回锁段内调用 `advanceCommitAndApply()`（无 peer 任务时 majority == self） |
| B2 | `log_.sync()` 返回值被忽略，fsync 失败仍计自持久 | 失败则不推进 `syncedIndex_`、清 `syncInFlight_`，`propose` 返回 `kErr` |
| B3 | `flushTarget` 与并发 truncate 竞争导致过报持久 | 回锁后夹紧 `durable = min(flushTarget, log_.lastIndex())`（truncate/compact 保留段必然已 fsync） |
| B4 | 锁外 `sync()` 与锁内 `compact()` 争同一 fd | **L9**：`FileLogStore::mu_` 保护 fd 生命周期（`sync/compact/load/persistMeta/truncateSuffix`） |
| B5 | `maybeSnapshot` 锁外 `save` 与 InstallSnapshot 竞争覆盖 | store 侧拒绝 `<= 已落盘边界` 的 save；RaftNode 侧用 `installEpoch_` 丢弃过期生成 |
| B6 | 经 InstallSnapshot 追平的节点当选后无法再服务快照 | 安装成功后 `snapshotBytes_ = encodeSnapshotFile(installed)`，并归零各 peer 发送偏移 |
| B7 | 丢回复 → 重复追加 / 永久 offset 不匹配，传输卡死 | 接收端幂等（`offset+len <= writtenEnd` 即成功）+ offset 不连续则重启；Leader 收到失败回复即把偏移归零 |
| B8 | 保留后缀未校验 term；`commitIndex = min(leaderCommit, lastIndex)` | 后缀 term 不匹配则整段丢弃；`commitIndex_ = min(leaderCommit, prevLogIndex+entries)`，且只增不减 |
| B9 | 安装快照无条件回退 `lastApplied_/commitIndex_`（违反 I4） | `commitIndex_ = max(...)`；`lastApplied_` 与 restore 后状态机一致，并在同一锁段内 `advanceCommitAndApply()` 追平（对外不可见回退） |
| B10 | decode 成功但 `restore` 失败 → `load` 把缺前缀当 torn tail 清空日志 | 构造器对"restore 失败"与"缺前缀（首条 `index > firstIndex_`）"一律抛异常拒绝启动 |
| B11 | 构造器忽略 `log_.load()` 返回值 | load 失败 → 抛异常，绝不带残缺日志参与选举 |
| B12 | `compact` 不 fsync 目录、reopen 失败半提交、上层忽略返回值 | compact 成功后 `fsyncDir`；reopen 失败即返回 false（上层抛异常）；只有成功才推进 RaftNode 边界 |
| B13 | 缺 §5.5 "prevLogIndex 落在压缩区以下"守卫；组提交路径绕过快照路由 | Follower 在 `prevLogIndex` **严格小于** `lastIncludedIndex` 时立即回 `conflictIndex = firstIndex()`（`== lastIncludedIndex` 的边界条目仍可校验，不能一并拒绝）；Leader 的 fast-backup 接受 peer 给出的提示（含"对方压缩边界更靠前"的**前跳**），并夹紧到不低于自己的 `lastIncluded_`；组提交路径跳过 `nextIndex_ <= lastIncluded_` 的 peer，改由 tick 走快照路由 |
| B14 | 并发 InstallSnapshot 对无锁 store 并发 `receiveChunk` | store 内部互斥 + (index,term) 传输标识（同 B5 的锁） |
| B15 | `fill --pipeline` 共享 `clientId` → 乱序 requestId 被静默丢弃 | 每个 worker 独立 `clientId`；新增 `verify <n>` 子命令与 bench 写回校验 |

**顺带修复的优化项**：O1（已存在条目也必须 fsync 后才能 ack 持久）、O5（快照/去重表 count 上界校验，防超大 `reserve`）、
O6（`compact(upTo <= lastIncluded_)` 为 no-op）、O7（`setBoundary` 单次范围 erase）、O9（`persistMeta` 补 `fsyncDir`）、
O10（`truncateSuffix` 缺 offset 不再退化成 `ftruncate(0)`，并 fsync）。
**明确推迟到 M5**：O2（cv 谓词加固；当前谓词为提交/降级条件，通知均伴随状态变化，未发现丢唤醒路径）、
O3（tick 的 noop 仍走 `log_.append()`：每任期一次 fsync，与 M2 行为一致）、O4（快照内存峰值流式化）、
O8（边界以下损坏记录跳过）、O11（per-peer 异步 transport）、O13（`.recv` 落盘带 (index,term) 头）、
O15（GTest 缺失改为 fatal）、O17（`main_raft_node` 线程池化）。
