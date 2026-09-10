# M2 设计文档：Raft 选主 + 日志复制

> 状态：待评审（评审通过后再拆成逐步实施计划）
> 关联：[roadmap.md](roadmap.md) · [protocol.md](protocol.md) · M1 代码（src/）
> 目标读者：实现者（本项目作者本人）与"未来的你"（面试前复习）

---

## 1. 目标与非目标

### 1.1 目标（每条都可验收）

| 编号 | 目标 | 验收方式 |
|---|---|---|
| G1 | 3 节点静态集群，任一点可接受客户端连接 | `scripts/raft_e2e.sh` 启动 3 进程 |
| G2 | 选主：任意时刻至多一个 Leader（同一 term 内） | 单测 `test_only_one_leader_per_term` |
| G3 | kill -9 Leader 后 ≤ 2s 选出新 Leader，写服务恢复 | `raft_e2e.sh` 断言 `status` |
| G4 | 写只被 Leader 接受，多数派 fsync 后才回复 OK | 单测 `test_commit_requires_majority` + e2e 双节点宕机 |
| G5 | 少数派 Leader 绝不返回假成功（宁可不服务） | e2e：停 2 节点后写必须失败 |
| G6 | 崩溃恢复：kill -9 任意节点重启后 term/votedFor/log 恢复 | 单测 12 + e2e 崩溃恢复 |
| G7 | 断线重试幂等：同 `requestId` 重试不重复应用 | 单测 `test_kv_apply_idempotent_on_retry` |
| G8 | 一致性读：读走 Leader（简化版线性一致） | e2e：向 Follower 发起 `get` 返回 NOT_LEADER + hint |
| G9 | 测试稳定性：故障注入脚本连续 50 次无 flaky | `scripts/raft_fault.sh --repeat 50` |

### 1.2 非目标（明确推迟，避免范围蔓延）

- 快照与日志压缩（**M3**）
- 在线成员变更（**M4**）
- ReadIndex / Lease Read 优化读（**M5**）
- 批处理 / 组提交 / pipeline 复制（**M5**）
- gRPC 迁移、TLS/鉴权、多分片、跨机部署（**M5 或不做**）
- 异步/协程网络模型（M2 沿用 M1 的线程池模型，M5 再换 epoll）

---

## 2. 与 M1 的关系：复用什么、改什么

| M1 组件 | M2 中的角色 | 动作 |
|---|---|---|
| `common.h` 大端编解码、日志 | RPC 消息编解码 | 复用 |
| `codec.{h,cpp}` 长度前缀帧 | peer RPC 与客户端协议统一帧格式 | 扩展（加消息类型字段） |
| `thread_pool.h` | RPC 服务端 worker + 出站 RPC 执行池 | 复用 |
| `server.cpp` accept/连接处理模式 | RPC 监听与连接分发 | 复用模式，新增消息分发 |
| `wal.{h,cpp}` CRC 记录格式与 torn-tail 处理 | **LogStore 的持久化格式基础** | 复用思路 + 扩展字段 |
| `store.{h,cpp}` | **降级为纯内存 StateMachine** | **重构：去掉它自带的 WAL** |
| `main_client.cpp` | 加 Leader 重定向 + 重试 + `status` 命令 | 扩展 |
| `main_server.cpp` | 变为"单节点 M1 模式"的保留入口 | 保留，新增 `main_raft_node.cpp` |

### 2.1 关键决策 D1：KV 不再自带 WAL

**决策**：M2 起，持久化的唯一真相来源是 **Raft log**；`StateMachine` 只保存内存状态 + 幂等去重表。

**理由**：若 KV 自己再写一份 WAL，就会出现两份日志，必须回答"谁先谁后、崩溃时以谁为准"，而这正是 Raft 要解决的问题本身。把持久化职责上移到 Raft log 后，"先持久化后可见"这条 M1 的不变式仍然成立，只是执行者换成了 Raft。

**代价**：M1 的 `store.cpp` 单测需要改写（去掉重放用例，改为状态机 apply 用例）；M1 单机模式若要保留，需要用 `--mode standalone` 决定是否走 Raft 包装。

---

## 3. 架构与数据流

### 3.1 部署形态（M2：同一台 VM 上 3 个进程）

```
   node1:9601        node2:9602        node3:9603
 ┌────────────┐   ┌────────────┐   ┌────────────┐
 │ RPC server │◀─▶│ RPC server │◀─▶│ RPC server │   节点间：RequestVote / AppendEntries
 │ RaftNode   │   │ RaftNode   │   │ RaftNode   │
 │ LogStore   │   │ LogStore   │   │ LogStore   │   fsync 落盘
 │ KV SM      │   │ KV SM      │   │ KV SM      │   已提交条目按序 apply
 └────────────┘   └────────────┘   └────────────┘
        ▲
        │ 客户端协议（复用 M1 帧 + 新状态码）
   raftkv_cli
```

### 3.2 单次写的完整路径

```
CLI ──ClientRequest(op,key,value,clientId,requestId)──▶ 任意节点
  ├─ 非 Leader → ClientReply{kNotLeader, leaderHint}    ← CLI 自动重试到 hint
  └─ Leader:
       1) LogStore.append(entry) + fsync        ← 持久化点（回复 OK 的前提）
       2) 广播 AppendEntries(prevLogIndex/prevLogTerm/entries/leaderCommit)
       3) 多数派 success ⇒ 推进 commitIndex（仅限当前 term 的条目，见 §6.4）
       4) 按序 apply 到 StateMachine ⇒ 唤醒等待中的 propose()
       5) 回复 ClientReply{kOk}
```

### 3.3 读路径（M2 简化）

客户端 `get` → 必须打到 Leader → Leader 直接读 StateMachine（已提交状态）。
Follower 收到客户端读写一律回 `kNotLeader` + `leaderHint`，不发 ReadIndex（M5 再做）。

---

## 4. 模块设计（seam 与接口）

设计原则：**深模块**（小接口、厚实现），seam 只开在真正会替换的地方。

| Seam | 两个适配器 | 为什么值得开这个 seam |
|---|---|---|
| `LogStore` | `FileLogStore` / `MemoryLogStore` | 单测用内存版；崩溃恢复测试用真实文件；**显式可控的"是否 fsync 成功"** |
| `Transport` | `TcpTransport` / `MemoryTransport` | 单测不需要真实网络和端口，可同步投递、可注入丢包/延迟 |
| `Clock` | `SteadyClock` / `FakeClock` | 选举超时是时间驱动的；假时钟让"选主"测试毫秒级且不 flaky |
| `StateMachine` | `KvStateMachine` / （后续）`NullStateMachine` | 状态机可脱离 Raft 单测；M3 快照需要它可序列化 |

> 说明（删除测试）：如果删掉 `Transport` 抽象，每个选举测试都要起真实端口、等真实超时——复杂度会散落到 N 个测试里。它值得保留。

### 4.1 文件结构（新增 / 修改）

```
src/
├── raft/
│   ├── types.h            类型别名(Role/Term/Index)、LogEntry、RPC 消息结构体
│   ├── message.{h,cpp}    消息 ↔ 字节 的编解码（复用 common.h 工具与 M1 帧格式）
│   ├── log_store.{h,cpp}  LogStore 接口 + FileLogStore / MemoryLogStore
│   ├── state_machine.{h,cpp} KV 状态机 + 幂等去重表
│   ├── transport.h        Transport 接口 + MemoryTransport
│   ├── transport_tcp.{h,cpp} 长连接 + 重连的 TCP 适配器
│   ├── clock.h            Clock 接口 + SteadyClock + FakeClock
│   └── raft_node.{h,cpp}  共识核心（选主/复制/提交/角色转换）
├── main_raft_node.cpp     节点进程入口（Raft + KV + 客户端协议服务）
├── main_server.cpp        M1 单机入口（保留）
└── main_client.cpp        扩展：Leader 重定向、重试、status
tests/
├── test_raft_election.cpp  选主与任期（fake clock + memory transport）
├── test_raft_log.cpp       复制、冲突截断、追赶
├── test_raft_commit.cpp    提交规则（含 §5.4.2 反例）
├── test_raft_restart.cpp   持久化与重启恢复（FileLogStore）
└── test_kv_idempotent.cpp  幂等重试
scripts/
├── raft_e2e.sh            3 进程生命周期 + kill -9 Leader
└── raft_fault.sh          分区/宕机注入 + 重复稳定性
```

每个文件一个职责：`raft_node.cpp` 只放共识算法，不含任何 socket/文件系统调用（依赖注入）。

### 4.2 接口定义（签名即契约）

```cpp
// src/raft/types.h
namespace raftkv::raft {

using Term  = uint64_t;
using Index = uint64_t;
constexpr Index kNoIndex = 0;  // 日志下标从 1 开始，0 表示"无"
constexpr Term  kNoTerm  = 0;

enum class Role : uint8_t { kFollower = 0, kCandidate = 1, kLeader = 2 };

struct LogEntry {
  Index index = kNoIndex;          // 由 Leader 赋值，Follower 校验一致性
  Term  term  = kNoTerm;
  raftkv::OpCode op = raftkv::OpCode::kGet;
  std::string key;
  std::string value;               // PUT 才有
  uint64_t clientId = 0;           // 幂等：客户端会话 id
  uint64_t requestId = 0;          // 幂等：会话内自增
};

struct RequestVoteArgs  { Term term; int candidateId; Index lastLogIndex; Term lastLogTerm; };
struct RequestVoteReply { Term term; bool voteGranted; };

struct AppendEntriesArgs {
  Term term; int leaderId;
  Index prevLogIndex; Term prevLogTerm;
  std::vector<LogEntry> entries;   // 心跳时为空
  Index leaderCommit;
};
struct AppendEntriesReply {
  Term term; bool success;
  Index conflictIndex;             // 快速回退（M2.5）
  Term  conflictTerm;
};

struct ClientRequest { raftkv::OpCode op; std::string key; std::string value;
                       uint64_t clientId; uint64_t requestId; };
enum class ClientStatus : uint8_t { kOk = 0, kNotFound = 1, kErr = 2, kNotLeader = 3 };
struct ClientReply   { ClientStatus status; std::string value; int leaderHint = -1; };

struct RaftConfig {
  int selfId = 1;
  std::vector<int> peerIds;                 // 不含自己
  uint64_t electionTimeoutMinMs = 150;
  uint64_t electionTimeoutMaxMs = 300;
  uint64_t heartbeatMs          = 50;
  uint64_t rpcTimeoutMs         = 100;
  size_t   maxEntriesPerAppend  = 128;
  size_t   maxBytesPerAppend    = 1u << 20;
};
}  // namespace raftkv::raft
```

```cpp
// src/raft/log_store.h —— 持久化日志与元数据（term / votedFor）
namespace raftkv::raft {
class LogStore {
 public:
  virtual ~LogStore() = default;

  // 启动恢复：读出 term / votedFor / 最后一条日志下标，并截断 torn tail
  virtual bool load(Term& term, int& votedFor, Index& lastIndex) = 0;

  // 回复 RequestVote 之前必须成功返回（fsync）
  virtual bool persistMeta(Term term, int votedFor) = 0;

  // Leader 接受写 / Follower 接受 AppendEntries 时调用；返回 true 表示已持久化
  virtual bool append(const std::vector<LogEntry>& entries) = 0;

  // 冲突时截断 [fromIndex, lastIndex]
  virtual bool truncateSuffix(Index fromIndex) = 0;

  virtual std::vector<LogEntry> slice(Index from, size_t maxEntries, size_t maxBytes) const = 0;

  virtual Index lastIndex() const = 0;
  virtual Term  lastTerm()  const = 0;   // 空日志返回 kNoTerm
  virtual Term  termAt(Index index) const = 0;  // 越界返回 kNoTerm
};
}  // namespace raftkv::raft
```

```cpp
// src/raft/state_machine.h —— KV 状态机（幂等）
namespace raftkv::raft {
class StateMachine {
 public:
  virtual ~StateMachine() = default;
  virtual void apply(const LogEntry& entry) = 0;      // 必须幂等（见 §6.5）
  virtual bool get(const std::string& key, std::string& out) const = 0;
  virtual Index lastApplied() const = 0;
};
}  // namespace raftkv::raft
```

```cpp
// src/raft/transport.h —— 节点间 RPC 发送（异步回调风格，单测用内存实现）
namespace raftkv::raft {
class Transport {
 public:
  using VoteCb   = std::function<void(const RequestVoteReply&)>;
  using AppendCb = std::function<void(const AppendEntriesReply&)>;
  virtual ~Transport() = default;
  virtual void sendRequestVote(int peerId, const RequestVoteArgs& args, VoteCb cb) = 0;
  virtual void sendAppendEntries(int peerId, const AppendEntriesArgs& args, AppendCb cb) = 0;
};
}  // namespace raftkv::raft
```

```cpp
// src/raft/clock.h
class Clock {
 public:
  virtual ~Clock() = default;
  virtual uint64_t nowMs() const = 0;
};
class SteadyClock : public Clock { /* steady_clock */ };
class FakeClock : public Clock { public: void advance(uint64_t ms); };
```

```cpp
// src/raft/raft_node.h —— 共识核心（不含任何 IO 实现）
namespace raftkv::raft {
class RaftNode {
 public:
  RaftNode(RaftConfig cfg, LogStore& log, StateMachine& sm,
           Transport& transport, Clock& clock);

  void tick();                                     // 由 ticker 每 10ms 调用一次
  RequestVoteReply   onRequestVote(const RequestVoteArgs& args);        // RPC 入口
  AppendEntriesReply onAppendEntries(const AppendEntriesArgs& args);    // RPC 入口
  void onRequestVoteReply(int peerId, const RequestVoteReply& reply);   // 回调入口
  void onAppendEntriesReply(int peerId, const AppendEntriesReply& reply);

  ClientReply propose(const ClientRequest& req, uint64_t timeoutMs);    // 阻塞直到提交/超时/失去领导权

  Role role() const; Term currentTerm() const; int leaderId() const;
  Index commitIndex() const; Index lastApplied() const;
};
}  // namespace raftkv::raft
```

### 4.3 线程模型与锁纪律

| 线程 | 职责 |
|---|---|
| ticker（1 个） | 每 10ms 调 `RaftNode::tick()`：选举超时检查、Leader 心跳与补日志 |
| RPC worker 池（N=4） | 处理入站 RPC：`onRequestVote` / `onAppendEntries`；以及客户端 `propose` |
| 出站 RPC 池（N=4） | 执行 `Transport::send*`（阻塞的网络调用），回调再回到 `RaftNode` |
| accept（1 个） | 复用 M1 模式，收连接后投递给 worker 池 |

**锁纪律（必须遵守，否则必出死锁）**：
1. `RaftNode` 内单一 `std::mutex mu_`，**所有状态读写都在锁内**。
2. **绝不在持锁期间做网络 IO 或 fsync**：需要发送时，先在锁内构造消息放入"出站队列"，解锁后由出站池发送。
3. `propose()` 用 `condition_variable` 等待提交结果；等待时**不持锁**。

---

## 5. 消息与持久化格式

### 5.1 RPC 帧（复用 M1 帧风格）

```
[frameLen:4][msgType:1][payload...]      frameLen 不含自身
msgType: 1=RequestVote  2=RequestVoteReply
         3=AppendEntries 4=AppendEntriesReply
         10=ClientRequest 11=ClientReply        (客户端复用同一帧格式)
```

### 5.2 客户端协议扩展

`ClientReply.status` 新增 **3 = NOT_LEADER**，此时 `value` 字段携带 `leaderHint`（十进制节点 id 或 `"host:port"`）。
M1 的 0/1/2 语义不变，向后兼容。

### 5.3 LogStore 文件布局（`<dataDir>/raft/`）

```
meta.dat   : [crc32:4][len:4][term:8][votedFor:4]                每变更重写（先写临时文件再 rename）
raft.log   : 重复记录 [crc32:4][len:4][index:8][term:8][op:1][klen:4][vlen:4][clientId:8][requestId:8][key][value]
```

- 启动时全量扫描 `raft.log` 建立内存映射 `index → fileOffset`（用于 `truncateSuffix` 定位 `ftruncate` 目标）
- CRC 失败 / 长度不足 → 视为 torn tail，截断到最后一条完整记录（沿用 M1 `wal.cpp` 的做法）
- **只追加 + 尾部截断**，不做原地改写

### 5.4 持久化顺序（正确性的命门）

| 时机 | 必须已持久化的内容 | 不这么做的后果 |
|---|---|---|
| 回复 RequestVote 之前 | `term`, `votedFor` | 崩溃后重复投票 → 同一 term 出现两个 Leader |
| 回复 AppendEntries success 之前 | 该批 `entries` | 已确认的日志丢失，可能违背已提交条目 |
| 推进 `commitIndex` / `apply` 时 | 无需额外持久化（可重放恢复） | —— |

---

## 6. 核心算法规则（逐条可直接实现）

### 6.1 角色与状态转换

```
Follower --选举超时--> Candidate      (term++, votedFor=self, 持久化, 广播 RequestVote)
Candidate --获得多数票--> Leader      (初始化 nextIndex[]=lastIndex+1, matchIndex[]=0, 立即发心跳)
Candidate --收到同/更高 term 的 AppendEntries--> Follower
任意角色  --收到更高 term 的任意 RPC--> Follower (更新 term, votedFor=null, 持久化)
Leader    --收到更高 term 的 RPC--> Follower
```

### 6.2 投票授予条件（全部满足才投票）

```
1) args.term > currentTerm           → 先更新 term、清空 votedFor 并持久化
2) votedFor == 空 或 votedFor == args.candidateId
3) 候选人日志"不旧于"本地日志:
      lastLogTerm > 本地 lastTerm
   || (lastLogTerm == 本地 lastTerm && lastLogIndex >= 本地 lastIndex)
回复前持久化 term/votedFor。
```

### 6.3 AppendEntries 处理

```
1) args.term < currentTerm → 直接拒绝 {term=currentTerm, success=false}
2) 重置选举超时计时器（只有 Leader/合法 Candidate 的心跳才重置）
3) 若 args.prevLogIndex > lastIndex → 失败 {conflictIndex=lastIndex+1}
   若 termAt(prevLogIndex) != args.prevLogTerm → 失败 {conflictIndex=该冲突 term 的首个 index}
4) 逐条写入 entries：同 index 同 term 跳过；同 index 不同 term → truncateSuffix(index) 后写入
5) commitIndex = min(args.leaderCommit, lastIndex)
6) 持久化本批 entries 后才回复 success=true
```

### 6.4 提交规则（Raft §5.4.2，最容易写错的一条）

Leader 只在满足**两个条件**时推进 `commitIndex`：

```
① 存在 N > commitIndex，使得 matchIndex 的多数派 ≥ N
② log[N].term == currentTerm        ← 只靠多数派复制旧 term 的条目不算提交
```

随后把 `(lastApplied, N]` 按序 apply 到状态机。
（单测 `test_old_term_entry_not_committed_by_count` 专门构造这个反例。）

### 6.5 客户端幂等（关闭 M1 遗留的 at-least-once 缺口）

- CLI 启动时生成随机 `clientId`（64 位），每条命令 `requestId` 自增；**重试时复用同一对 id**
- `LogEntry` 携带 `clientId/requestId`；`KvStateMachine` 维护 `unordered_map<uint64_t /*clientId*/, uint64_t /*lastRequestId*/>`
- `apply()` 规则：`requestId <= lastRequestId` → 直接丢弃（重复请求），否则应用并更新
- 去重表属于状态机状态，**M3 做快照时必须一并序列化**

### 6.6 领导权变更时挂起的请求

`propose()` 在等待期间若检测到本节点不再是 Leader（或 term 变化）→ 立即返回 `kNotLeader + leaderHint`，不等超时。
CLI 收到 `kNotLeader` → 连接 hint 节点重试（最多 3 次，总预算 1s）。

---

## 7. 测试策略

### 7.1 确定性单测（FakeClock + MemoryTransport，毫秒级、不 flaky）

| 测试 | 断言 |
|---|---|
| `test_single_node_becomes_leader` | 单节点集群，advance 超时后 role==kLeader |
| `test_only_one_leader_per_term` | 3 节点跑 100 个随机 tick 序列，同 term 内 leader 数 ≤ 1 |
| `test_split_vote_resolved_by_next_timeout` | 两候选同时竞选（票分裂）→ 下一个超时窗口内选出唯一 Leader |
| `test_vote_rejected_when_log_stale` | 日志落后的候选人拿不到票 |
| `test_replication_all_logs_match` | propose 20 条后，3 节点日志逐条 (index,term) 相同 |
| `test_commit_requires_majority` | 只有 1 个 Follower ack 时 commitIndex 不前进；第 2 个 ack 后前进 |
| `test_old_term_entry_not_committed_by_count` | §5.4.2 反例场景：多数派复制了旧 term 条目，但不得提交 |
| `test_conflicting_suffix_truncated_on_leader_change` | 少数派上的脏后缀在收到新 Leader 的 AppendEntries 后被截断 |
| `test_lagging_follower_catches_up` | 停掉的 Follower 恢复后，日志追平 |
| `test_term_and_vote_persisted_before_reply` | 用 spy LogStore 断言"持久化调用发生在 RPC 返回之前" |
| `test_kv_apply_idempotent_on_retry` | 同 (clientId,requestId) apply 两次，值只变一次 |
| `test_propose_returns_not_leader_after_step_down` | Leader 降级后挂起的 propose 立即返回 kNotLeader |

### 7.2 崩溃与持久化测试（真实文件）

- `test_raft_restart.cpp`：写入 → 析构节点 → 用同一目录重建 → term/votedFor/lastIndex 与新状态机重放结果一致
- 手工追加垃圾字节到 `raft.log` 尾部 → 重建后仍是有效前缀（沿用 M1 已验证的 torn-tail 行为）

### 7.3 端到端与故障注入（真实时钟、真实进程）

```bash
# 3 节点生命周期 + kill -9 Leader
scripts/raft_e2e.sh
  1. 启动 3 节点（临时 data dir，端口 19601/19602/19603）
  2. 轮询 `raftkv_cli status` 直到出现 Leader（超时 5s）
  3. 写入 k=v，读回校验
  4. kill -9 Leader → 轮询新 Leader ≤ 2s → 写读继续成功
  5. kill 第 2 个节点（只剩 1 个）→ 写必须失败（NOT_LEADER 或 ERR），且不得返回 OK
  6. 重启一个节点 → 追赶后 3 个节点的 (lastIndex, commitIndex) 一致
  7. 打印 "e2e: PASS"

# 分区 + 稳定性
scripts/raft_fault.sh --repeat 50
  - SIGSTOP 一个 Follower（模拟被隔离）→ 写 100 条（多数派仍在，必须成功）
  - SIGCONT → 断言该节点追平
  - SIGSTOP 两个节点 → 写必须失败（不返回假成功）
  - 重复 N 次，统计失败次数必须为 0
```

> 诚实说明局限：`SIGSTOP` 不是真正的网络分区（TCP 连接仍在，写入会阻塞到超时）。真正的分区注入留给 M5（用 `iptables`/`tc`）。

---

## 8. 里程碑切分与验收（每个阶段都能独立跑通）

| 阶段 | 交付物 | 验收命令 / 断言 | 预估 |
|---|---|---|---|
| **M2.1** | `types.h`、`message`、`clock.h`、`MemoryLogStore`、`MemoryTransport`、单节点自选主 | `raftkv_tests --filter raft_election` 中 `test_single_node_becomes_leader` PASS | 3–4h |
| **M2.2** | 3 节点选主、心跳续任、任期规则、投票限制 | `test_only_one_leader_per_term`、`test_split_vote_resolved_by_next_timeout`、`test_vote_rejected_when_log_stale` PASS | 6–8h |
| **M2.3** | 日志复制、提交规则、`KvStateMachine`、`propose`、客户端 NOT_LEADER + 重试 | `test_replication_all_logs_match`、`test_commit_requires_majority`、`test_old_term_entry_not_committed_by_count`、`raft_e2e.sh` 前 3 步 PASS | 8–10h |
| **M2.4** | `FileLogStore`（meta + log + truncate）、重启恢复、kill -9 语义 | `test_raft_restart`、`test_term_and_vote_persisted_before_reply`、`raft_e2e.sh` 全部步骤 PASS | 6–8h |
| **M2.5** | 冲突快速回退（conflictIndex/conflictTerm）、故障注入脚本、稳定性打磨 | `test_conflicting_suffix_truncated_on_leader_change` + `raft_fault.sh --repeat 50` 全绿 | 4–6h |

**顺序上的取舍**：M2.1–M2.3 用内存 LogStore + FakeClock 先把**算法**跑对（确定性、秒级反馈），M2.4 再引入真实持久化与崩溃语义。这样每个阶段都有清晰的成功判据，避免"算法没对就陷进文件 IO 调试"。

---

## 9. 风险与对策

| 风险 | 后果 | 对策 |
|---|---|---|
| 持锁做网络 IO / fsync | 死锁、心跳卡顿、假超时 | §4.3 锁纪律 + 出站队列；评审时专门检查 |
| 投票/日志持久化顺序错误 | 同 term 双主、已确认日志丢失 | §5.4 表格 + `test_term_and_vote_persisted_before_reply`（spy 断言调用顺序） |
| 提交规则漏掉"当前 term"条件 | 可能提交将被覆盖的条目 → 数据丢失 | `test_old_term_entry_not_committed_by_count` 反例测试常驻 |
| 真实时钟导致测试 flaky | 测试不可信，回归难 | 单测强制 FakeClock；e2e 才用真实时钟；选举超时下限 150ms 留余量 |
| 一个客户端占一个 RPC worker | 慢客户端拖垮节点 | M2 接受并写进文档；M5 改 epoll/异步（已在 roadmap 记录） |
| `SIGSTOP` 不等于真分区 | 故障覆盖不足 | 明确标注为已知局限，M5 用 iptables/tc 补 |
| 日志无限增长 | 重启变慢（M2 会出现，不致命） | M3 快照解决，本阶段只需在 README 标注 |

---

## 10. 做完 M2 后可以讲的面试点

1. **为什么"过半"是安全的**：任意两个多数派必有交集 ⇒ 已提交条目必出现在新 Leader 上。
2. **为什么必须持久化 term/votedFor 再回包**：不持久化 → 重启后可能重复投票 → 同 term 双主。
3. **§5.4.2 那条容易被忽略的规则**：为什么"多数派复制了旧 term 条目"不足以提交。
4. **锁与 IO 的分离**：为什么"持锁发 RPC"是分布式系统里的经典死锁来源。
5. **测试如何避免 flaky**：把时间与传输做成 seam（FakeClock/MemoryTransport），故障注入才可重复。
6. **和 M1 的衔接**：持久化职责从"KV 自带 WAL"上移到"Raft log 是唯一真相来源"——一条设计演进线，比孤立讲两个模块好得多。

---

## 11. 待你拍板的开放问题

1. **阶段顺序**：按 M2.1→M2.5（先内存算法后真实持久化）是否同意？（我的建议：同意，反馈最快）
2. **节点间连接**：长连接 + 自动重连（推荐）还是每次 RPC 短连接？
3. **是否保留 M1 单机模式**：`main_server` 保留为 `--mode standalone`，还是直接删除只留 Raft 模式？（我倾向保留，方便对照压测）
4. **`status` 输出格式**：一行 `key=value`（脚本好解析）还是 JSON？（我倾向一行 `key=value`）
