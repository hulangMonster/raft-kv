# M4 设计文档：集群成员变更 + 客户端路由（线性一致读）

> 状态：**设计定稿 v1.0（已获用户原则批准，待评审书面确认）**
> 关联：[roadmap.md](roadmap.md)（M4 验收口径）· [m2-design.md](m2-design.md) · [m2-prerequisites.md](m2-prerequisites.md) ·
> [m3-design.md](m3-design.md)（v1.3 §12）· [m3-prerequisites.md](m3-prerequisites.md) · [protocol.md](protocol.md) · [code-review.md](code-review.md)
> 冻结方式：本文件由 #0 brainstorming 产出（决策 ①–⑧ 见 §3）。**批准后不得擅自偏离**；若 #1/#3 发现设计缺陷，回退 #0 修订并记录版本。
> 前置基线：M3 已完成并打 tag `m3-snapshot`（`b747c70`）；M4 开发自该提交之后延续。

---

## 1. 目标与非目标

### 1.1 目标（每条可验收）

1. **在线增删节点**：5 节点集群在压测期间可 `add`/`remove` 单个节点，全程无长时间（>数秒）服务不可用。
2. **客户端任意节点接入**：客户端可连任一节点；非 Leader 节点返回重定向，客户端按最新拓扑自动路由到 Leader。
3. **GET 线性一致读**：默认读路径为 ReadIndex（quorum 确认 + 等待 applied），保证不返回陈旧值。
4. **配置持久化与恢复**：配置随 `raft.log` 与快照共同持久化；重启后拓扑不回退，压缩后拓扑不丢失。
5. **删除节点收敛**：被移除节点不再参与投票/提交，不再被客户端路由到；其状态可查询但不影响集群。

### 1.2 非目标（明确推迟）

- M5 性能项（异步/per-peer transport、快照流式序列化、火焰图、分段日志、tick no-op 绕组提交、`main_raft_node` 线程池化）。
- 自动扩缩容、跨机房/多区域部署、完整 Learner 日志追赶优化（CatchUp 只做"追平后才能投票"的最小实现）。
- 完整 Joint Consensus 状态机（本次采用"一次一个 + 双重多数派"，见决策①）。
- 动态地址自动发现（地址由 `add` 命令显式给出，见决策⑦）。

---

## 2. 与 M1~M3 的关系

### 2.1 决策 D1 的延续：配置是日志的一部分

M2 决策 D1 规定"**Raft log 是唯一持久化真相源**"。M4 因此**不引入独立的配置存储**：

- 配置变更以**配置条目**形式写入 `raft.log`，与普通条目一起复制/提交；
- 快照必须携带当时的配置（否则 InstallSnapshot 后拓扑丢失）；
- **不新增 `config_store.h` / `meta.json` 配置存储**（与开工指令 §1.4 的候选文件清单的偏差已在 §11 记录理由：独立存储会产生第二处真相源，与 D1 冲突，且崩溃时更难保持一致）。

### 2.2 不变量

沿用 m2-prerequisites.md 的 **I1~I8** 与 m3-design.md 的 **"快照 ≤ lastApplied"、"先 durable 快照、后 compact"**，M4 新增：

- **J1**：同一时刻**至多一个成员变更在途**（`configChangeInFlight_` 非空即拒绝新的变更请求）。
- **J2**：配置条目自身的提交判定必须同时满足 **majority(C_old)** 与 **majority(C_new)**。
- **J3**：`ClusterConfig::version` 严格单调，且只能由日志推进（配置条目的 `index`）；任何更小版本的配置**必须被拒绝**，不得回退拓扑。
- **J4**：被移除的节点**不再参与投票、不计入任何多数派**，其 `RequestVote` 被忽略、其日志不一致不阻塞提交。
- **J5**：`C_old ∩ C_new ≠ ∅`（由 J1 的"一次一个"保证），因此过渡期不存在两个不相交多数派各自选出 Leader。

### 2.3 复用 / 扩展 / 刻意不做

| 类别 | 内容 |
|---|---|
| **复用（不改）** | RaftNode 选主与复制核心、§5.4.2 提交规则、Transport seam、Clock、MemoryTransport、LogStore seam、SnapshotStore seam、组提交 `syncedIndex_` 语义、InstallSnapshot 幂等续传、KvStateMachine 幂等去重 |
| **扩展（少量修改）** | `common.h`（+1 枚举值）、`types.h`（`RaftConfig` 新增成员变更开关）、`message.{h,cpp}`（新增 4 个 msgType）、`raft_node.{h,cpp}`（配置状态机/成员管理/ReadIndex/清理）、`transport.h`+`transport_tcp.{h,cpp}`（地址簿动态更新）、`memory_adapters.cpp`（测试用地址簿与探针投递）、`snapshot_store.h`+`file_snapshot_store.cpp`+`memory_snapshot_store.cpp`（快照携带配置，RKS1 v2）、`kv_state_machine.cpp`（kConfig 视为 no-op）、`main_raft_node.cpp` / `main_raft_client.cpp`、`CMakeLists.txt` |
| **新增** | `src/raft/cluster_config.{h,cpp}`、`tests/raft_membership_test.cpp`、`scripts/raft_membership_e2e.sh`、`scripts/raft_membership_fault.sh` |
| **禁止改动** | M1 除 `common.h` 一行外的全部源码与 `tests/`、`scripts/e2e.sh`；M2 选主/复制算法路径与 §5.4.2 规则；M3 `RKS1` **v1 解码路径**（必须继续兼容）；`src/store.{h,cpp}`、`main_server.cpp`、`main_client.cpp` |

---

## 3. 决策记录（①–⑧）

### 决策① 成员变更协议 —— 选定 **A：一次一个 + 新节点先追平**

- **A（选定）**：同一时刻只允许一个变更；新节点先以**非投票成员**身份追平，追平后才追加配置条目；变更条目的提交需双重多数派（§5.3）。
- B：完整 Joint Consensus（追加 `C_old,new`，双多数派提交后再追加 `C_new`）——最稳，但多一个中间配置状态、多一轮提交、快照与恢复都要理解三种配置，实现量约 3 倍。
- C：直接替换（追加 `C_new` 即生效，不要求旧多数派）——最简单，但存在"新旧多数派不相交"导致双主的窗口，**不可接受**。
- **理由**：A 用 `configChangeInFlight_` + J5 换取与 Joint Consensus 同等的安全性，代价可控；且 CatchUp 前置顺带解决"新节点拖慢可用性"。

### 决策② 配置表示与持久化 —— 选定 **A：配置条目进 raft.log**

- **A（选定）**：配置 = 日志中的配置条目（`OpCode::kConfig`），内存配置由"快照配置 → 日志中的配置条目依次应用"重建。
- B：独立 `meta.json`（tmp+rename+fsync）——与 D1 冲突，且与日志的提交顺序无法原子协调。
- C：双写（日志 + meta.json）——两处真相源，崩溃后可能不一致。
- **理由**：只有 A 天然获得"提交顺序 = 配置生效顺序"与"快照/日志组合恢复"两件事。

### 决策③ 生效时机 —— 选定 **A：追加即生效 + 双重多数派提交**

- **A（选定）**：节点在本地日志中看到**更新的配置条目**时立即切换 `ClusterConfig`（Raft §6 单节点变更语义）；而**该条目自身的提交判定**需要 majority(C_old) 且 majority(C_new)。二者作用域不同，见 §5.3。
- B：提交后才生效——变更窗口内可能出现"新旧多数派都不足"的停顿，且 Leader 崩溃后恢复路径更长。
- **理由**：A 与 B 的安全性由 J1+J5 共同保证；A 在可用性上更好，且实现上只需在"提交判定"里区分"普通条目/在途配置条目"。

### 决策④ 新节点加入流程 —— 选定 **A：CatchUp（非投票）→ 追平 → 变更**

- **A（选定）**：新节点以 `--id/--port/--peers(seed)` 启动，Leader 收到 `add` 后先把它登记为 **CatchUp 目标**（仅 Leader 本地 `pendingPeers_`，不进配置、不计多数派、无投票权），用既有复制/InstallSnapshot 机制追平；当 `matchIndex_[新] >= commitIndex_` 时追加配置条目。
- B：直接加入（先追加配置再追平）——变更窗口内多数派可能包含一个空节点，提交需要它的 ack，**可用性直接下降**。
- **理由**：A 把"追平的慢"与"成员变更的窗口"解耦；且 CatchUp 失败（新节点挂了）只需放弃，不产生半成品配置。

### 决策⑤ 快照与配置 —— 选定 **A：RKS1 v2 尾部追加配置段**

- **A（选定）**：`SnapshotData` 增加 `config` 字段；文件格式从 v1 升到 **v2**：在 payload 之后追加 `[configLen:4][configCrc:4][config]`；解码同时支持 v1（无配置 → 视为"快照未携带配置"，回退 §5.1 的 seed/日志规则）。
- B：把配置塞进 StateMachine payload——分层错误（配置是 Raft 层概念），且 KV SM 需要被注入配置。
- C：不改格式，靠日志中的配置条目恢复——配置条目一旦被 compact 掉，InstallSnapshot 后拓扑永久丢失。
- **理由**：A 是唯一同时满足"不改 v1 语义"与"安装快照后拓扑正确"的方案。
- **注意**：v1 快照只可能来自 M3 时代的数据目录；M4 首次快照即写 v2。

### 决策⑥ 线性一致读 —— 选定 **A：ReadIndex + 专用探针**

- **A（选定）**：Leader 记录 `readIndex = commitIndex_` → 向所有投票成员发送 `kReadProbe`（msgType 9）→ 收集到 **majority(C_current)** 个同任期确认后，等待 `lastApplied_ >= readIndex` → 读 SM。探针用**独立 msgType**，不改动 M2 的 `AppendEntries` 编码。
- B：读也走一次 no-op 提交——线性一致但每次读一次 fsync，读吞吐不可接受。
- C：Leader Lease（基于时钟）——依赖时钟漂移假设，虚拟机环境下不安全。
- **理由**：A 不碰 M2 消息格式（`AppendEntries` 保持字节不变），探针语义清晰、可单测；代价是多一次轻量 RPC。
- **语义变化**：`propose()` 的 GET 分支改为调用 `linearizableGet()`（对外协议零变更，见 §5.8）。

### 决策⑦ 客户端路由与拓扑发现 —— 选定 **A：种子 + 拓扑缓存 + 重定向兜底**

- **A（选定）**：`--peers` 退化为**种子地址簿**；客户端通过 `kConfigRequest(action=get)`（msgType 7）拉取 `{configVersion, members[{id, addr, voting}]}` 并缓存；命中 `kNotLeader` 或连接失败时失效重取；节点侧对**不在自己配置内**或非 Leader 的请求一律回 `leaderHint`。
- B：纯重定向（不缓存拓扑）——每次 Leader 切换/成员变更都多一跳，且客户端无法避免打到已移除节点。
- C：客户端直连配置中心/外部服务发现——M4 非目标（额外组件）。
- **理由**：A 复用既有 `leaderHint` 机制，新增一个只读消息即可；地址簿来源明确（`add` 命令携带 addr）。

### 决策⑧ 移除清理与锁纪律 —— 选定 **A：出站作业式清理 + L10/L11**

- **A（选定）**：配置生效时清理 `nextIndex_/matchIndex_/lastSentEndIndex_/snapshotSendOffset_/snapshotChunkEnd_/readAcks_`；地址簿更新（`addPeer/removePeer`）**不在锁内执行**，而是与出站消息一样"锁内收集作业、锁外执行"（新增 L10）；被移除节点进入**退役**状态（不再竞选、写请求回 `kNotLeader`、仍答 status/config）。
- B：锁内直接调 transport 增删——transport 的 `roundTrip` 持自身锁做阻塞 RPC（InstallSnapshot 超时 2s），会把 `RaftNode::mu_` 拖住数秒。
- **理由**：A 与既有 L2"锁内不做 IO"一致；L11 固定锁序 `RaftNode::mu_ → Transport::mu_`（仅在锁外作业里真正获取 Transport 锁，故此顺序不会嵌套）。

---

## 4. 接口与数据结构

### 4.1 ClusterConfig / Member（新增 `src/raft/cluster_config.h`）

```cpp
namespace raftkv::raft {

struct Member {
  int id = 0;
  std::string addr;      // "host:port"：用于更新 transport 地址簿与客户端路由
  bool voting = true;    // false = CatchUp 中的非投票成员（J4：不计多数派）
  bool operator==(const Member& o) const {
    return id == o.id && addr == o.addr && voting == o.voting;
  }
};

struct ClusterConfig {
  uint64_t version = 0;              // 配置条目 index；seed 配置为 0；严格单调（J3）
  std::vector<Member> members;       // 含 self；按 id 升序存放，便于比较与编码

  const Member* find(int id) const;
  bool isVoting(int id) const;
  size_t votingCount() const;
  size_t majority() const;           // votingCount()/2 + 1
  bool contains(int id) const;
  std::vector<int> votingIds() const;
  bool operator==(const ClusterConfig& o) const;
};

// 编解码（配置条目 payload 与快照 config 段共用同一格式）
Bytes encodeClusterConfig(const ClusterConfig& c);
bool decodeClusterConfig(const Byte* p, size_t n, ClusterConfig& out);

}  // namespace raftkv::raft
```

### 4.2 配置条目格式（复用 M2 `LogEntry` 落盘布局，不破坏旧日志）

配置条目 = 普通 `LogEntry`，只是 `op = OpCode::kConfig`：

```
LogEntry: index(8) term(8) op(1)=4 key(空) value(配置 payload) clientId=0 requestId=0
配置 payload（BE，自带版本号便于演进）:
  [cfgVer:1]=1 | [configVersion:8] | [count:4] | count x { [id:4] [voting:1] [addrLen:2] [addr...] }
```

- `kEntryFixedLen = 41` 与帧格式**保持不变**，旧记录（op=1/2/3）解析路径不变；
- 三处 op 白名单需追加 kConfig：`src/raft/message.cpp`（2 处）、`src/raft/file_log_store.cpp`（1 处）；
- `KvStateMachine::apply` 对 `kConfig` 只推进 `lastApplied_`（与 kGet 同样的 no-op 分支）；
- 配置条目的**语义处理在 RaftNode**（`advanceCommitAndApply` 拦截，不下发给 SM 数据路径）。

### 4.3 RaftNode 新增 API（`src/raft/raft_node.h`）

```cpp
// 配置查询
ClusterConfig clusterConfig() const;                  // 副本（mu_ 内拷贝）
uint64_t configVersion() const;
bool retired() const;                                 // 自身已被移除（退役态）

// 成员变更（仅 Leader 接受；其余返回 kNotLeader + leaderHint）
//   add: 先登记 CatchUp，追平后自动追加配置条目；remove: 直接追加
ClientReply changeMembership(MembershipOp op, int targetId,
                             const std::string& targetAddr, uint64_t timeoutMs);

// 线性一致读（ReadIndex，见 §5.8）
ClientReply linearizableGet(const std::string& key, uint64_t timeoutMs);

// 探针回包入口（由 main_raft_node 的 handleConnection 分发）
ReadProbeReply onReadProbe(const ReadProbeArgs& args);
void onReadProbeReply(int peerId, const ReadProbeReply& reply);
```

新增成员状态（全部在 `mu_` 下）：

```cpp
ClusterConfig currConfig_;                 // 当前配置（含 self）
ClusterConfig prevConfig_;                 // 在途配置条目的 C_old（无在途时 == currConfig_）
Index inFlightConfigIndex_ = kNoIndex;     // 在途配置条目（J1）
bool retired_ = false;                     // 退役态
std::unordered_map<int, std::string> pendingPeers_;   // CatchUp 目标（不进配置、不计多数派）
std::unordered_map<int, uint64_t> readAcks_;          // ReadIndex 探针：peer -> seq
uint64_t readSeq_ = 0;                                 // 读请求序号（单调）
```

### 4.4 Transport 扩展（`src/raft/transport.h`，默认空实现保证 Memory 适配器零改动即可编译）

```cpp
virtual void addPeer(int id, const std::string& addr) { (void)id; (void)addr; }
virtual void removePeer(int id) { (void)id; }
```

`TcpTransport` 覆写：持自身 `mu_` 更新 `peers_` 并在 remove 时 `dropConnection`。
**锁纪律 L10**：这两个调用**只在锁外作业中执行**（§6）。

### 4.5 消息扩展（`src/raft/message.{h,cpp}`，复用 M2 长度前缀帧）

| msgType | 名称 | 方向 | 载荷 |
|---|---|---|---|
| **7** | `kConfigRequest` | 客户端 → 节点 | `[action:1][targetId:4][addrLen:2][addr]`（action: 0=get, 1=add, 2=remove） |
| **8** | `kConfigReply` | 节点 → 客户端 | `[term:8][ok:1][leaderHint:4][configVersion:8][count:4] count x {[id:4][voting:1][addrLen:2][addr]}` |
| **9** | `kReadProbe` | Leader → 节点 | `[term:8][leaderId:4][seq:8]` |
| **14** | `kReadProbeReply` | 节点 → Leader | `[term:8][ok:1][seq:8]` |

编号依据：1–6 为 Raft RPC 对，10=`kClientRequest`、11=`kClientReply`、12=`kStatusRequest`、13=`kSnapshotTrigger` 已占用，故新对取 7/8 与 9/14。
**M2 的 `AppendEntries`/`InstallSnapshot` 编码字节不变**（决策⑥）。

### 4.6 快照格式 RKS1 v2 与 `SnapshotData` 扩展

```
v1（M3，必须继续可解）:
  magic(4)="RKS1" | ver(1)=1 | lastIncludedIndex(8) | lastIncludedTerm(8)
  | payloadLen(4) | crc32(payload)(4) | payload[payloadLen]

v2（M4）: 在 v1 之后追加
  | configLen(4) | crc32(config)(4) | config[configLen]

解码规则:
  ver==1 -> 要求 n == 29 + payloadLen，config 置空（视为“未携带配置”）
  ver==2 -> 要求 n == 29 + payloadLen + 8 + configLen，并校验 configCrc
  其它 ver -> 丢弃（沿用 M3 torn/不支持即丢弃的既有语义）
```

`SnapshotData` 增加 `Bytes config;`（`snapshot_store.h`）；两个 store 只负责透传/编解码，不理解语义。

### 4.7 客户端与节点命令

- 节点 `main_raft_node`：新增 `--peers` 语义说明（**种子**）、分发 7/8/9/14；`status` 追加
  `config_version=`、`members=`（`1:host:port:v,2:...`）、`read_index=`、`retired=`。
- 客户端 `main_raft_client`：新增 `config`、`add <id> <host:port>`、`remove <id>`；
  拓扑缓存 + 失效重取 + 重定向兜底；`fill/verify` 等既有命令语义不变。

---

## 5. 算法规则

### 5.1 启动配置解析顺序（冻结）

1. `seed` = `--peers` 解析结果，`version = 0`；
2. 若 `SnapshotStore::load()` 成功且 **快照携带配置** → 用其配置（`version = 快照内的 configVersion`）；
3. `log_.load()` 后**重放日志中的配置条目**（按 index 升序），逐条覆盖内存配置；
4. 校验单调性（J3）：任何版本**小于**当前版本的配置一律拒绝并记日志；启动时若发现日志中的配置版本小于快照配置版本 → **拒绝启动**（拓扑回退必须显式暴露，禁止静默降级）；
5. 自身不在配置中 → 以**退役态**启动（只读 status/config）。

### 5.2 配置条目的生命周期

| 阶段 | 规则 |
|---|---|
| **追加** | 仅 Leader；`configChangeInFlight_` 为空时才允许（J1）；`changeMembership()` 内部走既有 `propose` 路径（`op=kConfig`，`clientId=0/requestId=0`），复用组提交与 `syncedIndex_` 门控 |
| **复制** | 与普通条目一致；peer 集合 = `currConfig_.members ∪ pendingPeers_`（后者用于 CatchUp） |
| **提交** | 普通条目：majority(`currConfig_`)；**在途配置条目**：majority(`prevConfig_`) 且 majority(`currConfig_`)（J2） |
| **生效** | 节点在本地日志中首次看到 index 更大的配置条目即切换 `currConfig_`（旧配置存入 `prevConfig_`）；提交后清空 `prevConfig_` 并置 `inFlightConfigIndex_ = kNoIndex` |
| **回滚** | 配置条目若因冲突被 `truncateSuffix` 截断 → 恢复为被截断前的配置（即重新按 §5.1 步骤 2–3 由**快照配置 + 剩余日志**重算），并把 `prevConfig_` 复位；绝不保留"日志里已不存在的配置" |

### 5.3 多数派计算（唯一入口）

```cpp
// 所有需要“多数派”的地方都必须经过这两个函数，禁止散落计算
size_t RaftNode::majorityOf(const ClusterConfig& c) const { return c.majority(); }
bool RaftNode::hasMajority(const ClusterConfig& c,
                           const std::function<bool(int)>& acked) const;
```

- **提交普通条目**：`hasMajority(currConfig_, matchIndex_ >= n)`；
- **提交在途配置条目**：`hasMajority(prevConfig_, …) && hasMajority(currConfig_, …)`；
- **选举**：候选人按**自己本地已知配置**的投票成员计票；跨节点一致性由 J1+J5 保证；
- **ReadIndex 探针**：`hasMajority(currConfig_, readAcks_ == seq)`（变更在途时同 J2 的双重判定）；
- **非投票成员（CatchUp 目标 / `voting=false`）**：既不计入多数派分母，也不作为被计票对象（J4）。

### 5.4 加入流程（`add <id> <addr>`）

1. 客户端 → 任一节点；非 Leader 回 `kConfigReply{ok=false, leaderHint}`，客户端重定向；
2. Leader 校验：`id` 不在配置中、地址非空、无在途变更、`id != selfId`（自我加入无意义）；
3. 登记 `pendingPeers_[id] = addr` + **锁外** `transport_.addPeer(id, addr)`；`nextIndex_[id] = lastIndex()+1`，`matchIndex_[id] = kNoIndex`；
4. CatchUp：走既有 AppendEntries / InstallSnapshot（落后超过边界时自动转快照）；
5. 追平判据（冻结）：`matchIndex_[id] >= commitIndex_` **且**该节点已应答至少一次当前任期的 AppendEntries；
6. 满足后构造 `C_new = currConfig_ + Member{id, addr, voting=true}`，`version = ` 即将追加的条目 index，追加配置条目；
7. 超时（`catchUpTimeoutMs`，默认 30s）或变更被拒 → 清 `pendingPeers_[id]` + 锁外 `transport_.removePeer(id)`，回 `ok=false`（不产生半成品配置）。

### 5.5 移除流程与清理（`remove <id>`）

1. 校验：`id` 在配置中、无在途变更、`id != selfId`（移除自身见 §5.7）；
2. 追加配置条目 `C_new = currConfig_ - id`；
3. **生效时**（本地看到该条目）即把 `id` 从 `currConfig_` 移除，并清理：`nextIndex_/matchIndex_/lastSentEndIndex_/snapshotSendOffset_/snapshotChunkEnd_/readAcks_/pendingPeers_`；
4. **锁外**执行 `transport_.removePeer(id)`（L10）；
5. 提交判定按 J2（旧配置多数派 + 新配置多数派）。

### 5.6 被移除节点的退役行为

- 收到任何携带配置的消息（配置条目或 InstallSnapshot 的配置段）且自身不在其中 → `retired_ = true`；
- 停止竞选（`tick` 不再进入 `startElection`）、不再计入任何多数派、不再接受写请求（`propose` 回 `kNotLeader` + `leaderHint`）；
- 仍响应 `kStatusRequest` 与 `kConfigRequest(get)`，便于运维观察；
- **不自杀、不删数据**（保留现场，符合"崩溃/故障可诊断"的工程要求）。

### 5.7 Leader 自我移除

- `remove <leaderId>` 由**其他节点**或客户端发起；Leader 追加该条目后**继续服务**直到该条目**提交并生效**，随后主动 `becomeFollower` 并进入退役态；
- 若客户端直接对 Leader 请求 `remove self`：允许，但回复在条目提交后才返回（复用 propose 的超时语义），回复中带 `leaderHint=-1` 提示自行重新发现拓扑。

### 5.8 ReadIndex 线性一致读

```
linearizableGet(key, timeoutMs):
  锁内：非 Leader 或 retired_ -> kNotLeader + leaderHint
        readIndex = commitIndex_; seq = ++readSeq_
        readAcks_ 清空；为 currConfig_ 的全部投票成员（含 self 立即计入）构造 kReadProbe(term, selfId, seq)
  解锁：发送探针
  恢复：收到 majority 个 ok=true 且 term 相同的回包后
        锁内：仍为同任期 Leader 且 seq 仍有效 -> 继续；否则 kNotLeader
        等待 lastApplied_ >= readIndex（cv_，谓词含降级/退役/超时）
        锁内读 sm_.get(key) -> kOk / kNotFound
  超时：kErr("read index timeout")
```

- ``kReadProbe` 接收侧：`args.term < currentTerm_` → `ok=false`；否则 `becomeFollower(args.term)`（承认其领导权）+ 重置选举计时 + `ok=true`；
- 与变更的关系：探针多数派用 §5.3 的规则（在途变更时双重多数派）；
- `propose()` 的 GET 分支改为调用本函数（D-2），写路径完全不变。

---

## 6. 线程安全与锁纪律

沿用 M2 的 L1–L7 与 M3 的 L8/L9，新增：

- **L10（地址簿与成员清理出锁执行）**：`Transport::addPeer/removePeer`、被移除节点的连接清理等一律**锁外作业**执行；锁内只收集/登记。
- **L11（锁序）**：`RaftNode::mu_` → `Transport::mu_` → `LogStore::mu_`；由于 L10，Transport 锁只在锁外获取，**任何路径都不得在持有 Transport 锁时回调 RaftNode**（既有 `roundTrip` 已满足：回调在外层、锁内不做 cb）。
- **条件变量谓词**：`cv_` 的等待谓词必须包含 `retired_` 与配置代际（`configVersion` 或 `inFlightConfigIndex_`），避免节点退役/变更后请求永久阻塞。
- **配置对象生命周期**：`clusterConfig()` 返回**值拷贝**；内部一律持锁读，禁止把 `currConfig_` 的引用/指针带出锁外。

---

## 7. 风险与对策

| # | 风险 | 对策 |
|---|---|---|
| R1 | 变更期间多数派用错配置集 → 双主/丢提交 | §5.3 单一入口 `hasMajority`；在途变更双重判定；单测构造 C_old 多数派缺失场景 |
| R2 | 配置条目与快照不一致 → 重启后拓扑回退 | J3 单调校验 + 启动第 4 步"版本回退即拒绝启动"；B 组组合恢复用例 |
| R3 | 新节点未追平即计票 → 可用性下降 | 决策④ CatchUp 判据；未追平节点 `voting=false` 且不进配置 |
| R4 | 移除节点仍在竞选搅局 | §5.6 退役态：不再 `startElection`；他人忽略其 `RequestVote`（J4） |
| R5 | 配置条目被 compact 后边界语义破坏 | 配置随快照携带；`termAt`/`slice` 的 D3 语义不变；B 组"compact 后恢复"用例 |
| R6 | ReadIndex 与 Leader 切换竞态读到旧值 | 探针必须同任期 + majority + 等 `lastApplied_ >= readIndex`；`seq` 校验丢弃过期回包 |
| R7 | 客户端拓扑缓存过期 → 打到已移除节点 | 缓存带 `configVersion`；被移除/非 Leader 节点一律回 `leaderHint`；连接失败即失效重取 |
| R8 | 变更中途崩溃 | 配置条目走日志（原子）；未提交的变更条目被截断即丢失（安全）；`pendingPeers_` 是 Leader 内存态，重启后由运维重试 |
| R9 | 锁内做地址簿/IO → 阻塞共识 | L10/L11 + 锁内只收集作业 |
| R10 | `RKS1` v2 与 v1 不兼容 | 解码按版本分支；v1 视为"未携带配置"，回退 seed/日志 |

---

## 8. 测试矩阵

### 8.1 A 组（确定性：FakeClock + MemoryTransport + MemoryLogStore + MemorySnapshotStore，零 flaky）

| # | 用例 | 断言要点 |
|---|---|---|
| A1 | `ConfigSerdeRoundTrip` | 编解码往返一致；截断/CRC 错误被拒 |
| A2 | `ConfigEntryReplicatedToAllPeers` | 3 节点追加配置条目后三者 `clusterConfig()` 相同 |
| A3 | `BootstrapSeedBecomesInitialConfig` | `--peers` → version=0 配置；self 在配置内 |
| A4 | `AddRequiresCatchUpBeforeVoting` | 未追平的新节点不计入多数派、不被计票 |
| A5 | `AddCommitsAndTakesEffect` | 追平后 add 成功；多数派按 C_new 计算 |
| A6 | `RemoveTakesEffectAndStopsCounting` | 移除后旧节点不再被计票，且不阻塞提交 |
| A7 | `SecondChangeRejectedWhileInFlight` | 在途变更期间第二个变更被拒（J1） |
| A8 | `ConfigCommitNeedsBothMajorities` | 构造 C_old 多数派缺失 → 配置条目不提交 |
| A9 | `ElectionRespectsNewConfig` | 变更后旧节点无法当选；退役节点不竞选 |
| A10 | `RemovedNodeRetiresAndRedirects` | 被移除节点：不竞选、写请求回 `kNotLeader`、仍答 status |
| A11 | `ConfigVersionMonotonicRejectsStale` | 更小版本的配置被拒（J3） |
| A12 | `SnapshotCarriesConfigAndRestartKeepsTopology` | 快照携带配置；重启后拓扑不丢 |
| A13 | `ReadIndexLinearizableAcrossLeaderChange` | 旧 Leader 降级后读不返回陈旧值 |
| A14 | `ReadIndexRequiresQuorum` | 少数派 Leader 读超时失败 |
| A15 | `SupraRulesHoldAcrossMembershipChange` | §5.4.2 反例在成员变更后仍成立 |
| A16 | `LeaderSelfRemovalStepsDownAfterCommit` | Leader 自我移除：提交生效后才下台（§5.7） |

### 8.2 B 组（`FileLogStore` + `FileSnapshotStore` + 真实临时目录）

| # | 用例 | 断言要点 |
|---|---|---|
| B1 | `ConfigPersistsAcrossRestart` | 重启后配置与版本号一致 |
| B2 | `ConfigCompactedThenRecoveredFromSnapshot` | 配置条目被 compact 后仍能从快照恢复拓扑 |
| B3 | `CrashDuringConfigChangeKeepsConsistentTopology` | 变更中途 kill -9：重启后配置要么是 C_old 要么是 C_new，不存在第三种 |
| B4 | `RKS1V1SnapshotStillLoads` | M3 的 v1 快照文件仍能解码（config 为空） |

### 8.3 脚本验收

- `scripts/raft_membership_e2e.sh`：3 节点启动 → 压测（`fill` + `verify`）→ `add` 第 4/5 节点 → 继续压测（**无长时间失败**）→ `remove` 1 个节点 → 校验拓扑（`config`/status）与线性一致读（`get` 必须命中已提交值）；
- `scripts/raft_membership_fault.sh --repeat 50`：变更窗口内注入 `kill -9` / `SIGSTOP`，每轮结束后校验：拓扑一致（所有存活节点 `config_version` 相同或单调推进）、压测可继续、无脑裂读。

---

## 9. 里程碑拆分（M4.1 → M4.5）

| 阶段 | 内容 | 通过判据（必须附实测输出） |
|---|---|---|
| **M4.1** | `ClusterConfig`/`Member` + 配置条目编解码/持久化/恢复 + `kConfigRequest/Reply(get)` + `status` 扩展 | A1–A3、B1 |
| **M4.2** | `add`/`remove`（CatchUp + 双重多数派 + 生效 + 清理 + 退役） | A4–A11、A16 |
| **M4.3** | 快照携带配置（RKS1 v2）+ 恢复顺序 + compact 后拓扑 | A12、B2–B4 |
| **M4.4** | `kReadProbe/Reply` + ReadIndex + 客户端拓扑缓存与路由 | A13–A15 |
| **M4.5** | 5 节点在线增删 + 故障注入 + 观测字段 + 稳定性 | `raft_membership_e2e.sh`、`raft_membership_fault.sh --repeat 50` 全通 |

---

## 10. 文件清单

**新增**
- `src/raft/cluster_config.h/.cpp`（数据结构、编解码、多数派计算）
- `tests/raft_membership_test.cpp`
- `scripts/raft_membership_e2e.sh`、`scripts/raft_membership_fault.sh`

**最小修改（必须改，逐项列出理由）**
- `src/common.h`：`OpCode` 追加 `kConfig = 4`（**M1 唯一改动**，纯新增枚举值）
- `src/raft/types.h`：`RaftConfig` 追加 `catchUpTimeoutMs`（默认 30000）、`readIndexTimeoutMs`（默认 500）
- `src/raft/message.{h,cpp}`：新增 msgType 7/8/9/14 编解码；op 白名单追加 `kConfig`
- `src/raft/raft_node.{h,cpp}`：配置状态、`changeMembership`、`linearizableGet`、探针处理、多数派单一入口、清理与退役
- `src/raft/transport.h`、`transport_tcp.{h,cpp}`：`addPeer/removePeer`
- `src/raft/memory_adapters.cpp`：`MemoryTransport` 的 `addPeer/removePeer`（维护 `nodes_`/`isolated_`）
- `src/raft/snapshot_store.h`、`file_snapshot_store.cpp`、`memory_snapshot_store.cpp`：`SnapshotData::config` + RKS1 v2
- `src/kv/kv_state_machine.cpp`：`kConfig` 视为 no-op（只推进 `lastApplied_`）
- `src/main_raft_node.cpp`：`--peers`=种子、分发 7/8/9/14、`status` 扩展
- `src/main_raft_client.cpp`：`config`/`add`/`remove` 子命令 + 拓扑缓存 + 重定向兜底
- `CMakeLists.txt`：`raftkv_raft` 追加 `cluster_config.cpp`；`raftkv_raft_tests` 追加 `tests/raft_membership_test.cpp`

**禁止改动**
- M1：除 `common.h` 一行外的全部源码、`tests/*`、`scripts/e2e.sh`、`main_server.cpp`、`main_client.cpp`
- M2：选主/复制算法路径、§5.4.2 提交规则、既有消息编码字节布局
- M3：`RKS1` v1 解码路径、D2/D3 语义、快照五步恢复顺序（只在尾部追加 config 段）

---

## 11. 自检记录（占位符 / 矛盾 / 歧义 / 范围）

1. **占位符扫描**：无 `TBD/TODO/待定`；msgType 编号（7/8/9/14）、配置 payload 字段与宽度、RKS1 v2 布局、超时默认值（30s/500ms）、多数派规则均为具体值。
2. **矛盾消解**：
   - 决策③"追加即生效"与 J2"提交需双重多数派"作用域不同（生效=使用哪套多数派；提交=条目自身的提交判定）——§5.2/§5.3 已分别定义，不矛盾；
   - 决策⑤ RKS1 v2 与"M3 格式冻结"：**只增不改**，v1 解码路径必须保留（§4.6、B4 用例）；
   - 开工指令 §1.4 列出 `config_store.h` 为候选新增文件，本设计**不新增**（§2.1 理由），已在 §2.3 与本节记录，属批准范围内的有意偏差。
3. **歧义消除**：CatchUp 追平判据精确到"matchIndex ≥ commitIndex 且已应答当前任期 AppendEntries"；退役态行为逐条列出；Leader 自我移除的收尾时序明确；客户端拓扑缓存失效条件明确。
4. **范围检查**：单一子系统（成员管理 + 读路径），M4.1–M4.5 每步可独立测试与提交；完整 Joint Consensus、自动扩缩容、服务发现已明确划入非目标。
5. **接口稳定性**：`Transport` 新增方法带默认实现 → `MemoryTransport`/测试桩零改动即可编译；`SnapshotData` 增字段为纯追加；`propose()` 对外行为不变（GET 语义变严格）。
6. **可验证性**：每条目标在 §8 有对应用例；每条风险在 §7 有对策与（R2/R4/R6/R8）专测。

---

## 12. 修订记录

- **v1.0**（#0 brainstorming）：首次定稿。决策①–⑧ 全部选定；新增不变量 J1–J5；锁纪律新增 L10/L11；快照格式升级 RKS1 v2（兼容 v1）。
