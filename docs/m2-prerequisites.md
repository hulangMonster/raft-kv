# M2 前置校验（Phase #1）

> 阶段：#1 / verification-before-completion（不写业务实现代码）
> 依据：`docs/m2-design.md`、`docs/roadmap.md`、M1 现有代码
> 环境验证时间戳与证据见 §6

---

## 1. M1 与 M2 边界不变量（复用 / 扩展 / 重构）

### 1.1 组件处置总表

| M1 组件 | 处置 | 说明 |
|---|---|---|
| `common.h` | **复用（禁止改）** | `Bytes/Byte`、`putU16/getU16/putU32/getU32`、`OpCode`、`StatusCode`、`logInfo/logError` 全部被 raft 模块 include |
| `thread_pool.h` | **复用（禁止改）** | `main_raft_node` 的 RPC worker 池、出站 RPC 池 |
| `codec.{h,cpp}` | **不修改** | M2 新增 `src/raft/message.{h,cpp}` 实现 RPC 帧，复用 common.h 原语与长度前缀**思想**；不改 M1 客户端 KV 协议 |
| `wal.{h,cpp}` | **不修改** | M2 集群不再用它；其 CRC32 + torn-tail 思路由 `FileLogStore` 重新实现 |
| `server.{h,cpp}` | **不修改** | 仅 M1 standalone 入口继续使用 |
| `store.{h,cpp}` | **见 §1.2 决策 D1'** | 默认方案：不修改；新增 `src/kv/kv_state_machine.{h,cpp}` |
| `main_server.cpp` | **极小改动（可选）** | 接受 `--mode standalone`（默认值，其余值报错并指向 `main_raft_node`） |
| `main_client.cpp` | **必须改** | 新增 NOT_LEADER 处理、leaderHint 重定向与重试、`status` 命令、交互模式 REPL |
| `CMakeLists.txt` | **必须改** | 新增 raft 库 / `main_raft_node` / raft 单测目标；**保留所有 M1 目标不变** |
| M1 `tests/*`、`scripts/e2e.sh` | **禁止改** | M1 测试与冒烟保持原样，新增 `tests/raft_*.cpp` 与 `scripts/raft_*.sh` |

### 1.2 决策 D1 的落地（重要，有一处需要你确认）

设计文档 D1 原话："KV 不再自带 WAL，store 降级为纯内存 StateMachine"。

**意图**：在 M2 集群里，**持久化唯一真相来源是 Raft LogStore**，KV 状态由"已提交条目按序 apply"重建，不再有第二份 KV WAL。

**落地冲突**：字面执行"剥离 store 的 WAL"会破坏 M1 standalone（单机模式需要 WAL 持久化），与"保留 M1 单机入口、最小改动 M1"矛盾。

**推荐方案（D1'）**：不改 `store.{h,cpp}`；M2 新增
- `src/raft/state_machine.h`：`StateMachine` 虚接口（`apply/get/lastApplied`）
- `src/kv/kv_state_machine.{h,cpp}`：纯内存 KV + 幂等去重表，实现 `StateMachine`

M2 集群完全不引用 M1 `Store`；M1 standalone 继续用 `Store+Wal` 原样工作。D1 的**意图**（Raft log 为唯一真相源）在集群内成立，且真正实现"M1 零改动"。

> 若你坚持按设计文档字面改 `store.{h,cpp}`，见 §7 的 B 方案。

### 1.3 边界不变量（M2 代码必须维持）

- I1：日志下标从 **1** 开始，`kNoIndex=0` 表示"空/不存在"；禁止访问 `log[0]` 作为真实条目。
- I2：`term` 单调不减；任何带更高 term 的合法 RPC 使节点降级为 Follower 并更新 term。
- I3：`votedFor` 在一个 term 内至多投一票（可为空）。
- I4：`commitIndex` 与 `lastApplied` 单调不减；只能 apply `(lastApplied, commitIndex]`。
- I5：回复 RequestVote 之前已持久化 `term/votedFor`；回复 AppendEntries success 之前已持久化该批 entries。
- I6：只有 Leader 接受客户端写；Follower 回 `kNotLeader + leaderHint`。
- I7：KV apply 幂等（`clientId/requestId` 去重）。
- I8：`RaftNode` 不直接依赖 socket / 文件系统（依赖注入 `Transport/LogStore/Clock`）。

---

## 2. 线程安全契约与锁纪律

| 规则 | 内容 |
|---|---|
| **L1 单锁** | `RaftNode` 内部唯一 `std::mutex mu_`；role/term/votedFor/log/commitIndex/lastApplied/leaderId/nextIndex/matchIndex 的读写**全部在锁内** |
| **L2 锁内禁阻塞** | 持 `mu_` 期间**禁止**：fsync、socket send/recv、`LogStore` 的任何磁盘方法、`Transport::send*` 的真实网络调用 |
| **L3 两段式持久化** | 写日志 = 锁内改内存日志 → 解锁 → `LogStore.append()`（内部自有锁串行，可能 fsync）→ 回锁确认；**确认持久化完成前不回复 RPC、不推进提交**。磁盘顺序由 LogStore 内部互斥量保证 |
| **L4 出站队列** | Leader 复制/心跳：锁内只构造 `AppendEntriesArgs` 入**出站队列**并 `notify`；锁外由出站线程池消费并真正 send；回调 `onXxxReply` 再进锁 |
| **L5 propose 等待** | Leader 维护 `requestId → 等待上下文`；提交 apply / 降级 / 超时 三者之一发生时 `notify`；**等待过程不持锁** |
| **L6 ticker** | 单独线程每 10ms 调 `RaftNode::tick()`（锁内）；单测直接手动调 `tick()`，不依赖真实线程 |
| **L7 生命周期** | 出站队列/等待表在节点析构前必须排空；`propose` 返回后无悬挂回调 |

**单测前置假设**：M2.1–M2.3 用 `MemoryLogStore`（无 IO），L2/L3 天然满足但**仍按两段式写**，保证 M2.4 换成 `FileLogStore` 时行为一致。

---

## 3. 风险清单

| 风险 | 后果 | 缓解 | 对应测试 |
|---|---|---|---|
| 同 term 双主 | 数据分叉 | 回复投票前持久化 term/votedFor | `test_term_and_vote_persisted_before_reply` + kill -9 e2e |
| 持久化顺序错误 | 已确认日志丢失 | I5 两段式 + spy LogStore 断言顺序 | 同上 |
| §5.4.2 提交规则遗漏 | 提交将被覆盖的旧 term 条目 | 提交条件显式含 `log[N].term==currentTerm` | `test_old_term_entry_not_committed_by_count` |
| propose 阻塞挂死 | 客户端卡死 | cv 谓词三条件（提交/降级/超时）；降级立即返回 | `test_propose_returns_not_leader_after_step_down` |
| 幂等失效 | 重试重复 apply | `clientId+requestId` 去重表 | `test_kv_apply_idempotent_on_retry` |
| 持锁 IO/fsync 死锁 | 心跳卡顿、假超时 | L2/L4 纪律 + 评审专查 | ——（review gate） |
| torn tail 损坏 | 启动崩 | CRC 校验 + 截断（沿用 M1 已验证思路） | `test_raft_restart` + B 组 |
| truncateSuffix 偏移错误 | 截错日志 | 启动重建 `index→offset` 映射 | `test_conflicting_suffix_truncated_on_leader_change` |
| leaderHint 循环/过期 | 客户端打转 | 重试上限 3 次、总预算 1s | e2e |
| 时钟 flaky | 测试不可信 | 单测 FakeClock；e2e 才用真实时钟；超时下限 150ms | —— |
| SIGSTOP ≠ 真分区 | 覆盖不足 | 文档标注局限，M5 用 iptables/tc | —— |

---

## 4. M1 存量代码修改点清单

### 【必须改】
1. `CMakeLists.txt` — 新增 `raftkv_raft` 静态库、`main_raft_node`、`raftkv_raft_tests`；`FetchContent` 引入 googletest（仅测试依赖）；**M1 目标 `raftkv_server/raftkv_cli/raftkv_tests` 全部保留**。
2. `src/main_client.cpp` — NOT_LEADER 处理 + leaderHint 重定向（≤3 次/1s）+ 请求幂等 id（clientId/requestId）+ `status` 命令 + 交互 REPL。
3. `src/main_server.cpp` — 接受 `--mode standalone`（默认；仅接受 standalone，其余值提示使用 `main_raft_node`）。

### 【可选扩展】（默认不做）
- `codec.{h,cpp}` 统一 KV 协议与 RPC 帧 —— 默认**不做**，用 `src/raft/message` 独立实现，真正零改 M1。
- `thread_pool.h` 无锁队列化 —— M5 再做。

### 【禁止改动】
- `src/common.h`、`src/codec.{h,cpp}`、`src/thread_pool.h`、`src/wal.{h,cpp}`、`src/server.{h,cpp}`
- `tests/test.h`、`tests/test_*.cpp`（M1 测试）、`scripts/e2e.sh`（M1 冒烟）
- `docs/protocol.md`、`.gitignore`、README 中 M1 章节

---

## 5. 边界 case 全集、未定义行为、测试前置假设

### 5.1 选举
- 空日志候选：`lastLogIndex=0,lastLogTerm=kNoTerm` 的投票比较
- 投票授予的日志新旧比较边界：`lastLogTerm 大者新`；同 term 时 `lastLogIndex 大者新`
- 同 term 收到重复投票请求 → 幂等返回 `votedFor==candidate`
- split vote → 下一超时窗口重选
- 收到过期 term 的 RPC → 拒绝并回带 `currentTerm`

### 5.2 复制
- `prevLogIndex==0`（首次复制）→ 不校验 prevLogTerm，直接 append
- `prevLogIndex > lastIndex` → `success=false, conflictIndex=lastIndex+1`
- `termAt(prevLogIndex)!=prevLogTerm` → 失败（M2.5 带 conflictIndex/conflictTerm 快速回退）
- 同 index 同 term → 跳过；同 index 异 term → 截断后 append
- `leaderCommit` → `commitIndex=min(leaderCommit,lastIndex)`
- Leader 首轮 `nextIndex[i]=lastIndex+1, matchIndex[i]=0`

### 5.3 提交与 apply
- 空日志 `commitIndex=0`；apply 只前进
- 提交规则同时要求"多数派 matchIndex>=N"且"log[N].term==currentTerm"

### 5.4 客户端
- NOT_LEADER 的 `leaderHint=-1`（未知）→ 客户端顺序探测 / 报错
- 重试同一 `(clientId,requestId)` → 服务端不重复应用
- `requestId <= lastRequestId` → 丢弃（含相同 id 的重复与乱序回退）

### 5.5 FileLogStore
- torn tail（CRC 失败/长度不足）→ 截断到最后完整记录
- `meta.dat` 原子写：先写临时文件再 `rename`
- `truncateSuffix` 到某 index 对应 offset；空文件；首条记录即损坏
- append 返回 false（IO 错误）→ 上层不推进 commit、不回复 success

### 5.6 未定义行为 / 禁止
- 禁止访问 `log[0]`；`termAt` 越界返回 `kNoTerm`，调用者必须先判 `prevLogIndex<=lastIndex`
- 禁止在 `mu_` 锁内调用任何磁盘/网络方法（违反 L2 即 UB/死锁）
- 禁止 `workers=0`、非法端口（沿用 M1 抛 `std::runtime_error`）

### 5.7 测试前置假设
- A 组单测：FakeClock + MemoryTransport + MemoryLogStore，无网络无磁盘，手调 `tick()`，毫秒级、零 flaky
- B 组单测：真实临时目录 + FileLogStore，验证崩溃重启与 torn-tail
- GTest 经 CMake `FetchContent` 获取（test-only，不进入运行时/核心库）
- 节点 id 从 1 连续；单测直接构造 3 节点并互相注入 MemoryTransport 回调

---

## 6. 环境验证证据（Phase #1 实测）

| 项 | 证据 |
|---|---|
| M1 构建 | `cmake --build build -j4` exit 0，目标全 Build |
| M1 单测 | `./build/bin/raftkv_tests` → `all tests passed (13)` |
| M1 e2e | `./scripts/e2e.sh` → `e2e: PASS` |
| GTest | 未安装；无 sudo 免密；**VM 有外网**（`git ls-remote github.com/google/googletest` 成功）→ 采用 FetchContent |
| CMake | 3.22.1（支持 `FetchContent_MakeAvailable`） |
| git / CPU | git 2.34.1 / 8 核 |

---

## 7. 已确认决策（用户拍板）

1. **store 处置 → D1′**：不改 M1 `store.{h,cpp}`；M2 新增 `src/raft/state_machine.h`（接口）+ `src/kv/kv_state_machine.{h,cpp}`（纯内存 + 幂等）。M1 standalone 原样保留。
2. **GTest 引入 → 用本机已有 GTest**：`/usr/local/include/gtest/gtest.h` + `/usr/local/lib/libgtest.a`（已实测存在）。CMake 用 `find_path(GTEST_INCLUDE_DIR ...)` / `find_library(GTEST_LIBRARY ...)` 定位，test-only，运行时仍零第三方依赖。
