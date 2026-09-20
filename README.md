# raftkv —— Raft-based Distributed Key-Value Store

> 里程碑：**M1 ✅ 单机 KV + WAL** · **M2 ✅ Raft 选主 + 日志复制** · **M3 ✅ 快照与日志压缩 + group commit** ·
> **M4 ✅ 成员变更 + 客户端路由 / 线性一致读** · **M5 ✅ 性能优化与可观测性**
> （M1–M5 全部实现并发布；p=8/64 的同机比值吞吐曾未达标，已定位并修复 ——
> 见 [性能与已知取舍](#性能与已知取舍) 与 [docs/m5-bench.md](docs/m5-bench.md) §3.11；
> 路线图见 [docs/roadmap.md](docs/roadmap.md)）

一个从零实现、面向简历与生产场景的分布式 KV：
M1 先写出**可持久化、可压测、有故障测试**的单机 KV，M2 在其上实现 Raft 共识 →
多节点、自动选主、可故障切换，M3 快照/压缩/组提交，M4 在线成员变更 + 线性一致读，
M5 两段式持久化（锁内零 fsync）+ epoll Reactor + 流式快照/断点续传 + 指标。

---

## 快速开始

### 0. 环境

| 需要 | 说明 |
|---|---|
| Linux | Ubuntu 22.04（或用 WSL2 / 虚拟机；项目只用 POSIX，没有平台特定代码） |
| 编译器 | `g++` ≥ 11（C++17） |
| 构建 | `cmake` ≥ 3.22 |
| 可选 | GTest（缺省时 `raftkv_raft_tests` 目标自动跳过；`raftkv_tests` 是零依赖的自带测试） |

### 1. 构建

```bash
git clone <this-repo> && cd raft-kv
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"

ls build/bin
# raftkv_server  raftkv_cli                 # M1 单机版
# raftkv_raft_node  raftkv_raft_cli         # M2+ 集群版 + 客户端
# raftkv_tests  raftkv_raft_tests           # 测试
```

可选：Sanitizer 构建（与 Release 构建互不影响，用独立目录）

```bash
cmake -S . -B build-asan -DCMAKE_BUILD_TYPE=RelWithDebInfo -DENABLE_ASAN=ON && cmake --build build-asan -j
cmake -S . -B build-tsan -DCMAKE_BUILD_TYPE=RelWithDebInfo -DENABLE_TSAN=ON && cmake --build build-tsan -j
```

### 2. 跑单机版（M1：KV + WAL 崩溃恢复）

```bash
# 终端 1：起服务（每次写都 fsync；--no-sync 可关掉做纯内存压测；./data 已在 .gitignore 中）
./build/bin/raftkv_server --port 9527 --workers 4 --data-dir ./data

# 终端 2：
./build/bin/raftkv_cli --port 9527 put user:42 alice   # -> OK
./build/bin/raftkv_cli --port 9527 get user:42         # -> alice
./build/bin/raftkv_cli --port 9527 del user:42         # -> OK
./build/bin/raftkv_cli --port 9527 get user:42         # -> NOT_FOUND
./build/bin/raftkv_cli --port 9527 bench 20000 --value-size 64   # QPS / avg / p50 / p99
```

### 3. 跑 3 节点集群（M2–M5）

```bash
PEERS="1=127.0.0.1:19601,2=127.0.0.1:19602,3=127.0.0.1:19603"

# 起三个节点（--snapshot-threshold 2000 让小阈值就触发快照/压缩）
for id in 1 2 3; do
  ./build/bin/raftkv_raft_node --id $id --port $((19600+id)) --peers "$PEERS" \
      --data-dir /tmp/raftkv-n$id --snapshot-threshold 2000 > /tmp/raftkv-n$id.log 2>&1 &
done
sleep 2

CLI="./build/bin/raftkv_raft_cli --peers $PEERS --host 127.0.0.1 --port 19601"
$CLI status            # role / term / commit_index / last_applied / snapshot_index / qps / fsync_ms / …
$CLI put hello world   # -> OK（连 follower 会自动重定向到 Leader）
$CLI get hello         # -> world（线一致读：ReadIndex + 同任期提交屏障）

$CLI fill 20000 --pipeline 64 >/dev/null   # 64 条并发连接批量写
$CLI verify 20000                          # -> verified 20000 missing 0（硬校验零丢写）
$CLI snapshot                              # 手动触发快照
$CLI metrics | head                        # Prometheus 文本（msgType 15）
$CLI config                                # 当前配置视图（version / members）
$CLI add 4 127.0.0.1:19604                 # 在线加入（新节点先 CatchUp 追平再进配置）
$CLI remove 4                              # 在线移除
$CLI                                       # 不带命令 = 交互 REPL（put/get/del/status/…/quit）
```

> `--peers` 只是**一个可达种子**：客户端会向任意可达节点索取最新配置（`kConfigRequest`）并按新拓扑路由，
> 因此成员变更后不需要重启客户端。

### 4. 跑测试与故障注入

```bash
./build/bin/raftkv_tests          # M1：13/13（零依赖自测）
./build/bin/raftkv_raft_tests     # M2–M5：94/94（gtest；含 A 组契约与 R 组 Reactor 用例）

# 端到端 + 故障注入（脚本会自己起/停节点、随机端口、每轮 verify missing 0）
./scripts/raft_e2e.sh                              # 3 节点基本路径
./scripts/raft_fault.sh --repeat 50                # SIGSTOP/kill -9 一个或多个 follower
./scripts/raft_snapshot_e2e.sh                     # 快照 + 空节点经 InstallSnapshot 追平
./scripts/raft_snapshot_fault.sh --repeat 20       # 10w 条 + 压缩有界 + 崩溃/挂起注入
./scripts/raft_membership_e2e.sh                   # 3→4 在线增删 + 线性一致读
./scripts/raft_membership_fault.sh --repeat 50     # 5 节点 + 变更窗口内 kill -9/SIGSTOP
./scripts/e2e.sh                                   # M1 冒烟 + WAL 崩溃恢复
```

TSan（本仓库要求 0 报告；`setarch -R` 关 ASLR，`tests/tsan.supp` 只窄抑制 libstdc++
`condition_variable_any` 的库层误报，文件里写了出处论证）：

```bash
TSAN_OPTIONS=suppressions=tests/tsan.supp setarch "$(uname -m)" -R ./build-tsan/bin/raftkv_raft_tests
```

### 5. 跑性能基准（同机交替 A/B）

```bash
./scripts/bench_m5_ab.sh --repeats 3     # M4 基线(dc6c56a) vs 当前工作区，同轮交替、3 次取中位数
./scripts/bench_m5_ab.sh --quick         # 快速冒烟（1 次）
./scripts/bench_group_commit.sh          # 组提交 / 批量的观察脚本
# 单次持久化提交延迟微基准（与 raftkv 无关，用于判断硬件下限）
g++ -O2 -std=c++17 -o /tmp/fsbench scripts/fsbench_commit_latency.cpp && /tmp/fsbench
```

---

## 命令参考

### `raftkv_raft_node`（集群节点）

| 参数 | 默认 | 说明 |
|---|---|---|
| `--id N` | 必填 | 节点 id |
| `--port P` | 必填 | 监听端口 |
| `--peers "1=h:p,2=h:p,…"` | 必填 | 启动种子配置（可含自己；不在其中 = 动态加入的非投票节点） |
| `--data-dir DIR` | `./raft-data-<id>` | 数据目录（`raft/raft.log`、`meta.dat`、`snapshot.dat`） |
| `--snapshot-threshold N` | 10000 | 距上次快照多少条已应用条目触发一次快照 + 日志压缩 |
| `--transport[=]sync\|reactor` | `sync` | 传输引擎（也可用环境变量 `RAFTKV_TRANSPORT`） |
| `--inflight-per-peer N` | 1 | 异步引擎下每 peer 允许同时在途的 AppendEntries 批数（滑动窗口） |
| `--group-linger-us N` | 0 | 组提交蓄批窗口（微秒）；实测无增益，默认关 |
| `--lock-wait-metrics` | 关 | 打开 `mu_` 等待耗时统计（`lock_wait_us_*`） |

### `raftkv_raft_cli`（集群客户端）

`--peers "1=h:p,…"`（种子）· `--host H` · `--port P` · 子命令：

| 子命令 | 说明 |
|---|---|
| `put <k> <v>` / `get <k>` / `del <k>` | 写/读/删（GET 走 ReadIndex 线性一致读） |
| `status` | 节点状态 + 指标（`qps/lat_p50_us/lat_p99_us/fsync_calls/fsync_ms/batch_avg/batch_max/repl_lag_max/elections_total/snapshots_total/snapshot_bytes/lock_wait_*/inflight_rpc`） |
| `fill <n> [--pipeline K]` | K 条并发连接批量写（每个 worker 独立 clientId） |
| `verify <n>` | 读回校验，输出 `verified n missing m`；`m=0` 是硬门禁 |
| `snapshot` | 手动触发一次快照 |
| `metrics` | Prometheus 文本指标（msgType 15，与 `status` 同源） |
| `config` / `add <id> <h:p>` / `remove <id>` | 配置视图 / 在线增删成员 |
| 无子命令 | 进入交互 REPL |

### `raftkv_server` / `raftkv_cli`（M1 单机版）

`--port` · `--workers` · `--data-dir` · `--no-sync`（M1 服务端）；`put/get/del/bench <n> [--value-size B]`
（M1 客户端，`--host/--port`）。

---

## 性能与已知取舍

**(1) 绝对判据在本机物理不可达 —— 已用独立微基准证明。**
`scripts/fsbench_commit_latency.cpp`（4 KiB 追加 + flush × 200，与 raftkv 无关）实测本机单次持久化提交：
`fsync 9.67 ms` / `fdatasync 9.74 ms` / `预分配+fsync 7.93 ms` / `O_DIRECT 7.60 ms` —— **下限 ≈8 ms**。
而 Raft 的 I5/I11 要求"条目 durable 之后才 ack"，所以 `pipeline=1 ≤8 ms/写` 等价于要求
"提交 + 复制往返"不花时间。冻结基线期同一台机器的 M4 自身也只有 16.4 ms/写。

**(2) 验收口径 = 同机比值**（`scripts/bench_m5_ab.sh`，同轮交替、3 次中位数、每格 `verify missing 0`）：

| pipeline | 判据 | 实测 M5/M4 | 结论 |
|---|---|---|---|
| 1 | 延迟 ≤ 1.2× | **1.03×** | ✅ 达标 |
| 8 | 吞吐 ≥ 0.8× | **1.34×** | ✅ 达标 |
| 64 | 吞吐 ≥ 0.8× | **2.20×** | ✅ 达标 |

（上表为 2026-09-20 P2a 修复后的复测值：p=8 n=20000 M5 510 qps vs M4 380，p=64 n=2000
2129 vs 967，p=1 n=500 6.84 vs 6.66 ms/写；每格 `verify missing 0`。修复前的 0.60×/0.57×
以及当时 M4 基线的 `missing` 已记入 `docs/m5-bench.md` §3.1/§3.11。）

同一轮 A/B 里 **M4 基线出现 `missing 5` / `missing 9`，而 M5 全部 `missing 0`** ——
M5 的墙钟代价换来的是"零丢写 + 锁内零 fsync + Reactor + 可观测性"。

**(3) p=8/64 差距：已定位并修复（P2a，2026-09-20；详见 `docs/m5-bench.md` §3.11）。**
分段实测否定了原先"唤醒 + 单把全局锁争用"的归因：瓶颈在**复制发送段的排队** —— `syncInFlight_`
在 fsync 返回后立即放开，于是"上一批还在阻塞发送、下一批已开始 fsync"，多个 flusher 在
`TcpTransport` 唯一那把锁上互相排队（peer=2 的发送"拿到锁之前"中位等 **11.5 ms**，而一次真正
往返 **<1 ms**）。修法：flusher 等本批两次 `sendAppendEntries` 都发出后再放开 flush 窗口。
复测：p=8 **0.47×→1.34×**、p=64 **~1.06×→2.20×**、p=1 延迟 **1.08×→1.03×**，每格 `missing 0`；
顺带 `batch_avg` 由 1 升到 4~8（组提交一并变好）。§3.9 的"并行扇出更差"与 §3.10 的"唤醒机制"结论
据此作废。

**(4) 默认引擎仍是 `sync`（P2a 后重测过口径）。** Reactor（epoll）引擎端到端与故障注入全绿、
丢写根因已修并有回归用例。同轮交替重测（发布内容、n=500/2000/2000、每格 `missing 0`）：
p=1 **reactor 快 1.17×**（4.87 vs 5.69 ms/写）、p=8 **打平**（514 vs 511 qps）、
p=64 **sync 快 1.38×**（2317 vs 1676 qps）。⇒ 低并发 reactor 略优、**高并发 sync 明显更优**，
默认保持 `sync`；只要低并发延迟时显式 `--transport=reactor` 即可。
（P2a 之前"reactor 在 p=8 领先 2.06×"的对照已作废——那时 sync 还处在多个 flusher 抢传输锁的状态，
见 `docs/m5-bench.md` §3.11。）

**(5) 未做的（非本阶段目标）**：分片锁/并发哈希、perf 火焰图（本机 `perf_event_paranoid=4`）、gRPC 接口层。

---

## 当前能力（按里程碑）

### M1：单机 KV + WAL

- 二进制 TCP 协议（长度前缀 + 版本校验 + CRC 防护），`put/get/del`
- WAL 追加写 + 每次写 fsync（`--no-sync` 可关）；启动重放；`kill -9` 后半条记录被 CRC 检测并截断
- 线程池服务端（accept + N worker），SIGINT/SIGTERM 优雅退出；命令行客户端 + 顺序压测
- 测试：`raftkv_tests` 13/13 + `scripts/e2e.sh`（含崩溃恢复）

### M2：Raft 集群

- 3 节点选举（随机超时、心跳续任、任期规则）、日志复制与冲突截断；§5.4.2 提交规则；`KvStateMachine` 幂等 apply
- `FileLogStore`（`meta.dat` 原子写 + `raft.log` CRC + torn-tail 截断）—— **Raft log 是唯一持久化真相源**
- `raftkv_raft_node` / `raftkv_raft_cli`（NotLeader 重定向 + REPL）

### M3：快照与日志压缩 + 组提交

- 快照两段式（锁内取一致视图、锁外序列化落盘）；`SnapshotStore` seam = Memory/File（`RKS1` 格式、tmp+fsync+rename+fsync(dir)、CRC、损坏丢弃）
- `LogStore::compact` 前缀物理删除 + 基址模型（`firstIndex = lastIncluded+1`，边界感知 `termAt/slice/lastIndex/lastTerm`）；10w+ 条后 `raft.log` **有界**
- InstallSnapshot（msgType 5/6 分块）；启动先载快照再重放日志尾部
- group commit：`appendNoSync()` + `sync()`，一次 fsync 摊一批；fsync 失败绝不记为已持久（I5）

### M4：成员变更 + 客户端路由 / 线性一致读

- 在线成员变更：one-at-a-time + **CatchUp**（非投票、不计多数派）后进配置；配置条目进 raft.log、**追加即生效**，提交需 **C_old 与 C_new 双多数派**（J2）
- 配置持久化：快照携带配置（`RKS1` **v2**，兼容 v1）；启动按「快照配置 → 日志配置条目」重建，**版本回退即拒绝启动**（J3）
- 移除语义：被移除节点在**确认收到**移除条目之前仍是复制目标（`drainingPeers_` 带预算），确认后回收并摘除；被移除/未入配置节点进入**退役态**（不竞选、不接受写、不投票）
- 选举资格（J4）：只给当前配置的投票成员投票——否则已移除但仍持旧配置的分区节点可能当选并截断已提交条目
- 线性一致读：GET 走 **ReadIndex**（探针 + quorum + §8 同任期提交屏障 + 等 `lastApplied ≥ readIndex`），失败即报错，**绝不退化为读本地状态机**

### M5：性能优化与可观测性

- **两段式持久化（I9：锁内 fsync==0）**：锁内只 `write`，fsync 一律移到锁外、回锁后重新校验 term/role/lastIndex/installEpoch。
  断言不是"看代码"：`SpyLogStore` + 线程局部锁探针（`lockprobe::consensusHeld()`）统计持锁窗口内 `sync/compact/persistMeta` 必须为 0（**A9 当场抓到一处虚调用陷阱**：`truncateSuffixNoSync` 里无限定名调用落回含 fsync 的版本）
- **Reactor（epoll）传输**：`--transport=reactor`；非阻塞发送、每连接请求-应答严格配对、超时丢帧不回调（上层槽位 TTL 兜底）、`stop()` 停循环并丢弃在途回调
- **批处理与滑动窗口**：`--inflight-per-peer`（默认 1）+ 乐观 `nextIndex_` 推进 + TTL 过期回退
- **流式快照（§8.3）**：`SnapshotView::stream()` 分块产出（超大单条 carry-over，块严格 ≤ maxChunk）；`saveStreaming()` 两趟走流（先算 payloadLen/增量 CRC32，再分块写 + fsync + rename + fsyncDir），落盘与整块编码**逐字节一致**；leader 发 InstallSnapshot 时 `readInstalled()` 按需读文件切片（O(块)，不再常驻整块）
- **跨进程断点续传（§8.4）**：`.recv` 尾部携带 `RKR1|idx|term|receivedLen|crcSoFar`（每块更新）；**新进程**从尾部恢复进度续传；安装时 `ftruncate` 掉尾部再 `rename`
- **可观测性**：`status` 全量指标 + `metrics` 端点（只读，不参与任何正确性判定）
- **P2a 复制流水线修正（p=8/64 达标）**：flusher 等本批两次 `sendAppendEntries` 都发出后再放开
  `syncInFlight_`，消除"多个 flusher 并发在 `TcpTransport` 全局锁上排队"（M5.A16 守门）。
  同机比值：p=8 **0.47×→1.34×**、p=64 **~1.06×→2.20×**、p=1 延迟 **1.08×→1.03×**（详见 [docs/m5-bench.md](docs/m5-bench.md) §3.11）
- 测试：`raftkv_raft_tests` **94/94**（含 A13/A14/A15/A16 四个 P2a 阶段新增的 RED→GREEN 用例）、
  TSan **94/94 且 0 报告**、干净重建 0 warning、`raft_e2e.sh` + `raft_fault/snapshot_fault/membership_fault`
  各 10 轮全 PASS、A/B 每格 `missing 0`
- 复盘（面试口径：六个真 bug 的定位/根因/回归 + 两次负结果 + 方法论）见 **[docs/m5-review.md](docs/m5-review.md)**

---

## 目录结构

```
raft-kv/
├── CMakeLists.txt
├── src/
│   ├── common.h codec.* wal.* store.* thread_pool.* server.*   # M1 单机版
│   ├── main_server.cpp main_client.cpp                          # M1 入口
│   ├── main_raft_node.cpp main_raft_client.cpp                   # M2+ 节点/客户端入口
│   ├── kv/kv_state_machine.*                                     # KV 状态机（幂等 apply + 快照编解码）
│   └── raft/                                                     # Raft 核心
│       ├── raft_node.*  types.h  message.*  cluster_config.*      # 共识状态机 / 线格式 / 配置
│       ├── log_store.h  file_log_store.cpp  memory_adapters.cpp   # 日志存储（基址模型 + CRC）
│       ├── snapshot_store.h  file_snapshot_store.cpp              # 快照（流式落盘 + 断点续传）
│       ├── transport.h  transport_tcp.*  transport_reactor.*      # 传输（sync / epoll Reactor）
│       ├── reactor.*                                             # epoll 事件循环
│       ├── metrics.*  lock_probe.h                               # 指标 / 锁探针
│       └── state_machine.h  clock.h
├── tests/        raftkv_tests（13）+ raft_*_test.cpp（gtest，90）+ tsan.supp
├── scripts/      e2e/raft_e2e/fault/snapshot/membership 系列 + bench_m5_ab.sh + fsbench_commit_latency.cpp
└── docs/         protocol.md roadmap.md m2/m3/m4/m5-design.md m5-prerequisites.md m5-bench.md m5-review.md
```

---

## 协议速览

见 [docs/protocol.md](docs/protocol.md)。要点：长度前缀帧（4 字节大端长度 + 负载）、版本与 op 白名单、
CRC32 校验；客户端 `ClientRequest/ClientReply`（含 `kNotLeader` + `leaderHint`）、节点间
`RequestVote/AppendEntries/InstallSnapshot/ReadProbe`（msgType 9/14）、配置查询与 `metrics`（msgType 15）。
**kConfig 不在客户端白名单内**——客户端无法伪造配置条目。

## 为什么这样做（面试可讲的三句话）

1. **先持久化后可见**：写操作先落 WAL 再改内存，且加锁保证二者顺序一致；崩溃后内存态由日志精确重放
   —— 这正是后面 Raft 日志复制的同款地基（M5 更进一步：fsync 移出锁，但"durable 之后才 ack"不变）。
2. **故障可证伪**：`kill -9` + 重启 + 数据仍在、`SIGSTOP` 下多数派仍可写、`verify missing 0` 是硬门禁，
   全部由 `scripts/*.sh` 自动化证明，而不是"我说它可靠"。
3. **先量化再优化、负结果也入档**：绝对性能判据先被独立微基准证伪（8 ms 提交下限），再改成同机比值口径；
   两次失败的优化尝试（并行扇出、蓄批）都带数据写进 `docs/m5-bench.md`，避免后人重走。第三次换了方法——
   先做**分段插桩**定位，才发现真因是"多个 flusher 并发抢传输锁"，修好后比值 p=8 **0.47×→1.34×**、
   p=64 **→2.20×**（§3.11）：**负结果不是终点，"下一步该测什么"才是资产**。
