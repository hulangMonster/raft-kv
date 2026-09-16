# raftkv —— Raft-based Distributed Key-Value Store

> 里程碑：M1 ✅ 单机版 KV + WAL；M2 ✅ Raft 选主 + 日志复制；M3 ✅ 快照与日志压缩（含 group commit，已按 #4 评审加固）；M4 ✅ 成员变更 + 客户端路由 / 线性一致读（已按 #4 第三轮独立评审加固，7 个阻断项全部修复）。Roadmap 见
> [docs/roadmap.md](docs/roadmap.md)。

一个从零实现、面向简历与生产场景的分布式 KV 存储项目：
M1 先写出一台**可持久化、可压测、有故障测试**的单机 KV，
M2 在它之上实现 Raft 共识，演进为**多节点、可自动选主、可故障切换**的集群。

## 当前能力（M1）

- 二进制 TCP 协议（长度前缀 + 版本校验 + CRC 防护），支持 `put/get/del`
- 内存 KV（单锁阶段，WAL 与内存顺序一致）
- WAL 追加写 + 每次写 fsync（`--no-sync` 可关闭做压测）
- 启动时 WAL 重放；`kill -9` 后半条记录会被 CRC 检测并截断
- 线程池服务端（accept 线程 + N 个 worker），SIGINT/SIGTERM 优雅退出
- 命令行客户端 + 顺序压测（QPS / avg / p50 / p99）
- 单元测试（存储 / WAL 重放 / 编解码）+ `scripts/e2e.sh` 端到端冒烟与崩溃恢复测试

## 当前能力（M2：Raft 集群）

- 3 节点集群：Leader 选举（随机超时、心跳续任、任期规则）、日志复制与冲突截断
- §5.4.2 提交规则（只提交当前 term 的多数派）、`KvStateMachine` 幂等 apply
- `FileLogStore` 持久化（meta.dat 原子写 + raft.log CRC + torn-tail 截断），Raft log 为唯一持久化真相源
- 节点进程 `raftkv_raft_node`、集群客户端 `raftkv_raft_cli`（NotLeader 重定向 + 交互 REPL）
- 测试：`raftkv_raft_tests` 14/14；`scripts/raft_e2e.sh`；`scripts/raft_fault.sh --repeat 50`

## 当前能力（M3：快照与日志压缩 + 组提交）

- **快照**：按 `lastApplied` 两段式生成（锁内取一致视图、锁外序列化+落盘）；`SnapshotStore` seam = `MemorySnapshotStore` / `FileSnapshotStore`（`RKS1` 格式、tmp+fsync+rename+fsync(dir)、CRC、torn-snapshot 丢弃）
- **日志压缩**：`LogStore::compact` 前缀物理删除 + **基址模型**（压缩后 `firstIndex = lastIncluded+1`、边界感知 `termAt/slice/lastIndex/lastTerm`）；**10w+ 条后 `raft.log` 有界**
- **InstallSnapshot**（msgType 5/6，分块传输）：落后 / 空节点经快照快速追平
- **启动恢复**：先加载快照，再重放 `raft.log` 尾部（`setBoundary` 先于 `load`）
- **group commit**：`appendNoSync()` + `sync()`，一次 fsync 摊一批；`buildAppendEntries` 只复制已持久化条目；fsync 失败绝不记为已持久（I5）
- **#4 评审加固**（15 个阻断项，见 `docs/m3-design.md` §12）：单节点组提交提交点、`LogStore` fd 锁下沉（L9）、
  `SnapshotStore` 边界不回退 + 并发安装串行、InstallSnapshot 幂等续传（丢回复不卡死）、后缀 term 校验、
  `commitIndex` 按"最后一条与本 leader 匹配的条目"推进、恢复路径失败即拒绝启动、压缩区守卫
- 客户端：`fill <N> --pipeline K`（K 条并发连接，**每个 worker 独立 clientId**）、`verify <N>`（写回校验）、`snapshot`（手动触发）
- 测试：`raftkv_raft_tests` **35/35**（含 ASan 全绿）；`scripts/raft_snapshot_e2e.sh`；`scripts/raft_snapshot_fault.sh --repeat 50`；`scripts/bench_group_commit.sh`
- 实测吞吐（3 节点，`bench_group_commit.sh`，**每次 `fill` 后用 `verify` 校验零丢写**）：
  `--pipeline 1` → **129 qps**；`--pipeline 8` → **746 qps**；`--pipeline 64` → **2840 qps（≈22×）**，三次均 `missing 0`
  （旧记录的 2623 qps 是在"`--pipeline` 共享 clientId 会静默丢写"的 bug 下测得的，不计入）

```bash
# 小阈值触发快照 + 压缩；批量写 10w 条（K 条并发连接）
./build/bin/raftkv_raft_node --id 1 --port 19601 --peers "$PEERS" --data-dir /tmp/r1 --snapshot-threshold 2000
./build/bin/raftkv_raft_cli --peers "$PEERS" --host 127.0.0.1 --port 19601 fill 100000 --pipeline 64
./build/bin/raftkv_raft_cli --peers "$PEERS" --host 127.0.0.1 --port 19601 status   # snapshot_index/term
./build/bin/raftkv_raft_cli --peers "$PEERS" --host 127.0.0.1 --port 19601 snapshot # 手动触发
```

## 当前能力（M4：成员变更 + 客户端路由 / 线性一致读）

- **在线成员变更**：one-at-a-time + 新节点先 **CatchUp**（非投票、不计多数派）追平后才写入配置；配置条目进 raft.log（延续 D1，无独立配置存储），**追加即生效**（决策③）且提交需 **C_old 与 C_new 双多数派**（J2）
- **配置持久化**：快照携带配置（RKS1 **v2**，尾部追加 config 段，**兼容 v1**）；启动按「快照配置 → 日志配置条目」重建，**版本回退即拒绝启动**（J3）；配置条目被截断时按基线**回滚**（A19）
- **移除语义**：被移除节点在**确认收到**移除它的配置条目之前一直是复制目标（`drainingPeers_` 带确认目标与预算，超时兜底），确认后回收每 peer 状态并从 transport 摘除（`removablePeersLocked()`）——避免它永远自认成员、靠竞选抬高任期搅乱集群；被移除 / 未入配置的节点进入**退役态**（不竞选、不接受写、**不投票**，但继续接收复制）
- **选举资格（J4）**：只给当前配置里的投票成员投票；非成员、CatchUp 目标、退役节点一律无票——否则已移除但仍持旧配置的分区节点可以拿旧多数派当选并截断已提交条目
- **线性一致读**：GET 走 **ReadIndex**（探针 msgType 9/14 + quorum 确认 + **§8 同任期提交屏障** + 等 lastApplied >= readIndex），失败即报错，**绝不退化为读本地状态机**，也绝不在新 Leader 尚未提交本任期条目时按陈旧 commitIndex 返回旧值
- **客户端路由与拓扑发现**：可连任意节点；客户端缓存 `{configVersion, id→addr}`，收到 `kNotLeader`（leaderHint 查不到）或连接失败时向任意可达节点取最新配置视图后重新路由，`--peers` 退化为"一个可达种子"；新增 config / add <id> <host:port> / remove <id> 子命令
- **变更串行化**：`changeMembership` 全过程（校验 → CatchUp → 追加配置条目 → 等提交）串行化，并发变更立即被拒（J1）
- 测试：raftkv_raft_tests **68/68**（M2 14 + M3 14 + M4 A1–A29 / B1–B4）；scripts/raft_membership_e2e.sh（3→4 节点在线增删 + 线性一致读 + **客户端拓扑刷新两步**）；scripts/raft_membership_fault.sh --repeat 50（5 节点，变更窗口内 kill -9 / SIGSTOP）

      # 任意节点接入即可（自动重定向到 Leader；--peers 只给一个可达种子也够）
      ./build/bin/raftkv_raft_cli --peers "$PEERS" --host 127.0.0.1 --port 19601 config
      ... add 4 127.0.0.1:19604   # 第 4 个节点用 --peers 现有成员启动（非投票），追平后加入
      ... remove 4
      ... status                  # config_version / members / retired / read_index

- **吞吐测量条件（重要）**：同一机器状态下同一探针 fill 200 --pipeline 1 = **13.2 ms/写**，
  且 M3 收尾版（b747c70）与 M4 当前版**完全一致** ⇒ M4 无可测回归；
  bench_group_commit.sh（threshold=5000，会频繁触发压缩）本次测得 34 / 175 / 814 qps，
  历史记录的 129 / 746 / 2840 qps 是当时较空闲机器状态下所测，**跨会话不可直接比较**。

## 目录结构

```
raft-kv/
├── CMakeLists.txt
├── src/
│   ├── common.h        类型、日志、大端编解码工具
│   ├── codec.{h,cpp}   线协议编解码（见 docs/protocol.md）
│   ├── wal.{h,cpp}     Write-Ahead Log（CRC32 + torn-tail 截断）
│   ├── store.{h,cpp}   内存 KV + WAL 一致性（先 WAL 后内存）
│   ├── thread_pool.h   固定线程池
│   ├── server.{h,cpp}  多线程 TCP 服务端
│   ├── main_server.cpp 服务端入口
│   └── main_client.cpp 命令行客户端 / 压测
├── tests/              轻量单元测试（无第三方依赖）
├── scripts/e2e.sh      e2e + kill -9 崩溃恢复冒烟
└── docs/               protocol.md / roadmap.md
```

## 构建与运行（Linux，建议在虚拟机 / WSL 里）

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j

# 终端 1：启动服务（数据落在 ./data，每次写 fsync）
./build/bin/raftkv_server --port 9527 --data-dir ./data --workers 4

# 终端 2：命令行操作
./build/bin/raftkv_cli put user:42 alice
./build/bin/raftkv_cli get user:42
./build/bin/raftkv_cli del user:42
./build/bin/raftkv_cli get missing        # -> NOT_FOUND

# 单元测试 + 端到端冒烟
./build/bin/raftkv_tests
./scripts/e2e.sh
```

## Raft 集群（M2）快速开始

```bash
PEERS="1=127.0.0.1:19601,2=127.0.0.1:19602,3=127.0.0.1:19603"

# 三个终端（或后台）各启动一个节点
./build/bin/raftkv_raft_node --id 1 --port 19601 --peers "$PEERS" --data-dir /tmp/raft1
./build/bin/raftkv_raft_node --id 2 --port 19602 --peers "$PEERS" --data-dir /tmp/raft2
./build/bin/raftkv_raft_node --id 3 --port 19603 --peers "$PEERS" --data-dir /tmp/raft3

# 集群客户端：连任意节点即可（自动重定向到 Leader）
./build/bin/raftkv_raft_cli --peers "$PEERS" --host 127.0.0.1 --port 19601 put user:42 alice
./build/bin/raftkv_raft_cli --peers "$PEERS" --host 127.0.0.1 --port 19602 get user:42
./build/bin/raftkv_raft_cli --peers "$PEERS" status

# 不带命令进入交互模式（REPL）：put/get/del/status/quit
./build/bin/raftkv_raft_cli --peers "$PEERS" --host 127.0.0.1 --port 19601

# 集群端到端 + 故障注入稳定性
./scripts/raft_e2e.sh
./scripts/raft_fault.sh --repeat 50
```

压测（注意：默认每次写都 fsync，压测请加 `--no-sync` 对比）：

```bash
./build/bin/raftkv_server --no-sync --data-dir /tmp/raftkv-bench &
./build/bin/raftkv_cli bench 20000 --value-size 256
# 输出示例：bench: n=20000 ... qps=... avg_us=... p50_us=... p99_us=...
```

## 协议速览

详见 [docs/protocol.md](docs/protocol.md)。要点：

```
请求 : [ver:1][op:1][keyLen:2][valLen:4][key...][value...]
响应 : [status:1][valLen:4][payload...]   # GET: 值；错误时：错误消息
```

## 为什么这样做（面试可讲的三句话）

1. **先持久化后可见**：所有写操作先落 WAL 再改内存，且加锁保证二者顺序一致，
   崩溃后内存态可以由日志精确重放 —— 这是后面 Raft 日志复制的同款地基。
2. **故障可证伪**：`kill -9` + 重启 + 数据仍在，由 `scripts/e2e.sh` 自动化证明，
   不是"我说它可靠"。
3. **零件不散装**：线程池、定长/变长编解码、CRC、同步原语都是项目内自然长出来的，
   而不是简历上单独列一排 demo。
