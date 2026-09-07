# Roadmap

每个里程碑都带**验收标准**：完成的标准不是"写了代码"，而是
`cmake --build build && ./build/bin/raftkv_tests && ./scripts/e2e.sh`（或对应阶段脚本）通过，
并且能对着架构图讲清楚一个设计决策。

## M1 ✅ 单机版 KV + WAL（当前已完成）

- 内存 KV（put/get/del）、WAL 追加写与启动重放
- 写路径：WAL 先行 → 内存应用，加锁保证顺序一致
- CRC32 检测 torn tail，启动时截断
- 线程池 TCP 服务端、命令行客户端、顺序压测
- 验收：单测全绿；`e2e.sh` 覆盖 kill -9 后重启数据仍在

**M1 → M2 之间补课**：Raft 论文 §5（5.1–5.3）、gRPC 入门示例、
一致性哈希 / CAP 通俗材料，读一遍即可动手。

## M2 🔜 Raft：选主 + 日志复制（下一个里程碑）

- 3 节点（进程）模拟：Leader 选举（随机超时、心跳续任）
- 日志追加与复制（AppendEntries）、提交过半即应用
- 客户端只写 Leader；写操作先落本机 WAL 再走 Raft（复用 M1 的 Wal）
- 验收：
  - 脚本启 3 节点，kill Leader → 其余节点在超时窗口内选出新 Leader
  - kill 任意 1~2 节点期间写入不丢（数据由日志重放恢复）
  - `scripts/raft_test.sh` 可重复跑通（含故障注入）
- 设计要点：节点间消息沿用现有 `codec` 风格自描述帧，先不引入 gRPC，
  减少依赖、便于讲清协议边界；M5 可选统一迁移 gRPC。

## M3 快照与日志压缩

- 日志无界增长 → 定期生成快照（序列化整份 KV）
- 追日志慢的节点通过 InstallSnapshot 快速跟上
- 验收：写 10w+ 条后日志文件有界；新节点可快速追平

## M4 成员变更 + 客户端路由

- 单节点增减（Joint Consensus 简化版：先只支持一次加/减一个）
- 客户端从任意节点路由到 Leader；GET 默认读 Leader（线性一致）
- 验收：5 节点集群在线增删节点，压测期间无长时间不可用

## M5 性能与可观测性（压测故事 + 优化）

- 观测：接入 perf / 火焰图，先量化再优化
- 优化方向：epoll/Reactor 替代 thread-per-conn、分片锁或并发哈希、
  WAL 组提交（group commit）、批处理复制
- 输出：优化前后 QPS / p99 / 锁竞争对比表（这部分直接进简历）
- 可选：Node Exporter 风格指标端点、gRPC 接口层

## 贯穿性工程要求

- 每个阶段先写"怎么验证"，再写实现
- 提交信息讲清楚动机（如 `wal: truncate torn tail on replay`）
- 每完成一个里程碑更新 README 的"当前能力"与架构图
