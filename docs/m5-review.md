# M5 复盘（性能优化与可观测性）——面试口径

> 这份文档是 M5 的"讲清楚"版本：做了什么、**怎么定位**、哪些结论是负的、以及为什么最终的
> 判定长这样。所有数字都能在 `docs/m5-bench.md` 与本仓库的测试/脚本里复现。

## 1. 一句话概括

在 M1–M4 的 Raft KV 上做 M5：把 fsync 移出共识锁（I9）、引入 reactor（epoll）传输、做组提交与
滑动窗口、补流式快照与断点续传、加指标；**过程中定位并修复了 6 个真实缺陷（含 1 个已提交条目
被覆盖的安全性缺陷与 1 个崩溃）**，全部配回归用例；性能上**证明了绝对目标在本机物理不可达**
（单次持久化提交 ≈8 ms），把验收口径改为同机比值，并留下两轮 A/B 与两份定量负结果。

## 2. 六个真 bug：怎么发现的、怎么修的

| # | 现象 | 定位手段 | 根因 | 修复 + 回归 |
|---|---|---|---|---|
| 1 | ASan 报 `SEGV in FileLogStore::termAt` | ASan + 独立评审的 gdb 栈 | M5 把 `compact()` 移出 `mu_` 后，只读访问器（`slice/termAt`）仍靠调用方锁保护 → 与 compact 竞争 | store 自持 `recursive_mutex`，所有访问器持锁 |
| 2 | ASan 报锁内 fsync（违反 I9） | `SpyLogStore` + 线程局部"是否持共识锁"探针（M5.A1/A9） | 冲突回滚走 `truncateSuffix()`（含 fsync）；且 `MemoryLogStore::truncateSuffixNoSync()` 里是**无限定名的虚调用**，落回子类含 fsync 的版本 | 拆出 `truncateSuffixNoSync()` + 限定名调用；durability 交同批锁外 `sync()` |
| 3 | reactor 下 `raft_fault` 2/3 轮 `NOT_LEADER`、term 每秒 +1.5 | `RAFTKV_TRACE` 临时插桩看 `appendSkip`/`startElection` | "每 peer 单批在途"的静音窗口 = `2*rpcTimeoutMs=200ms` **大于** follower 选举超时 150ms；reactor 丢帧不回调 → 健康 follower 收不到心跳 | 窗口 = `min(2*rpcTimeoutMs, electionTimeoutMinMs/3)`（I15）+ M5.A10 |
| 4 | reactor + 频繁 compact 时节点**无日志静默死亡** | 机制复现脚本 + `kill -0` 监视 | `maybeSnapshot`（ticker）与 `onInstallSnapshot`（reactor 线程）都在 `mu_` 外 compact 同一个 `FileLogStore`；踩坏后 `compact` 返回 false → `throw` 从 reactor 回调逃逸 → `std::terminate` | 叶子锁 `snapshotOpMu_` 串行化 + 回调异常打印后重抛（可诊断） |
| 5 | p=64 偶发丢写（`missing 1..49`） | 给每帧算**内容签名**两端对账（`[tx]/[rx]/[ae]`）+ reactor 发送序号统计写出字节 | `flushLocked()` 在"队首已完整写出、正在等应答"时**仍会把队首重新装进 out 再发一次**，而事件循环尾部每轮都调用它 → 实测同一帧被完整发送 **2.0 次**（高负载下 44% 的帧重复）→ 对端回两次 → 第二次答复弹掉**下一批**的 Pending → ack 归因错位（reactor 1–3% vs sync **0/855**）→ `matchIndex_` 虚高 → 提交并 ack 了 follower 从未持有的条目 | `awaitingReply` 标志 + 应答后发送统一走 `flushLocked`；回归 **R6**（对"只收不回"的对端等 400ms，断言只收到 1 帧；去掉守卫即 RED） |
| 6 | 清洗后仍有 ~1/10 万写的残留丢失 | 索引级插桩：`[app] idx=` / `[commit] match={...}` 对账 | `awaitCommit()` 的两处成功判断只看 `index <= commitIndex_`。真实序列：leader 追加到 I（未提交）→ 失去领导权 → 新 leader 用**不同条目**覆盖 I → I 被提交（提交的是新条目）→ 本节点 `commitIndex_` 推进后**误报 kOk**，客户端以为写成功 | 成功判断追加 `log_.termAt(index) == term`；被覆盖则回 `kNotLeader` 让客户端重试；回归 **A12**（用"只挡第一次 sync"的 store 把 propose 停在锁外 fsync 窗口里构造该序列，RED=返回 kOk） |

另外修了一个真实崩溃：`ticker` 线程 joinable，而 socket/bind/listen 失败直接 `return 1`
→ 析构 joinable thread → `std::terminate`（实测 bind 冲突时复现）；已加 RAII 守卫。

**可迁移的方法论**（比 bug 本身更值钱）：
1. **把不变量变成可断言的基础设施**：线程局部锁探针（I9）、Spy/Blocking 适配器、帧内容签名。
2. **对照实验定位**：同一场景跑两个引擎（reactor vs sync）比"猜"快得多——1–3% vs 0/855 一眼看出问题在传输层。
3. **RED 优先**：每个修复都先写能失败的用例（A9/A10/A12、R6 都留了 RED 证据）。
4. **负结果也入档**：两次失败尝试（并行扇出、蓄批）都带数据写进文档，避免后人重走。

## 3. 性能结论（含为什么绝对判据被改口径）

* **独立微基准**（`scripts/fsbench_commit_latency.cpp`，4 KiB 追加 + flush ×200）：
  本机单次持久化提交 `fsync 9.67ms / fdatasync 9.74ms / 预分配+fsync 7.93ms / O_DIRECT 7.60ms`
  → **下限 ≈8 ms**。Raft 必须 durable 后才 ack，所以 `p=1 ≤8 ms/写` 等价于要求
  "提交 + 复制往返"不花时间 —— 物理不可达（冻结基线期 M4 自身也只有 16.4 ms/写）。
* **口径改为同机比值**（用户决定，`m5-bench.md` §3.7）：p=1 延迟 ≤1.2×M4；p=8/64 吞吐 ≥0.8×M4；
  每次 `verify missing 0` 为硬门禁。实测（当轮）：**p=1 达标（1.11×）**；p=8/64 **未达标（0.60×/0.57×）**。
  > **后续（P2a，2026-09-20）**：根因改判为**复制发送段排队**（不是本节下文说的"唤醒 + `mu_` 争用"）；
  > 修复后 p=8 **1.34×**、p=64 **2.20×**、p=1 延迟 **1.03×**。见 `m5-bench.md` §3.11 与提交 `ab7bd57`。
* **同一轮 A/B 里 M4 基线丢了 `missing 5`/`missing 9`，而 M5 全绿** —— M5 的墙钟代价换来的是
  零丢写 + 锁内零 fsync + reactor 引擎 + 可观测性。
* **p=8/64 差距的定量定位**（§3.9/§3.10）：单批内部 fsync 8ms + 等 ack 13ms + 发送 14ms + 间隙 3ms
  ≈38 ms；整轮里 **fsync 只占 ~10%**；剩余约 40% 墙钟在"批与批之间"的**唤醒 + 单把全局锁争用**
  （34+ proposer 抢 `mu_`）。M4 之所以快，是因为它把 fsync 关在锁内 —— 那是个**全局串行点**，
  没有 thundering herd。这是"I9（锁内不做 IO）"与"高并发吞吐"的结构性张力，不是调参能解决的。
* ~~**下一步**：改唤醒机制（按批的条件变量/事件计数，把"批成形"从抢锁里拆出来）~~ —— **已由 P2a 取代**：
  实测证明瓶颈在"复制发送段的排队"（`syncInFlight_` 放开过早 → 并发 flusher 抢 `TcpTransport` 全局锁），
  与唤醒机制无关；§3.9 的"并行扇出更差"也不构成反证（那次缺背压、会重复构建 AE）。

## 4. 交付物与验收（当前 HEAD）

| 项 | 状态 |
|---|---|
| 文档 | `m5-design.md`（v1.0→v2.8 修订记录）、`m5-prerequisites.md`（I9–I17/L12–L18）、`m5-bench.md`（§1 冻结基线 → §3.10）、本复盘 |
| 测试 | 单测 **94/94**（M5.A1–A16、R1–R6 等）；TSan **94/94 且 0 报告**（`tests/tsan.supp` 窄抑制 libstdc++ `condition_variable_any` 的 `notify_all`/`wait_until` 两族误报，含出处论证） |
| 脚本 | e2e/fault 运行全 PASS（sync 侧 5 + reactor 侧 4）；P2a 后复跑 `raft_e2e` + `raft_fault/snapshot_fault/membership_fault` 各 10 轮 PASS，A/B 每格 `missing 0` |
| 构建 | 干净重建 0 warning |
| 交付 | tag `m5-performance`（注释写明未达标项与偏差）、已 push、本地 clone 已同步 |
