# raftkv —— Raft-based Distributed Key-Value Store

> 里程碑 M1：单机版 KV + WAL 持久化（当前进度）。Roadmap 见
> [docs/roadmap.md](docs/roadmap.md)。

一个从零实现、面向简历与生产场景的分布式 KV 存储练习项目：
先用 C++ 写出一台**可持久化、可压测、有单元测试与故障测试**的单机 KV，
再在它之上实现 Raft 共识，逐步演进为多节点、可故障切换的存储系统。

## 当前能力（M1）

- 二进制 TCP 协议（长度前缀 + 版本校验 + CRC 防护），支持 `put/get/del`
- 内存 KV（单锁阶段，WAL 与内存顺序一致）
- WAL 追加写 + 每次写 fsync（`--no-sync` 可关闭做压测）
- 启动时 WAL 重放；`kill -9` 后半条记录会被 CRC 检测并截断
- 线程池服务端（accept 线程 + N 个 worker），SIGINT/SIGTERM 优雅退出
- 命令行客户端 + 顺序压测（QPS / avg / p50 / p99）
- 单元测试（存储 / WAL 重放 / 编解码）+ `scripts/e2e.sh` 端到端冒烟与崩溃恢复测试

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
