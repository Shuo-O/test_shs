# 代码解说（core 精简分支）

> 这是为讲解和理解而保留的**最小核心实现**，只覆盖题目两个需求的本质机制，去掉了
> 监控计数、心跳、容错降级、大页、跨架构时钟、性能化的批量发布等工程化外围代码。
> 这些外围在别的分支里有，面试时可以作为"还能怎么加固/优化"的展开点。

---

## 一、题目与设计思路

`DemoMd::on_md(const MDUniOrder&)` 会以极高频率持续推送 A 股逐笔行情
（约 5000 只股票，全天约 3 亿条）。要实现两件事：

- **需求 A（实时查询）**：多个策略线程/进程能按 `instrumentId` 低时延读取该股票**最新 n 条**逐笔。
- **需求 B（持久化）**：把 09:25–15:00 的全部逐笔落盘，并转成 Parquet。

### 三条核心设计原则

1. **热路径只写内存、发布序号**。`on_md` 里绝不做 malloc / 加锁 / 文件 IO / 系统调用。
   一切 IO、编码、压缩都丢到后台线程。
2. **查询和持久化用两套独立的数据结构**，各自最优：
   - 查询要的是"按股票取最新 n 条" → **每只股票一个定长环形缓冲**（seqlock 保护）。
   - 持久化要的是"全市场有序流" → **一条全局 append-only 日志**，后台顺序刷盘。
3. **跨进程共享用共享内存 + 纯 POD 布局**。策略进程只读映射，零拷贝、零 RPC。

### 为什么这么选（关键决策的依据）

| 决策 | 选择 | 放弃 | 依据 |
|---|---|---|---|
| id→下标 | 直接数组 `index[1e6]` | 哈希表 | A 股代码 < 100 万，数组仅 3.8 MiB 常驻 L3，查表是 O(1) 一次 load，无哈希碰撞抖动 |
| 多读者同步 | seqlock（无锁） | 读写锁/互斥锁 | 锁会阻塞写者（写者是热路径）；seqlock 写者永不等读者，读者偶尔重读 |
| 槽大小 | 64 字节对齐 | 紧凑 40 字节 | 一个 cache line 一条记录，避免 false sharing 与跨行撕裂 |
| 持久化结构 | 单条大数组顺序写 | 队列/链表 | 顺序写最大化 NVMe 带宽，无指针追逐 |
| Parquet | 后台批量（离线脚本） | 实时逐行写 | 逐行写 Parquet ~10µs/行，会卡死 on_md |

---

## 二、总体架构

```
                      ┌─────────────────────────────────────────┐
   行情源 ──on_md──►  │  DemoMd（摄入，单写线程，热路径）           │
                      │   ① 写全局日志   ② 写本股票环    ③ 发布序号 │
                      └──────────┬───────────────────┬────────────┘
                                 │                   │
                writer.committed_seq            rings[idx]（seqlock）
                                 │                   │
            ┌────────────────────▼──────┐   ┌────────▼─────────────────┐
            │ StorageTailer（后台线程）  │   │ 策略进程/线程（只读映射） │
            │  读日志→过滤窗口→批量WAL    │   │  query_latest_n(id, n)    │
            │  →fsync→发布 durable_seq    │   │  → 最新 n 条              │
            └───────────┬───────────────┘   └──────────────────────────┘
                        │
                 WAL (*.bin, 64B/条)
                        │  scripts/wal_to_parquet.py（离线）
                        ▼
                 Parquet（date=…/bucket=…）
```

### 三段共享内存（`shm_open` + `mmap`，独立映射）

| 段 | 内容 | 谁映射 |
|---|---|---|
| `/md_ctrl` | 超级块 + `index[1e6]` + 两个游标 | 写者读写、策略只读 |
| `/md_rings` | `rings[N]`，每只股票一个 seqlock 环 | 写者读写、策略只读 |
| `/md_daylog` | 全局 append-only 日志 | 写者写、tailer 读（策略**不**映射） |

分三段的好处：策略进程只需映射前两段（生产约 5 GiB），不必映射 32 GiB 的日志。

### 线程模型

- **摄入线程**：调用 `on_md`，是唯一的写者（single-writer 假设，所以环和序号都不需要 CAS）。
- **Tailer 线程**：后台 spin 读 `committed_seq`，批量刷 WAL。
- **策略线程/进程**：任意多个，只读，走 `query_latest_n`。

---

## 三、核心机制详解

### 1）id → 下标：直接查表

`ControlRegion.index[1,000,000]`，未注册为 `-1`。`on_md` 里一次数组 load 拿到该股票
在 `rings[]` 的下标，O(1) 且无分支预测抖动。首次见到某股票时 `register_instrument`
分配一个下标（冷路径，全天只发生 ~5000 次）。

### 2）每股票 seqlock 环（需求 A 的核心）

每只股票一个定长环 `Ring { RingHead head; Slot slots[capacity]; }`，
`capacity` 是 2 的幂，用 `pos & (capacity-1)` 取下标。

**槽的巧妙设计**——`Slot.seq` 一个字段同时承担两个职责：

```cpp
struct alignas(64) Slot {
    std::atomic<uint64_t> seq;   // 既是 seqlock 见证，又编码"这个槽现在装的是第几条"
    MDUniOrder            order; // 40B 负载
    uint8_t               _pad[16];
};
```

约定 ring 位置 `p` 的稳定见证值 `= 2*(p+1)`（偶数）；写入中 `= 2*(p+1)-1`（奇数）。
因为偶数值**编码了位置**，读者即便在环回绕后也能判断"这个槽是不是我要的那条"，
不需要额外存 `global_seq`。

**写协议**（`demo.cpp::on_md`，单写者）：
```cpp
uint64_t pos = head.write_seq;            // 本股票下一个写位置
Slot& s = slots[pos & (cap-1)];
uint64_t stable = 2*(pos+1);
s.seq.store(stable-1, relaxed);           // ① 置奇：宣告"正在写"
atomic_thread_fence(release);             // ② 屏障：负载写不能跑到奇标记之前
s.order = order;                          // ③ 写 40B 负载
s.seq.store(stable, release);             // ④ 置偶：发布（与读者 acquire 配对）
head.write_seq.store(pos+1, release);     // ⑤ 推进本股票写序号
```

**读协议**（`strategy_reader.h::query_latest_n`）：
```cpp
uint64_t ws = head.write_seq;             // 最新写序号
n = min(n, min(ws, cap));  start = ws - n;// 可读区间 [start, ws)
for 每个 pos in [start, ws):
  for(;;){
    s1 = slot.seq.load(acquire);
    if (s1 != want(pos)) {                // want = 2*(pos+1)
       if (偶数) → 已被新数据覆盖，返回 kErrOverwritten;
       else       → 写者正在写，自旋重试;
    }
    copy = slot.order;
    atomic_thread_fence(acquire);          // 负载读不能滑到第二次读 seq 之后
    if (slot.seq.load(relaxed) == s1) { out = copy; break; }  // 稳定，接受
    // 否则期间被写，重试
  }
最后再查一次 write_seq，若写者已绕过 start → kErrOverwritten;
```

要点：
- **single-writer** → 写序号、环槽都不需要 CAS。
- 返回**到达顺序**（oldest-first）；可读不足 n 就返回实际条数；n 超过容量按容量截断。
- **撕裂检测**：奇偶 + 见证值不匹配；**覆盖检测**：见证值变大 / 末尾 write_seq 绕过 start。
- 内存序：写者 `release` 与读者 `acquire` 配对；弱内存序（ARM）下两个 `fence` 不可少，
  x86（TSO）上 fence 编译成空操作，零开销。

### 3）全局日志 → WAL → Parquet（需求 B 的核心）

**`on_md` 里只多做一步**：把记录追加进全局日志，并发布 `committed_seq`：
```cpp
LogRecord& rec = log->records[seq & (kLogCapacity-1)];
rec.global_seq = seq; rec.order = order;
rec.flags = kFlagValid | (in_window ? kFlagInWindow : 0);   // 09:25–15:00 标记
writer.committed_seq.store(seq+1, release);                  // 发布给 tailer
```
窗口判断用的是 `order.recvTime`（hhmmss），不是系统时钟。

**Tailer 线程**（`storage_tailer.cpp`）：
```
spin 读 committed_seq；处理 [read_seq, committed) 区间的记录：
  - rec.global_seq != read_seq → 槽已被环覆盖（极端落后才会），跳过
  - 命中 09:25–15:00 → 进缓冲
  - 缓冲满 kWalFlushRows → write() 一批 + fsync()，然后 durable_seq = read_seq
  - 空闲时也把零头刷掉，保证低峰期延迟有界
```
`durable_seq` 只在 **fsync 之后**推进——它代表"已落盘"，与"已读取"区分开。

**WAL → Parquet**：`scripts/wal_to_parquet.py` 离线把 64B 定长 WAL 解析出来，
按 `instrumentId % 16` 分桶，写成 `date=YYYYMMDD/bucket=NN/part-*.parquet`。

---

## 四、逐文件功能

| 文件 | 作用 |
|---|---|
| `md.h` | 题目给定的 `MDUniOrder`（40 字节 ABI），不可改。 |
| `config.h` | 编译期尺寸常量。默认小尺寸（笔记本可跑）；`make PROD=1` 切到生产尺寸（5000/16384/512Mi）。 |
| `shm_layout.h` | 共享内存里所有 POD 结构：`Superblock`、两个游标、`Slot`/`Ring`、`LogRecord`，以及 `in_trading_window`、`stable_seq_for` 等小工具。**整套 ABI 的唯一定义处。** |
| `shm_manager.{h,cpp}` | `shm_open`/`ftruncate`/`mmap` 的封装：`create`（写者建段并初始化）、`open`（策略只读挂载，校验 magic/版本）、`close`。 |
| `demo.{h,cpp}` | `DemoMd`——摄入侧。`start/stop` 拉起/收尾 tailer；`on_md` 是热路径（查表→写日志→写环→发布序号）；`register_instrument`/`resolve` 管 id→下标。 |
| `strategy_reader.h` | 纯头文件的查询 API：`query_latest_n(mapping, id, n, out, stats?)`。策略进程内联它。`QueryStats` 是调用方自己持有的计数（只读映射不能写共享内存）。 |
| `storage_tailer.{h,cpp}` | `StorageTailer`——持久化侧后台线程：读全局日志→按窗口过滤→批量写 WAL→fsync→推进 `durable_seq`。 |
| `main.cpp` | 常驻进程入口：建共享内存、起 tailer、挂到信号上等退出。 |
| `tests.cpp` | 两个测试，兼作演示：`test_functional`（latest-n 正确性 + 环回绕 + WAL 窗口计数）、`test_concurrent`（1 写 4 读，用"序号编码进多字段"做撕裂检测）。 |
| `scripts/wal_to_parquet.py` | 离线把 WAL 转 Parquet，按 `id%16` 分桶分区。 |
| `Makefile` | `make`（建 server）、`make test`（建并跑测试）、`make PROD=1`（生产尺寸编译）。 |

---

## 五、构建与运行

```bash
make test          # 编译并跑功能 + 并发测试（默认小尺寸，笔记本可跑）
make               # 编译常驻 server（小尺寸）
make PROD=1        # 用生产尺寸编译（需 ~5–37 GiB 内存才能真正运行）

# 需求 B 离线转换（需 pip install pyarrow）：
python3 scripts/wal_to_parquet.py wal parquet_out --trading-day 20260608
```

预期：
```
functional: ok
concurrent: ~30万+ consistent reads, overwrites=...    # 0 次撕裂
All tests passed
```

---

## 六、这个精简版刻意省掉了什么（面试可展开）

为聚焦核心，下列工程化内容在本分支被移除，但都是真实生产需要的，可作为"如何加固"的讨论点：

- **可观测性**：吞吐/重试/覆盖计数器、写者心跳（liveness）、读者侧 `QueryStats` 已保留接口。
- **持久化加固**：WAL 故障 fail-closed、durable 落后告警、CRC 校验、崩溃后按 WAL 回放重建。
- **延迟优化**：热路径原始时钟戳（rdtsc/cntvct 替代 steady_clock，省 ~10ns/tick）、
  统计量周期性发布以避开竞争缓存行、查询端**分块批量 fence**把 latest-1000 的
  p99 从 ~22µs 降到 ~2µs（ARM 上；x86 因 fence 是空操作本就达标）。
- **部署调优**：大页（`MADV_HUGEPAGE`）、绑核/隔核、NUMA 单路、`mlockall`。

> 一句话：本分支是"把两个需求讲清楚"的最小骨架；性能关键路径（seqlock 协议、
> cache line 对齐、热/冷路径分离、共享内存零拷贝）都完整保留，不影响功能与核心性能特征。
