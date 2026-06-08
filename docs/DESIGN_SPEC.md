# 技术方案：A 股实时行情存储与查询系统

---

## 一、硬件环境分层

设计需适配三个环境，所有尺寸参数通过编译期常量隔离。

| 层级 | CPU | 内存 | 存储 | NIC | 目标用途 |
|------|-----|------|------|-----|----------|
| **L0 Dev** | MacBook/笔记本, 4-8 核, 3.0-4.0 GHz | 16-32 GB DDR4 | SATA/NVMe PCIe 3, ~2 GB/s | 普通网卡 | 本地开发，功能验证 |
| **L1 量化基准** | Intel Xeon Gold 6326 / AMD EPYC 7313, 16-32 核, 3.0-3.8 GHz | 128-256 GB DDR4 ECC | PCIe Gen4 NVMe (~5-7 GB/s) | Mellanox ConnectX-5 | 多数量化公司的实际部署 |
| **L2 极速** | AMD EPYC 9575F, 64 核, 5.0 GHz Boost | 512 GB+ DDR5, 12 通道 | PCIe Gen5 NVMe (>10 GB/s) | Solarflare X4 + Onload | 高频/顶级量化公司 |

**关键约束差异：**
- L0：无大页（macOS 不支持 `MAP_HUGETLB`），无 `mlockall`，内存上限 ~2 GiB 用于本系统，无 CPU 隔离
- L1：可用大页（2 MiB），可 `mlockall`，`isolcpus` 可配，DDR4 内存带宽约 50-100 GB/s（2-4 通道）
- L2：1 GiB 大页，全部 OS 调优，DDR5 理论带宽 614 GB/s（12 通道）

---

## 二、数据规模基准

所有设计数字从以下基础数据推导：

```
交易窗口: 09:25–15:00 = 20,100 秒
日总 ticks: 300,000,000
平均吞吐: 300M / 20,100 = 14,925 ticks/s ≈ 15,000 ticks/s
每股均值: 3.0 ticks/s  （300M / 5000 / 20,100）
每股日量: 60,000 ticks
MDUniOrder 大小: 40 B（原始），64 B（cache 对齐内部格式）
```

**吞吐带宽对照表：**

| 场景 | ticks/s | 写带宽（64B/记录） | 说明 |
|------|---------|------------------|------|
| 正常交易 | 15,000 | 1.0 MB/s | 无压力 |
| 集中竞价 | 100,000 | 6.4 MB/s | 开收盘 |
| 设计目标 | 1,000,000 | 64 MB/s | L1 硬件可达 |
| 短期峰值 | 2,000,000 | 128 MB/s | L2 硬件可达 |
| 极限冗余 | 5,000,000 | 320 MB/s | L2 NVMe 可达 |

**L1 量化基准带宽验证：**
- PCIe Gen4 NVMe 顺序写：~5 GB/s >> 128 MB/s（峰值带宽占用仅 2.6%）
- DDR4 内存带宽（双通道）：~50 GB/s >> 320 MB/s（极限带宽占用 0.6%）
- 结论：**带宽不是瓶颈，延迟和 cache 命中是核心问题**

---

## 三、整体架构

```
on_md (单线程, Core 0)
  │
  ├──[O(1) array lookup]──► instrument_to_index[instrumentId] → slot_idx
  │
  ├──[~20 cycles]──► GlobalDayLog[global_seq % 512M]   (顺序写, 64B)
  │   store_release(committed_seq)
  │
  └──[~30 cycles]──► PerSymbolRing[slot_idx].slots[write_seq & 16383]
                      seqlock protocol
                      store_release(ring.write_seq)

StorageTailer (Core 1, 独立线程)
  │ spin-read committed_seq
  ├──► WAL 顺序写 (64B 记录, 256 MB 批次)
  │     filter: 09:25 ≤ recvTime ≤ 15:00
  └──► Parquet 批量转换 (4M rows/批, 独立线程 Core 2-3)

Strategy Reader (Core 4+, 多进程/线程)
  └──► shm_open + mmap → query_latest_n(instrumentId, n)
       seqlock 读协议, O(n) 拷贝
```

**热路径约束（绝对禁止）：**
`malloc` / `free` / `new` / `delete` / `cout` / 文件 IO / socket / syscall / mutex / hash 表查找 / 无界队列 push / Parquet 编码

---

## 四、共享内存布局（精确到字节）

采用 **3 段独立 mmap** 而非单一大段，原因：
1. GlobalDayLog（32 GiB）和 PerSymbolRing（4.88 GiB）对大页粒度要求不同
2. 策略进程只需 read-only 映射 PerSymbolRing，不需要映射 GlobalDayLog（节省地址空间）
3. 故障隔离：一段 mmap 失败不影响其他段

### Segment 1：控制面（约 4 MiB）

```
/dev/shm/tick_ctrl
  ├── ShmHeader          64 B
  ├── instrument_to_index[1,000,000]    3,814 KiB ≈ 3.73 MiB  ← 可完全住 L3 Cache
  └── RuntimeStatus      256 B
  总计: ~3.8 MiB
```

```cpp
struct alignas(64) ShmHeader {
    uint64_t magic;                          // 0xTICKDATA_V1
    uint32_t abi_version;                    // 递增，不匹配拒绝映射
    uint32_t trading_day;                    // YYYYMMDD
    std::atomic<uint64_t> committed_seq;     // 已发布的最新 global_seq
    std::atomic<uint64_t> writer_heartbeat;  // TSC 时间戳，策略据此判断数据源存活
    uint32_t instrument_count;               // 实际活跃股票数
    uint32_t ring_capacity;                  // 16384（方便读者验证）
    uint8_t  _pad[16];
};  // 64 B
```

`instrument_to_index[1,000,000]` 覆盖 A 股所有可能代码（沪 600000-699999/900000+，深 000001-399999），无效代码填 `-1`。

**为何不用 hash map：**
- 数组 3.8 MiB，全部命中 L3 Cache（典型 Xeon Gold L3 = 24 MiB）
- hash map 有碰撞分支 + 指针 chasing，在极端频率下 p99 抖动大
- A 股代码空间 < 1,000,000，直接索引无稀疏浪费问题

### Segment 2：PerSymbolRing（4.88 GiB）

```
/dev/shm/tick_rings
  PerSymbolRingArray[5000]
  每个 PerSymbolRing = 64 B header + 16384 × 64 B slots = 1,048,640 B ≈ 1 MiB
  总计: 5000 × 1,048,640 B = 4,882 MiB ≈ 4.77 GiB
```

**大页配置（L1 量化基准）：**
- 使用 2 MiB 大页：4.88 GiB / 2 MiB = 2,499 页
- 建议在 `/etc/sysctl.conf` 预分配：`vm.nr_hugepages = 2600`（留 100 页余量）

```cpp
struct alignas(64) TickSlot {
    std::atomic<uint64_t> begin_seq;  // 写前置奇数
    MDUniOrder            order;      // 40 B
    uint64_t              global_seq; // 全局序号，方便审计
    std::atomic<uint64_t> end_seq;    // 写后置偶数
};  // 8+40+8+8 = 64 B，完美填满一个 cache line

struct alignas(64) PerSymbolRingHeader {
    std::atomic<uint64_t> write_seq;  // 单调递增，读者据此定位最新数据
    uint32_t              symbol_id;  // instrumentId
    uint32_t              _pad[13];
};  // 64 B

struct PerSymbolRing {
    PerSymbolRingHeader header;
    TickSlot            slots[16384];
};
```

**环形缓冲 overwrite 安全分析：**
```
平均速率下写满 ring 需要: 16384 / 3.0 = 5,461 s ≈ 91 min
10x 峰值速率下写满 ring:  16384 / 30  =   546 s ≈  9 min
读取 1000 条所需时间（均值）: 1000 / 3.0 = 333 s
=> 在任何合理峰值下，读取 latest-1000 都不会触发 overwrite
```

### Segment 3：GlobalDayLog（32 GiB）

```
/dev/shm/tick_daylog
  TickRecord[536,870,912]  (512M 条 × 64 B = 32 GiB)
  1.8× 日容量冗余（300M 条/天）
```

**大页配置（L1 量化基准）：**
- 使用 2 MiB 大页：32 GiB / 2 MiB = 16,384 页
- 推荐使用 1 GiB 大页（如内核支持）：仅需 32 页，TLB 占用极小

```cpp
struct alignas(64) TickRecord {
    uint64_t   global_seq;  // 全局单调序号
    uint64_t   recv_tsc;    // RDTSC 纳秒时间戳
    MDUniOrder order;       // 40 B 原始数据
    uint32_t   crc32;       // 可选校验（L0/L1 可关闭提升性能）
    uint32_t   flags;       // bit0=valid, bit1=in_window, bit2=crossed_midnight
};  // 8+8+40+4+4 = 64 B
```

### 各环境内存使用对照

| 配置 | INSTRUMENT_COUNT | RING_CAPACITY | LOG_CAPACITY | 总占用 | 适用 |
|------|-----------------|---------------|--------------|--------|------|
| **DEV_MOCK** | 256 | 1,024 (2^10) | 16M | ~1.02 GiB | L0 开发 |
| **STANDARD** | 5,000 | 16,384 (2^14) | 512M | ~37.7 GiB | L1 量化基准 |
| **EXTENDED** | 5,000 | 65,536 (2^16) | 512M | ~22.8 GiB rings + 32 GiB log | L2 极速 |

---

## 五、热路径延迟分析

### 延迟预算分解（以 L1 量化基准 @3.5 GHz 为基准）

```
1 cycle = 0.286 ns @ 3.5 GHz
p50 目标: 500 ns = 1,750 cycles
p99 目标: 2,000 ns = 7,000 cycles
```

| 操作 | 预期 cycles | 说明 |
|------|-------------|------|
| `instrument_to_index[id]` 查表 | 4-14 | L1 命中（3.8 MiB，高度重用） |
| 递增 `local_seq`（寄存器） | 1 | 无争用本地计数 |
| 写 `TickRecord` 64 B | 10-50 | L1/L2 命中，顺序写 prefetch 有效 |
| `store_release(committed_seq)` | 5-10 | x86 store+sfence 或 ARM stlr |
| seqlock 写 `TickSlot` 64 B | 10-50 | 依赖 ring slot cache 状态 |
| `store_release(ring.write_seq)` | 5-10 | 同上 |
| **hot path 合计（cache 热）** | **~50-150** | **~14-43 ns，p50 轻松达标** |
| **hot path 合计（cache 冷）** | **~700-1,200** | **~200-343 ns，最坏 p99 达标** |

**cache 冷场景分析：**
- 每个 PerSymbolRing slots 区域 = 1 MiB
- 5000 只股票全部 ring slots = 4.88 GiB >> L3 Cache（Xeon Gold 6326 = 24 MiB L3）
- 写入特定股票时，slot 大概率不在 L3，需要 DDR4 访问（~70 ns = 245 cycles @3.5GHz）
- 但热股（TOP 200 日成交）频繁访问，实际保持在 L3

**关键结论：** 热股 p50/p99 达标无悬念；冷股（3 ticks/s 以下）最坏 p99 约 200-350 ns，仍优于 2 µs 目标。

### on_md 写协议（seqlock）

```
Writer（on_md 线程，单写者）:
  1. slot.begin_seq.store(odd_seq, relaxed)   // begin_seq 奇 → 读者知道写入中
  2. memcpy slot.order = order               // 40B 拷贝
  3. slot.global_seq = global_seq
  4. slot.end_seq.store(odd_seq+1, release)  // end_seq 偶 → 写入完成
  5. ring.header.write_seq.store(write_seq+1, release)

Reader（策略线程）:
  loop:
    b = slot.begin_seq.load(acquire)
    if b & 1 → retry  // 正在写入，自旋
    memcpy local = slot.order
    e = slot.end_seq.load(acquire)
    if b == e → accept  // 未被改写
    else → retry
```

**retry 概率估算（L1 量化基准）：**
- 写入 64B 耗时约 10-50 cycles ≈ 3-14 ns
- 读取 64B 耗时约 10-50 cycles ≈ 3-14 ns
- 10 个策略线程同时读同一 slot 的概率（正常速率）：`(14ns / 333ms) × 10 ≈ 0.04%`
- 不 retry 概率 > 99.96%，对策略延迟无影响

---

## 六、需求 A：实时查询方案

### 策略进程接入

策略进程通过 `shm_open` + `mmap` 只读映射 Segment 1（控制面）和 Segment 2（rings），不需要访问 Segment 3（GlobalDayLog）。

```cpp
// strategy_reader.h —— 对外接口
struct ShmContext {
    ShmHeader*       ctrl;     // Segment 1 映射
    int32_t*         id_index; // instrument_to_index 数组指针
    PerSymbolRing*   rings;    // Segment 2 映射，5000 个 ring
    int32_t          ring_capacity;
};

// 返回 instrumentId 最新 n 条，records 按到达时序排列（oldest first）
// 返回值: 实际返回数量；负值为错误码
//   -1 = ERR_UNKNOWN_INSTRUMENT
//   -2 = ERR_OVERWRITE_DETECTED
//   -3 = ERR_STALE_WRITER
int query_latest_n(const ShmContext* ctx,
                   int32_t           instrumentId,
                   int               n,        // 1 ≤ n ≤ ring_capacity
                   MDUniOrder*       out_buf,  // caller 分配 n × 40B
                   int*              out_count);
```

### 查询实现（O(1) 查表 + O(n) 拷贝）

```
1. idx = ctx->id_index[instrumentId]
   if idx < 0 → return ERR_UNKNOWN_INSTRUMENT

2. ring = &ctx->rings[idx]
   ws = ring->header.write_seq.load(acquire)   // 最新序号

3. actual_n = min(n, min(ws, ring_capacity))   // 可读数量
   start_seq = ws - actual_n

4. for i in [0, actual_n):
     seq = start_seq + i
     slot_idx = seq & (ring_capacity - 1)
     seqlock_read(ring->slots[slot_idx], &out_buf[i])

5. // overwrite 检测：读完后再次检查
   ws2 = ring->header.write_seq.load(acquire)
   if ws2 - start_seq > ring_capacity → return ERR_OVERWRITE

6. return actual_n
```

### 查询延迟估算（L1 量化基准 @3.5 GHz）

| 查询规模 | 数据量 | L3 命中时延 | p99 目标 | 达标？ |
|----------|--------|------------|----------|--------|
| latest-100 | 6,400 B | ~6,400B / 300 GB/s ≈ 21 ns | < 5,000 ns | ✅ |
| latest-1,000 | 64,000 B | 64KB 命中 L3 ≈ 213 ns | < 20,000 ns | ✅ |
| latest-16,384 | 1 MiB | 1 MiB / 300 GB/s ≈ 3,413 ns | 无目标 | 可接受 |

无大页时 TLB miss 额外开销（1000 条）：约 1,000 × 1 page / 16K slots × 5 ns ≈ < 1 ns，可忽略。

---

## 七、需求 B：持久化方案

### 时间窗口过滤

`MDUniOrder.recvTime` 字段类型为 `int32_t`，需在初始化时确认单位：

```cpp
// 若 recvTime 为 hhmmss 整数格式（如 93000 = 09:30:00）
bool in_window = (order.recvTime >= 92500 && order.recvTime < 150000);

// 若 recvTime 为自 epoch 的秒数，则在 ShmHeader 中记录当日 09:25:00 epoch
// 两种情况都在 StorageTailer 中过滤，不在 on_md 中判断
```

实现上同时记录 `recv_tsc`（RDTSC）保留纳秒级延迟分析能力。

### StorageTailer 设计

```
StorageTailer（单线程, Core 1）:
  local_read_seq = 0    // 上次已读到的 global_seq

  loop:
    cmt = header->committed_seq.load(acquire)
    if cmt <= local_read_seq → cpu_relax() / yield()

    batch_end = min(cmt, local_read_seq + WAL_BATCH_SIZE)

    for seq in [local_read_seq, batch_end):
      rec = &daylog[seq % LOG_CAPACITY]
      if rec.flags & FLAG_IN_WINDOW:
        wal_buffer[buf_pos++] = *rec

    if buf_pos >= WAL_FLUSH_ROWS:
      writev(wal_fd, wal_buffer, buf_pos × 64B)
      durable_seq = batch_end
      buf_pos = 0

    local_read_seq = batch_end
    status->backlog = cmt - durable_seq  // 暴露监控指标
    if backlog > ALERT_THRESHOLD:
      status->alert_flags |= ALERT_BACKLOG
```

**backlog 阈值设定依据：**
```
ALERT_THRESHOLD = 1,000,000 条 = 64 MB
理由：NVMe Gen4 5 GB/s 顺序写，追赶 1M 条需 64MB / 5GB/s = 12.8 ms
      若 backlog 持续增大说明存储路径有问题，需人工介入
```

### WAL 文件格式

```
文件命名: wal_YYYYMMDD_NNNNNN.bin  （NNNNNN = 段编号，每段 4M 条）
记录格式: TickRecord（64B，与 GlobalDayLog 相同结构）
每段大小: 4,000,000 × 64B = 244 MiB
日总 WAL: 300M × 64B = 17.88 GiB（不压缩原始二进制）
文件数:   75 段/天（300M / 4M）
```

**为何 WAL 不压缩：**
- 峰值写带宽 128 MB/s << NVMe Gen4 5 GB/s（占用 2.6%）
- LZ4 压缩 CPU 开销：~400 MB/s 处理速度，会消耗 Core 1 约 32% 资源
- 不压缩可保证 Core 1 始终跟上 on_md 写入速度，backlog 不增长

**WAL recovery 流程：**
```
1. 找最后一个 wal_YYYYMMDD_*.bin
2. 从文件末尾向前扫描，找最后一条 CRC 校验通过的记录
3. 截断文件至该记录末尾
4. 从 global_seq 继续追加
```

### Parquet 转换

**依赖：Apache Arrow C++（>= 12.0）**

```
ParquetBuilder（Core 2-3, 独立线程池）:
  监控 WAL 目录，每 4M 行（一个 WAL 段）触发一次转换

  分区路径:
  data/date=YYYYMMDD/bucket={instrumentId % 16}/part-{seq:06d}.parquet

  每批处理流程:
  1. 读取 WAL 段（244 MiB）到内存
  2. 按 instrumentId % 16 分组
  3. 每组构建 Arrow RecordBatch
  4. 写入 Parquet（LZ4 压缩）
  5. 更新 parquet_written_seq 到 RuntimeStatus
```

**批次大小选择依据：**
```
4M 行 × 40B raw = 160 MB
Arrow LZ4 编码吞吐（@3.5GHz 单核）: ~400 MB/s
单批编码时间: 160 MB / 400 MB/s = 400 ms
全天批次数:   75 批
4 核并行总时: 75 × 400ms / 4 = 7.5 s  （可在交易时间内完成）

LZ4 压缩后 Parquet 大小估算:
  int 列整体压缩比约 3-5x
  全天: 75 × (244MiB / 4) ≈ 4.6 GiB（相比 WAL 17.88 GiB，节省 74%）
```

**16 bucket 分区依据：**
```
300M / 16 = 18.75M 行/bucket
18.75M × 40B = 750 MB raw → 压缩后约 150-200 MB/bucket
符合 Parquet row group 推荐 256MB-1GB 范围
避免生成 5000 个小文件（每只股票一文件），减少文件系统压力
```

**Parquet Schema：**

```
global_seq:    UINT64, PLAIN
recv_tsc:      UINT64, DELTA_BINARY_PACKED （相邻时间差小，压缩好）
instrument_id: INT32,  RLE_DICTIONARY      （仅 5000 值，字典极小）
type:          INT8,   RLE
bs_flag:       INT8,   RLE
tick_type:     INT8,   RLE
channel:       INT16,  RLE_DICTIONARY
n_tp:          INT32,  DELTA_BINARY_PACKED
recv_time:     INT32,  DELTA_BINARY_PACKED
price:         INT32,  DELTA_BINARY_PACKED （同一股票价格变化小）
qty:           INT32,  PLAIN + LZ4
biz_seq:       INT32,  DELTA_BINARY_PACKED
order_seq:     UINT32, DELTA_BINARY_PACKED
order_id:      UINT32, PLAIN
```

---

## 八、Mock 开发环境设计

### 编译期配置隔离

```cpp
// config.h  —— 所有量化参数的唯一来源
#if defined(DEV_MOCK)
  constexpr int32_t  kInstrumentCount  = 256;
  constexpr int32_t  kIdArraySize      = 1'000'000;  // 保持真实大小（仅 3.8 MiB）
  constexpr int32_t  kRingCapacity     = 1'024;       // 2^10
  constexpr int64_t  kLogCapacity      = 16'000'000;  // 16M 记录 = 1 GiB
  constexpr int32_t  kWalBatchRows     = 4'096;
  constexpr int32_t  kParquetBatchRows = 100'000;
  constexpr bool     kUseHugePages     = false;
  constexpr bool     kUseMlockall      = false;
  constexpr bool     kUseCrc           = false;
  constexpr int64_t  kAlertBacklog     = 100'000;

#elif defined(STANDARD)
  constexpr int32_t  kInstrumentCount  = 5'000;
  constexpr int32_t  kIdArraySize      = 1'000'000;
  constexpr int32_t  kRingCapacity     = 16'384;      // 2^14
  constexpr int64_t  kLogCapacity      = 536'870'912; // 512M
  constexpr int32_t  kWalBatchRows     = 1'000'000;
  constexpr int32_t  kParquetBatchRows = 4'000'000;
  constexpr bool     kUseHugePages     = true;        // 2 MiB pages
  constexpr bool     kUseMlockall      = true;
  constexpr bool     kUseCrc           = false;       // 关闭降低热路径开销
  constexpr int64_t  kAlertBacklog     = 1'000'000;

#elif defined(EXTENDED)
  constexpr int32_t  kInstrumentCount  = 5'000;
  constexpr int32_t  kIdArraySize      = 1'000'000;
  constexpr int32_t  kRingCapacity     = 65'536;      // 2^16
  constexpr int64_t  kLogCapacity      = 536'870'912;
  constexpr int32_t  kWalBatchRows     = 4'000'000;
  constexpr int32_t  kParquetBatchRows = 16'000'000;
  constexpr bool     kUseHugePages     = true;        // 1 GiB pages
  constexpr bool     kUseMlockall      = true;
  constexpr bool     kUseCrc           = true;
  constexpr int64_t  kAlertBacklog     = 4'000'000;
#endif
```

**各配置内存占用：**
```
DEV_MOCK:
  控制面:  3.8 MiB
  Rings:   256 × 1024 × 64B = 16 MiB
  DayLog:  16M × 64B = 1,024 MiB
  合计:    ~1.04 GiB  ← 16 GB 笔记本完全可用

STANDARD:
  控制面:  3.8 MiB
  Rings:   5000 × 16384 × 64B = 4,882 MiB ≈ 4.77 GiB
  DayLog:  512M × 64B = 32 GiB
  合计:    ~37.7 GiB  ← 需要 128 GB+ 机器
```

### macOS 兼容性处理

| 功能 | Linux | macOS 替代 | 影响 |
|------|-------|-----------|------|
| `shm_open` + `mmap` | ✅ | ✅ | 无 |
| `MAP_HUGETLB` | ✅ | ❌ → `MAP_ANONYMOUS` fallback | TLB miss 略增，Dev 可忽略 |
| `mlockall` | ✅ | `mlock` per region（需 `ulimit -l unlimited`）| 可能偶发 page fault |
| `rdtsc` | ✅ | `mach_absolute_time()` 替代 | 时钟分辨率 ns 级，可用 |
| `O_DIRECT` WAL | ✅ | ❌ → 普通 `fwrite` + `fsync` | IO 路径有 page cache，Dev 无影响 |
| `pthread_setaffinity_np` | ✅ | ❌ → 编译期 `#ifdef __linux__` 屏蔽 | 无 CPU 绑核，接受调度噪声 |

### Mock 数据生成器（mock_feeder）

```cpp
// 三种注入模式，通过命令行参数选择：
//   --steady:  15,000 ticks/s，5000 只股票均匀分布，模拟正常交易
//   --peak:    1,000,000 ticks/s 持续 10 秒后恢复 steady，验证峰值不阻塞
//   --replay <wal_file>:  从 WAL 文件回放真实数据，验证 WAL 格式正确性

// 时间注入：
//   on_md 调用前设置 order.recvTime = 092500 起步，按速率递增
//   保证 StorageTailer 窗口过滤逻辑可测
```

---

## 九、文件结构与接口定义

```
├── config.h               编译期参数（DEV_MOCK / STANDARD / EXTENDED）
├── md.h                   MDUniOrder（不改动）
├── shm_layout.h           所有 POD 结构体定义，共享内存布局常量
├── shm_manager.h          ShmContext 结构 + open/close API
├── shm_manager.cpp        shm_open/ftruncate/mmap 封装 + 大页逻辑
├── demo.h                 DemoMd 类声明（扩展接口）
├── demo.cpp               热路径 on_md 实现
├── strategy_reader.h      query_latest_n API（纯头文件）
├── storage_tailer.h       StorageTailer 类声明
├── storage_tailer.cpp     WAL 写入 + Parquet 批量转换
├── mock_feeder.h          MockFeeder 类声明
├── mock_feeder.cpp        测试数据生成器
├── benchmark.cpp          rdtsc 延迟测量，输出 p50/p99/p999
├── main.cpp               初始化共享内存，启动 tailer 线程
└── Makefile               多目标：mock / standard / extended / bench
```

**Makefile 关键目标：**

```makefile
CXX      := g++
CXXFLAGS := -std=c++17 -Wall -Wextra

mock:
	$(CXX) $(CXXFLAGS) -O2 -DDEV_MOCK \
	    demo.cpp shm_manager.cpp storage_tailer.cpp mock_feeder.cpp main.cpp \
	    -lpthread -o tick_mock

standard:
	$(CXX) $(CXXFLAGS) -O3 -march=native -DSTANDARD \
	    demo.cpp shm_manager.cpp storage_tailer.cpp main.cpp \
	    -lpthread -larrow -lparquet -o tick_server

bench:
	$(CXX) $(CXXFLAGS) -O3 -march=native -DSTANDARD \
	    benchmark.cpp demo.cpp shm_manager.cpp \
	    -lpthread -o tick_bench

clean:
	rm -f tick_mock tick_server tick_bench
```

---

## 十、线程与核心分配

### L1 量化基准（16-32 核）推荐分配

```
Core 0 (isolated): on_md 写入线程
  - isolcpus / nohz_full / rcu_nocbs 配置
  - 禁用 SMT（hyperthreading 对该核）
  - 绑核：pthread_setaffinity_np

Core 1 (isolated): StorageTailer WAL 写入线程
  - spin 读 committed_seq，不 sleep
  - 禁用 SMT

Core 2-3: Parquet 批量转换线程池
  - 非实时，可浮动

Core 4+: 策略进程/线程
  - read-only 映射 Segment 1 + 2
  - 不需要隔离核
```

### L0 Dev（无隔离核）

```
全部线程浮动在 OS 调度上
接受 p99 抖动（可能 10-100 µs）
仅验证功能正确性，不做性能评估
```

---

## 十一、OS 调优清单（L1 量化基准）

```bash
# CPU governor
echo performance > /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor

# 禁用 C-state 深睡眠（减少唤醒延迟）
echo 1 > /sys/devices/system/cpu/cpu0/cpuidle/state*/disable

# 大页预分配（2 MiB）
echo 19000 > /proc/sys/vm/nr_hugepages   # rings + daylog

# 或 1 GiB 大页
echo 37 > /proc/sys/kernel/nr_hugepages_hugepagesz_1073741824

# 核心隔离（/etc/default/grub）
GRUB_CMDLINE_LINUX="isolcpus=0,1 nohz_full=0,1 rcu_nocbs=0,1"

# NIC IRQ 绑核（远离 Core 0-1）
service irqbalance stop
# 手动将 NIC IRQ 绑到 Core 8+

# 内存锁定
ulimit -l unlimited  # 或在 /etc/security/limits.conf 配置
```

---

## 十二、可观测性指标

```cpp
struct alignas(64) RuntimeStatus {
    // 序列号进度
    std::atomic<uint64_t> committed_global_seq;   // on_md 已写入
    std::atomic<uint64_t> durable_wal_seq;         // WAL 已落盘
    std::atomic<uint64_t> parquet_written_seq;     // Parquet 已写入
    uint64_t              storage_backlog;          // committed - durable

    // 性能统计
    std::atomic<uint64_t> total_ticks_received;
    std::atomic<uint64_t> total_ticks_in_window;
    std::atomic<uint64_t> reader_retry_count;      // seqlock retry 次数
    std::atomic<uint64_t> ring_overwrite_count;    // 读者检测到的 overwrite

    // 健康标志
    std::atomic<uint64_t> writer_heartbeat_tsc;    // 最近 on_md 调用的 TSC
    uint32_t              alert_flags;              // bit0=backlog, bit1=seq_gap
    uint32_t              _pad[11];
};  // 64 B 对齐
```

---

## 十三、验收标准

### 功能验收

| 项目 | 验证方法 | Pass 条件 |
|------|----------|-----------|
| latest-n 查询正确性 | 单线程注入 N 条，query n，比对内容 | 完全一致，oldest-first 顺序 |
| seqlock 一致性 | 10 reader + 1 writer 并发 60s | 无 torn read，无数据损坏 |
| overwrite 检测 | 写满 ring+1，query 旧起始位置 | 返回 `ERR_OVERWRITE` |
| WAL 行数 | 注入 X 条 09:25-15:00 数据 | WAL 行数 == X，无重复，global_seq 连续 |
| Parquet 行数 | WAL → Parquet 转换 | Parquet 行数 == WAL 行数 |
| WAL recovery | 模拟进程崩溃，重启后继续 | 无记录丢失，无重复写入 |
| 跨进程查询 | 两个独立进程读同一 shm | 两进程结果一致 |

### 性能验收（L1 量化基准基线）

| 指标 | 测量方法 | Pass 条件 |
|------|----------|-----------|
| on_md p50 | rdtsc 插桩，100M 样本 | < 500 ns |
| on_md p99 | 同上 | < 2,000 ns |
| on_md p99.9 | 同上 | < 5,000 ns |
| query_latest_n(100) p99 | rdtsc，10M 次调用 | < 5,000 ns |
| query_latest_n(1,000) p99 | rdtsc，1M 次调用 | < 20,000 ns |
| 持续 1M ticks/s 吞吐 | mock_feeder --peak 持续 60s | on_md 无阻塞，backlog < 1M 条 |
| WAL 落盘延迟 p99 | durable_seq 跟踪 | < 5 ms |

---

## 十四、主要设计决策与取舍

| 决策 | 选择 | 放弃 | 数据依据 |
|------|------|------|----------|
| 仪器 ID 查找 | 直接数组 `[1M]` | hash map | 数组 3.8 MiB 住 L3，hash 碰撞导致 p99 抖动 |
| 多读者同步 | seqlock | rwlock/mutex | mutex 阻塞写入；seqlock retry 概率 < 0.04% |
| GlobalDayLog | 单一大数组（顺序写） | 队列/链表 | 顺序写最大化 NVMe 带宽；链表有 pointer chasing |
| 共享内存分段 | 3 段独立 mmap | 单一大段 | 策略进程只需映射 ~5 GiB，而非 ~38 GiB |
| Parquet 写入时机 | 异步批量（4M 行） | 实时逐行 | 逐行 Parquet 写入 ~10 µs/row，阻断 on_md |
| WAL 是否压缩 | 不压缩（raw binary） | LZ4 | 峰值 128 MB/s << NVMe 5 GB/s，压缩 CPU 不值得 |
| 大页粒度（L1） | 2 MiB | 1 GiB | 1 GiB 大页需内核特殊配置，通常量化公司未预置 |
| recvTime 处理 | 原始字段过滤 + TSC 补充 | 只用 recvTime | TSC 提供 ns 级延迟分析，recvTime 精度待确认 |
| Parquet 库 | Apache Arrow C++ | DuckDB | Arrow 是 Parquet 标准实现，列式编码控制更精细 |
