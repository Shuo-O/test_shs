# Code Walkthrough

这份文档说明当前第一版实现的代码结构、关键数据流和运行方式。它对应 `DEV_MOCK` 配置，可以在 macOS 上直接构建、测试和跑 benchmark。

## 入口

主要入口文件：

```text
main.cpp                 常驻 DemoMd 进程入口
demo.h / demo.cpp        行情写入主类，包含 on_md 热路径
tests.cpp                正确性测试
benchmark.cpp            macOS 本地性能测试
```

运行命令：

```bash
make test
make bench BENCH_ROWS=500000
make all
```

`make test` 会覆盖：

```text
latest-n 查询
未知 instrumentId
ring wrap 后读取
WAL 行数
WAL -> Parquet 转换
Parquet 读回校验
```

## 模块职责

```text
config.h
  编译期容量参数。默认 DEV_MOCK，适合 macOS 本地测试。

shm_layout.h
  共享内存 ABI。所有跨进程可见的数据结构都在这里定义。

shm_manager.h / shm_manager.cpp
  shm_open / mmap 封装，负责创建、打开和清理三段共享内存。

demo.cpp
  实现 DemoMd::on_md。热路径写 GlobalDayLog 和 PerSymbolRing。

strategy_reader.h
  策略侧查询 API：query_latest_n(instrumentId, n)。

storage_tailer.cpp
  后台线程，从 GlobalDayLog 顺序追数据并写 WAL。

scripts/wal_to_parquet.py
  离线把 WAL 转为 Hive-style 分区 Parquet。

scripts/verify_parquet.py
  测试用 Parquet 校验脚本。
```

## 共享内存布局

当前分成三段：

```text
ControlSegment
  ShmHeader
  instrument_to_index[1_000_000]
  RuntimeStatus

RingSegment
  PerSymbolRing[kInstrumentCount]

DayLogSegment
  TickRecord[kLogCapacity]
```

为什么分段：

```text
策略进程只需要 ControlSegment + RingSegment。
StorageTailer 才需要 DayLogSegment。
读者不用映射大 day log，地址空间和 cache 干扰更小。
```

## 序列号语义

代码统一使用 exclusive-end 语义。

```text
header.committed_seq = 全局已发布记录的右开边界
可读全局范围 = [0, committed_seq)
最新全局记录 = committed_seq - 1

ring.header.write_seq = 某只股票已发布记录的右开边界
可读本地范围 = [0, write_seq)
最新本地记录 = write_seq - 1
```

这个约定避免“latest seq”和“next seq”混用导致的漏读。

## on_md 热路径

`DemoMd::on_md` 的主要步骤：

```text
1. instrumentId -> symbol index
2. 分配 global_seq
3. 写 GlobalDayLog[global_seq % kLogCapacity]
4. 写 PerSymbolRing[symbol].slots[local_seq & mask]
5. 发布 ring.write_seq
6. 发布 committed_seq
```

热路径刻意避免：

```text
日志打印
文件 IO
Parquet 编码
动态分配
mutex
hash map
```

当前 demo 在 `get_or_register_instrument` 中支持运行时注册 instrument。真实生产系统更建议启动时一次性加载完整股票列表，避免热路径第一次遇到新股票时产生额外分支。

## Seqlock 读写协议

每个 `TickSlot` 有一个 `version` 字段。

写入：

```text
version = even + 1  // odd，表示写入中
写 local_seq / global_seq / order
version = even      // even，表示稳定
ring.write_seq = local_seq + 1
```

读取：

```text
v1 = version
如果 v1 是 odd，重试
拷贝 payload
v2 = version
如果 v1 == v2 且为 even，说明没有读到半条
再检查 slot.local_seq == expected_seq，防止 ring wrap 后读到新 epoch
```

`query_latest_n` 最后还会重新读取 `write_seq`，检查复制过程中是否发生覆盖。

## WAL 路径

`StorageTailer` 不参与 `on_md` 同步调用。它异步读取：

```text
GlobalDayLog[next_read_seq_ % kLogCapacity]
```

并验证：

```text
record.global_seq == next_read_seq_
```

如果不相等，说明 circular day log 可能已经覆盖未消费记录。

WAL 写入特点：

```text
只保存 09:25-15:00 窗口内记录
64B TickRecord 原样顺序写
DEV_MOCK 中每次 flush 后 fsync
durable_wal_seq 表示 fsync 后的全局 exclusive-end
```

## Parquet 路径

Parquet 转换在 Python 脚本中完成，不在 C++ 热路径中。

输出结构：

```text
parquet_out/date=YYYYMMDD/bucket=xx/part-000000.parquet
```

分桶逻辑：

```text
bucket = instrument_id % buckets
```

脚本会跨 WAL 文件累积同一 bucket 的 rows，达到 `--row-group-rows` 后写出，避免频繁小文件。

## Benchmark

`benchmark.cpp` 做三件事：

```text
1. 预注册 symbols，避免把注册成本计入 steady-state on_md。
2. 注入 BENCH_ROWS 条 09:25-15:00 窗口内数据。
3. 测 on_md、query_latest_n(100)、query_latest_n(1000) 延迟。
```

性能结果记录在：

```text
docs/PERFORMANCE_MACOS.md
```

## 当前边界

当前版本已经完成：

```text
macOS DEV_MOCK 可运行
共享内存 latest-n 查询
WAL 持久化
Parquet 转换和校验
性能 benchmark
```

生产化仍需要：

```text
Linux STANDARD/EXTENDED 配置实测
启动时加载完整证券列表
多进程 reader benchmark
WAL crash recovery 测试
Arrow C++ Parquet writer
CPU 绑核、大页、mlockall、IRQ 隔离
```
