# Technical Requirements

## 1. Problem Scope

The system receives simulated real-time A-share tick-by-tick order data through `DemoMd::on_md`.

It must support two functions:

1. Multiple strategy threads or processes can query the latest `n` ticks for a given `instrumentId` with low latency.
2. All ticks received between 09:25 and 15:00 must be persisted and converted to Parquet.

The design should prioritize performance and low tail latency.

## 2. Data Scale

Known input scale:

```text
Number of instruments: about 5,000
Daily tick count: about 300,000,000
sizeof(MDUniOrder): 40 bytes
Raw daily payload: 300,000,000 * 40B = 12,000,000,000B ~= 11.18 GiB
```

For internal storage, use 64-byte aligned records:

```text
Internal record size: 64 bytes
Daily aligned data: 300,000,000 * 64B = 19.2GB ~= 17.88 GiB
```

Throughput target:

```text
Average throughput: about 15,000-20,000 ticks/s
Design peak: >= 1,000,000 ticks/s
Short burst target: >= 2,000,000 ticks/s
Extreme headroom: 5,000,000 ticks/s
```

Peak write bandwidth based on 64-byte records:

```text
1M ticks/s = 64 MB/s
2M ticks/s = 128 MB/s
5M ticks/s = 320 MB/s
```

## 3. Hardware Assumptions

Assume a high-performance quantitative trading server.

Recommended baseline:

```text
CPU: single-socket high-frequency server CPU
Memory: 512GB+ DDR5
Storage: PCIe Gen5 enterprise NVMe
Network: low-latency NIC with kernel bypass
Optional: FPGA for extreme latency paths
```

Example performance-oriented setup:

```text
CPU: AMD EPYC 9575F
- 64 cores / 128 threads
- max boost up to 5.0GHz
- all-core boost about 4.5GHz
- 12 DDR5 channels
- per-socket memory bandwidth about 614 GB/s
- PCIe 5.0

NIC: AMD Solarflare X4 series
- sub-microsecond networking target
- kernel bypass support through Onload

Storage: PCIe Gen5 enterprise NVMe
- single-drive sequential write can reach GB/s level
```

Prefer single socket for the hot path to avoid cross-socket NUMA latency.

## 4. Real-Time Query Requirements

Strategies must be able to:

```text
Input: instrumentId, n
Output: latest n MDUniOrder records for that instrument
```

Functional requirements:

```text
Support multiple strategy threads.
Support multiple strategy processes through shared memory.
Return records in arrival order.
If available records < n, return all available records.
If n > ring capacity, return at most ring capacity records.
Detect overwritten data when reader is too slow.
```

Recommended maximum query depth:

```text
max_n = 16,384
```

Complexity requirements:

```text
instrumentId lookup: O(1)
latest n query: O(n)
write per tick: O(1)
```

Latency targets:

```text
on_md p50 < 0.5 us
on_md p99 < 2 us
on_md p99.9 < 5 us
Read latest 100 ticks p99 < 5 us
Read latest 1,000 ticks p99 < 20 us
```

These are design targets and must be validated by benchmark.

## 5. Real-Time Memory Layout

Use a fixed shared-memory layout.

Top-level layout:

```text
Header
InstrumentIdIndex
PerSymbolRing[5000]
GlobalDayLog
RuntimeStatus
```

Do not put process-local objects in shared memory:

```text
No std::vector
No std::unordered_map
No std::string
No raw process-local pointers
```

Only use:

```text
POD structs
fixed arrays
integer offsets
atomic sequence numbers
cache-line aligned records
```

## 6. Instrument Mapping

Use direct array lookup instead of hash map.

For A-share style numeric IDs:

```text
int32_t instrument_to_index[1,000,000]
Memory: 1,000,000 * 4B ~= 3.8 MiB
```

Lookup:

```text
symbol_index = instrument_to_index[instrumentId]
```

Invalid instruments should map to `-1`.

## 7. Per-Symbol Ring Buffer

Each instrument owns one fixed-size ring buffer.

Recommended configuration:

```text
symbol_count = 5,000
ring_capacity = 16,384
slot_size = 64B
```

Memory usage:

```text
5,000 * 16,384 * 64B = 5,242,880,000B ~= 4.88 GiB
```

Ring rules:

```text
Capacity must be power of two.
slot_index = sequence & (capacity - 1)
Single writer per ring.
Multiple readers allowed.
```

Recommended slot:

```cpp
struct alignas(64) TickSlot {
    std::atomic<uint64_t> begin_seq;
    MDUniOrder order;
    uint64_t global_seq;
    std::atomic<uint64_t> end_seq;
};
```

Use a seqlock-like protocol:

```text
Writer:
1. write begin_seq as odd
2. copy order and global_seq
3. write end_seq as even
4. publish ring write_seq

Reader:
1. read begin_seq
2. copy order
3. read end_seq
4. accept only if begin_seq == end_seq and even
5. otherwise retry
```

## 8. Global Day Log

Maintain a full-day append-only memory log.

Purpose:

```text
Provide ordered full-market data.
Serve storage tailers.
Support replay and diagnostics.
Avoid blocking on disk in on_md.
```

Recommended capacity:

```text
capacity = 512M records
record_size = 64B
memory = 512M * 64B = 32 GiB
```

This covers:

```text
512M / 300M = 1.7x daily expected volume
```

Recommended record:

```cpp
struct alignas(64) TickRecord {
    uint64_t global_seq;
    uint64_t recv_tsc;
    MDUniOrder order;
    uint32_t crc;
    uint32_t flags;
};
```

Write path:

```text
1. Increment local global sequence.
2. Write TickRecord into GlobalDayLog.
3. Publish committed_seq with release semantics.
```

## 9. on_md Hot Path Requirements

`DemoMd::on_md` may only do:

```text
instrumentId -> symbolIndex lookup
write GlobalDayLog
write PerSymbolRing
publish atomic sequence numbers
return
```

Forbidden in hot path:

```text
malloc/free
new/delete
std::cout
logging
file IO
Parquet encoding
socket IO
syscall
mutex contention
hash table lookup
unbounded queue push
```

At 5GHz CPU:

```text
1M ticks/s => 5,000 cycles/tick
2M ticks/s => 2,500 cycles/tick
5M ticks/s => 1,000 cycles/tick
```

Target hot path budget:

```text
<= 300-500 cycles/tick for memory publication
```

## 10. Inter-Process Sharing

For strategy processes, use shared memory:

```text
shm_open
ftruncate
mmap
```

Shared-memory requirements:

```text
Fixed versioned layout.
Magic number and ABI version in header.
Trading day in header.
Writer heartbeat.
Committed global sequence.
Per-symbol write sequence.
Read-only mapping for strategy processes when possible.
```

Reader failure behavior:

```text
If requested range was overwritten, return available part or error code.
If writer heartbeat is stale, report data source stale.
If trading_day mismatch, reject mapping.
```

## 11. Persistence Requirements

All records between 09:25 and 15:00 must be persisted.

Persistence path:

```text
GlobalDayLog -> StorageTailer -> WAL -> Parquet
```

`on_md` must not directly write Parquet.

WAL requirements:

```text
Append-only
Sequential write
Contains global_seq
Contains receive timestamp
Contains full MDUniOrder payload
Optional checksum
Recoverable after crash
```

Recommended WAL record size:

```text
64B
```

Daily WAL size:

```text
300M * 64B = 19.2GB ~= 17.88 GiB
```

Storage tailer behavior:

```text
Read GlobalDayLog by increasing global_seq.
Write large sequential batches.
Track durable_seq.
Expose backlog = committed_seq - durable_seq.
Alert if backlog exceeds threshold.
```

If storage falls behind:

```text
Do not block on_md.
Prioritize WAL over Parquet.
Raise backlog alert.
Defer Parquet generation if necessary.
```

## 12. Parquet Requirements

Parquet generation must be batched.

Do not:

```text
Write one row at a time.
Create one file per tick.
Create excessive small files.
Run Parquet encoding in on_md.
```

Recommended configuration:

```text
Batch size: 4M-16M rows
Row group size: 512MB-1GB
Compression: LZ4 or Snappy for speed
Archive compression: ZSTD if disk saving is more important
```

Partitioning:

```text
date=YYYYMMDD / bucket=instrumentId % 16 / part-xxxxx.parquet
```

With 16 buckets:

```text
300M / 16 = 18.75M rows per bucket
18.75M * 40B = 750MB raw payload per bucket
```

This matches the target Parquet row group size range.

Recommended Parquet schema:

```text
global_seq: uint64
recv_time: int32 or int64
instrument_id: int32
type: int8
bs_flag: int8
tick_type: int8
channel: int16
nTP: int32
price: int32
qty: int32
biz_seq: int32
order_seq: uint32
order_id: uint32
```

## 13. Thread and Core Allocation

Suggested core allocation on a single-socket machine:

```text
Core 0: market data ingest / on_md writer
Core 1: WAL tailer
Core 2: second WAL or replication tailer
Core 3-6: Parquet batch builder / converter
Core 7+: strategy processes
```

Hot-path cores should be isolated.

## 14. OS Tuning Requirements

Required tuning for performance-oriented deployment:

```text
CPU governor = performance
Disable deep C-states
Disable SMT on hot cores
Pin hot threads to dedicated cores
Pin NIC interrupts or polling threads
Pin storage IRQs away from hot cores
Use mlockall(MCL_CURRENT | MCL_FUTURE)
Use huge pages for large shared-memory regions
Use isolcpus / nohz_full / rcu_nocbs for hot cores
Disable noisy background services on trading host
```

## 15. Reliability and Observability

Required runtime metrics:

```text
committed_global_seq
durable_wal_seq
parquet_written_seq
storage_backlog
per-symbol write_seq
reader retry count
ring overwrite count
WAL write latency
Parquet batch latency
writer heartbeat
```

Required checks:

```text
global_seq continuity
WAL recovery correctness
Parquet row count equals WAL row count for time window
Per-symbol latest ring sequence correctness
Reader consistency under concurrent writes
```

Crash recovery:

```text
Replay WAL from last valid record.
Validate checksum if enabled.
Regenerate Parquet from WAL if needed.
Do not depend on Parquet as the primary intraday durability layer.
```

## 16. Acceptance Criteria

Functional acceptance:

```text
Strategies can query latest n ticks by instrumentId.
Queries return ordered and consistent records.
All 09:25-15:00 ticks are persisted.
Parquet files can be generated from persisted data.
Crash recovery can replay WAL.
```

Performance acceptance:

```text
Single ingest thread handles >= 1M ticks/s.
Short bursts of >= 2M ticks/s do not block on_md.
on_md p99 <= 2us under target load.
Latest 1,000-tick query p99 <= 20us.
Storage backlog remains bounded during peak load.
```

Data acceptance:

```text
WAL row count equals number of received ticks in target window.
Parquet row count equals WAL row count after conversion.
No gaps in global_seq.
No partial or torn records returned to readers.
```

## 17. Final Design Boundary

The implementation must satisfy:

```text
Real-time latest-n query: shared-memory per-symbol ring buffer.
Full-day persistence: append-only global log plus WAL.
Parquet conversion: asynchronous batch process outside on_md.
```

Primary principle:

```text
The hot path only writes memory and publishes sequence numbers.
Everything involving IO, compression, encoding, or recovery runs outside the hot path.
```
