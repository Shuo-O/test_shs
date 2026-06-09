# macOS Performance Report

Measured on the local Mac for `main` / `functional-core-dev` after the batched
seqlock query optimization.

## Environment

```text
Date: 2026-06-10
OS: macOS 26.5, build 25F71
CPU: Apple M3 Pro
Cores reported by hw.ncpu: 11
Memory: 36 GiB
Compiler: Apple clang 21.0.0
Python: 3.14.5
pyarrow: 24.0.0
Production build: -O3 -march=native
Test/bench build: -O3 -march=native -DDEV_MOCK
```

`DEV_MOCK` parameters:

```text
Instruments: 256 max, benchmark used 128
Ring capacity: 1,024 records per instrument
Global day-log capacity: 1,048,576 records
WAL segment rows: 4,096
Slot size: 64 bytes
LogRecord size: 64 bytes
MDUniOrder size: 40 bytes
```

This is a functional macOS benchmark. It does not use Linux CPU isolation, fixed
frequency tuning, huge pages, IRQ pinning, or kernel-bypass networking.

## Commands

```bash
make all
make test
make bench BENCH_ROWS=500000
```

## Correctness Results

```text
make all: passed
make test: passed
C++ tests: passed
WAL -> Parquet verification: passed
Parquet rows verified in benchmark: 500,000
WAL rows written in benchmark: 500,000
daylog_overwrite_count: 0
```

## Ingest Benchmark

`tick_bench` pre-registers 128 instruments, then measures 500,000 in-window
ticks.

```text
rows=500000
symbols=128
ingest_seconds=0.0209705
ingest_throughput_ticks_per_sec=2.38431e+07
committed_seq=500000
durable_seq=500000
tailer_drain_ms=0.101542
```

`on_md` latency:

```text
p50:   0 ns
p99:   84 ns
p999:  958 ns
max:   48,958 ns
```

Interpretation:

```text
Average measured ingest throughput is about 23.84M ticks/s.
This is about 23.8x above a 1M ticks/s design target.
This is about 11.9x above a 2M ticks/s burst target.
The p99 on_md latency is well below 2us.
The p50=0ns value reflects macOS steady_clock granularity at this scale.
The max value reflects scheduler/runtime noise and is not a production p9999.
```

## Query Benchmark

`query_latest_n(100)`:

```text
p50:   167 ns
p99:   208 ns
p999:  292 ns
max:   7,125 ns
```

`query_latest_n(1000)`:

```text
p50:   1,584 ns
p99:   2,084 ns
p999:  2,250 ns
max:   8,167 ns
```

Interpretation:

```text
latest-100 p99 is far below 5us.
latest-1000 p99 is 2.084us, far below the self-imposed 20us L1 benchmark.
The batched seqlock reader snapshots versions, copies payloads, then rechecks
versions in tiles, reducing acquire fences from O(n) to O(n/tile) on weak-memory
CPUs while preserving the per-slot seqlock correctness fallback.
```

## WAL and Parquet Benchmark

WAL output:

```text
bench_wal size: 31 MiB
records: 500,000
record size: 64 bytes
segments: 123 files
```

Parquet output:

```text
bench_parquet size: 12 MiB
files: 16
rows verified: 500,000
```

Parquet conversion time for 500,000 rows:

```text
real: 0.92 s
user: 0.85 s
sys:  0.05 s
```

Estimated conversion throughput:

```text
500,000 rows / 0.92s ~= 543,000 rows/s
```

Interpretation:

```text
WAL persistence is correct and outside the writer's direct fsync path.
The Python Parquet converter is intentionally a dev/test implementation.
For production, replace it with Arrow C++ or a parallel converter.
```

## Assessment

```text
Production binary build: passed
C++ correctness tests: passed
Parquet conversion/read-back tests: passed
on_md throughput target: passed
on_md p99 target: passed
latest-100 p99 target: passed
latest-1000 <20us p99 target on macOS: passed
```

Recommended production validation:

```text
Run the same benchmark on a tuned Linux single-socket server.
Pin writer, tailer, and readers to isolated cores.
Use huge pages for the large shared-memory regions.
Measure cross-process readers, not only same-process mmap readers.
Add crash-recovery and truncated-WAL benchmarks.
Move Parquet conversion from Python to Arrow C++ for throughput testing.
```
