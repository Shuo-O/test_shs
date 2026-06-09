# macOS Performance Report

Measured on the local Mac for branch `functional-core-dev`.

## Environment

```text
Date: 2026-06-09
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
ingest_seconds=0.023421
ingest_throughput_ticks_per_sec=2.13484e+07
committed_seq=500000
durable_seq=500000
tailer_drain_ms=0.110125
```

`on_md` latency:

```text
p50:   0 ns
p99:   125 ns
p999:  1,416 ns
max:   155,333 ns
```

Interpretation:

```text
Average measured ingest throughput is about 21.35M ticks/s.
This is about 21.3x above a 1M ticks/s design target.
This is about 10.7x above a 2M ticks/s burst target.
The p99 on_md latency is well below 2us.
The p50=0ns value reflects macOS steady_clock granularity at this scale.
The max value reflects scheduler/runtime noise and is not a production p9999.
```

## Query Benchmark

`query_latest_n(100)`:

```text
p50:   583 ns
p99:   1,500 ns
p999:  5,417 ns
max:   73,917 ns
```

`query_latest_n(1000)`:

```text
p50:   29,625 ns
p99:   43,458 ns
p999:  127,958 ns
max:   733,667 ns
```

Interpretation:

```text
latest-100 p99 is below 2us on this Mac run.
latest-1000 copies 40KB of payload and validates 1,000 seqlock slots per call.
The measured latest-1000 p99 is 43.458us, so this macOS run does not meet a
20us p99 target for 1,000-record copy-out queries.
For a strict <20us latest-1000 target, use a Linux tuned host and consider a
zero-copy/iterator API or a bounded query depth used by the strategy hot path.
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
real: 1.21 s
user: 1.05 s
sys:  0.09 s
```

Estimated conversion throughput:

```text
500,000 rows / 1.21s ~= 413,000 rows/s
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
latest-1000 <20us p99 target on macOS: not passed in this run
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
