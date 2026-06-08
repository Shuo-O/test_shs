# macOS Performance Report

Measured on the local Mac after implementing the DEV_MOCK build.

## Environment

```text
OS: macOS 26.5
CPU: Apple M3 Pro
Cores reported by hw.ncpu: 11
Memory: 36 GiB
Compiler: Apple clang 21.0.0
Python: 3.14.5
pyarrow: 24.0.0
Build: -O2 -DDEV_MOCK
```

DEV_MOCK parameters:

```text
Instruments: 256 max, benchmark used 128
Ring capacity: 1,024 per instrument
GlobalDayLog capacity: 1,048,576 records
Internal record size: 64 bytes
```

This is a functional/macOS benchmark, not a production low-latency host benchmark. macOS has no isolated cores, no Linux huge pages, no IRQ pinning, and no kernel-bypass NIC in this test.

## Commands

```bash
make test
make bench BENCH_ROWS=500000
```

## Correctness Results

```text
make test: passed
WAL -> Parquet verification: passed
Parquet rows verified: 500,000
WAL rows written: 500,000
daylog_overwrite_count: 0
```

## Ingest Benchmark

`tick_bench` generated 128 warm-up records outside the trading window, then measured 500,000 in-window ticks.

```text
rows=500000
symbols=128
ingest_seconds=0.0566921
ingest_throughput_ticks_per_sec=8,819,570
committed_seq=500128
durable_wal_seq=500128
tailer_drain_ms=0.41425
```

`on_md` latency:

```text
p50:   42 ns
p99:   292 ns
p999:  2,042 ns
max:   187,875 ns
```

Interpretation:

```text
Average measured throughput is about 8.8M ticks/s.
This is 8.8x above the 1M ticks/s design target.
This is 4.4x above the 2M ticks/s burst target.
The p99 on_md latency is well below the 2us target.
The max value reflects macOS scheduling/runtime noise and should not be treated as production p9999.
```

## Query Benchmark

`query_latest_n(100)`:

```text
p50:   166 ns
p99:   250 ns
p999:  292 ns
max:   20,542 ns
```

`query_latest_n(1000)`:

```text
p50:   2,084 ns
p99:   2,833 ns
p999:  3,750 ns
max:   30,666 ns
```

Interpretation:

```text
latest-100 p99 is far below the 5us target.
latest-1000 p99 is far below the 20us target.
Tail spikes are visible on macOS but do not affect p99 in this run.
```

## WAL and Parquet Benchmark

WAL output:

```text
bench_wal size: 31 MiB
records: 500,000
record size: 64B
```

Parquet output:

```text
bench_parquet size: 12 MiB
files: 16
rows verified: 500,000
```

Parquet conversion time for 500,000 rows:

```text
real: 1.02 s
user: 0.93 s
sys:  0.07 s
```

Estimated conversion throughput:

```text
500,000 rows / 1.02s ~= 490,200 rows/s
```

Interpretation:

```text
Parquet conversion is correct but currently Python-based, so it is not optimized.
For the interview/system-design scope this is acceptable because Parquet is outside the hot path.
For production, replace the Python converter with Arrow C++ or a multi-process Python converter.
```

## Assessment

The macOS DEV_MOCK implementation meets the first-version goals:

```text
Real-time latest-n query: passed
WAL persistence: passed
Parquet conversion and read-back verification: passed
on_md throughput target: passed
on_md p99 target: passed
latest-100 p99 target: passed
latest-1000 p99 target: passed
```

Remaining production gaps:

```text
Use Linux production build with huge pages, CPU isolation, and core pinning.
Move Parquet conversion to Arrow C++ for higher throughput.
Add multi-process reader benchmark, not just same-process mmap reader.
Add crash-recovery benchmark with truncated WAL.
Add larger STANDARD run on a 128GB+ Linux host.
```
