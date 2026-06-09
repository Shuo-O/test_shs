# macOS Performance Report

Measured on the local Mac for `main` / `functional-core-dev` after the reader
consistency revision (redundant final wrap guard removed, unstable-tile
fallback resumes at the first failed slot, n==1 single-slot fast path).

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

## Reader changes in this revision

```text
1. query_latest_n no longer re-reads write_seq after the copy loop. Every
   accepted slot is identity-checked (witness == 2*(pos+1)) on both sides of
   the copy, so the result already equals the ring content at the initial
   write_seq snapshot. The trailing recheck added no correctness; it cost one
   extra acquire load of the writer-hot RingHead line per query and aborted
   consistent snapshots whenever the writer advanced more than
   capacity - count positions mid-copy. With a full-speed same-symbol writer,
   latest-1000 on the 1,024-slot DEV ring aborted 99.8% of the time before
   this change (measured with an equivalent microbenchmark on the previous
   reader).
2. When a tile recheck finds an unstable slot, the per-slot fallback now
   resumes from that slot instead of re-reading the whole tile (slots before
   it are already validated).
3. count==1 queries take a single-slot seqlock read: one acquire plus one
   fence instead of two fences.
```

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
C++ tests: passed (concurrent: 338,139 consistent reads, 0 torn reads)
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
ingest_seconds=0.0241729
ingest_throughput_ticks_per_sec=2.06843e+07
committed_seq=630534 (500,000 measured rows + 130,534 live-phase ticks)
durable_seq=500000
tailer_drain_ms=0.135333
```

`on_md` latency:

```text
p50:   41 ns
p99:   125 ns
p999:  1,250 ns
max:   39,000 ns
```

Interpretation:

```text
Average measured ingest throughput is about 20.7M ticks/s (run-to-run range
on this machine: 19.7M - 24.3M).
This is about 20x above a 1M ticks/s design target.
This is about 10x above a 2M ticks/s burst target.
The p99 on_md latency is well below 2us.
p50 resolves to one 41.67ns tick of the macOS 24MHz steady-clock counter;
runs that land between ticks report p50=0.
The max value reflects scheduler/runtime noise and is not a production p9999.
```

## Query Benchmark (idle writer)

`query_latest_n(100)`:

```text
p50:   167 ns
p99:   209 ns
p999:  333 ns
max:   14,000 ns
```

`query_latest_n(1000)`:

```text
p50:   1,583 ns
p99:   2,041 ns
p999:  2,291 ns
max:   12,792 ns
```

Interpretation:

```text
latest-100 p99 is far below 5us.
latest-1000 p99 is 2.041us, far below the self-imposed 20us L1 benchmark.
Idle-writer latencies are unchanged from the previous revision within
run-to-run noise; the reader changes target the contended path.
The batched seqlock reader snapshots versions, copies payloads, then rechecks
versions in tiles, reducing acquire fences from O(n) to O(n/tile) on
weak-memory CPUs while preserving the per-slot seqlock correctness fallback.
```

## Query Benchmark (live writer, new in this revision)

A writer thread pushes out-of-window ticks into the queried symbol's ring at
full speed (130,534 ticks during the phase) while the reader queries the same
symbol. kErrOverwritten means the ring genuinely wrapped under the reader; the
benchmark counts it and samples successful snapshots.

`query_latest_n(100)` under live writer:

```text
p50:   167 ns
p99:   292 ns
p999:  375 ns
max:   458 ns
aborts: 0 of 2,000 (0%)
```

`query_latest_n(1000)` under live writer:

```text
p50:   1,708 ns
p99:   2,291 ns
p999:  2,375 ns
max:   2,375 ns
aborts: 541 vs 1,000 successes (35.1%)
```

Interpretation:

```text
latest-100 under a full-speed writer matches the idle numbers with zero
aborts: tile validation plus the per-slot fallback absorb the contention.
latest-1000 asks for 1000 of 1024 ring slots, so the writer only needs to
advance 24 positions (~1us at ~20M ticks/s into one symbol) to genuinely
overwrite the oldest requested slot before it is copied; the ~35-40% abort
rate is that physical limit, not protocol overhead. Before this revision the
trailing write_seq recheck pushed the same scenario to a 99.8% abort rate by
also rejecting consistent snapshots.
The production profile (ring 16,384, per-symbol rates far below a single
benchmark writer at full speed) makes deep-query aborts correspondingly rare.
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
real: 0.94 s
user: 0.84 s
sys:  0.06 s
```

Estimated conversion throughput:

```text
500,000 rows / 0.94s ~= 532,000 rows/s
```

Interpretation:

```text
WAL persistence is correct and outside the writer's direct fsync path.
Live-phase ticks are out-of-window by design, so WAL row accounting is
unaffected by the contended query phase (wal_rows == rows still holds).
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
latest-100 under live writer: passed (0% aborts)
latest-1000 under live writer: successes within target; aborts bounded by
the n ~= ring_capacity physical overwrite limit (was 99.8% pre-revision)
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
