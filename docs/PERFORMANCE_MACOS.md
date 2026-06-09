# macOS Performance Report

Measured on the local Mac for `main` / `functional-core-dev` after the reader
consistency revision and the hardening pass (atomic control scalars, tailer
torn-lap recheck, honest durable cursor, size-validated attach).

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

## Changes covered by this report

Reader (query path):

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
   reader). The same redundant guard was removed from the frozen v2_opus
   comparison reader; its tests still pass.
2. When a tile recheck finds an unstable slot, the per-slot fallback resumes
   from that slot instead of re-reading the whole tile.
3. count==1 queries take a single-slot seqlock read: one acquire plus one
   fence instead of two fences.
```

Hardening (no hot-path cost):

```text
4. index[], sb.magic and sb.instrument_count are now atomics: the writer
   release-publishes index entries after ring-head init and magic after
   superblock init; readers load relaxed (ring data carries its own
   release/acquire edges). Relaxed u32/u64 atomics compile to plain
   loads/stores, so the hot paths are unchanged. The seqlock payload copy
   deliberately stays a plain 40-byte copy (standard kernel seqlock idiom;
   per-word atomics would pessimize the vectorized copy for no practical
   gain -- the witness protocol already rejects torn data).
5. The tailer copies each day-log record and rechecks its global_seq behind
   an acquire fence before persisting: a record overwritten mid-copy by a
   (by-design unreachable) full-log lap is dropped and counted instead of
   written torn to the WAL.
6. Honest durable cursor: when the tailer is fully drained with an empty
   buffer, durable_seq advances to read_seq. Off-window traffic no longer
   shows phantom durability lag, and after a clean drain
   durable_seq == committed_seq (visible below and asserted in tests).
7. ShmManager::open() fstats each segment and refuses to map one smaller
   than this build expects (a stale segment from another config previously
   SIGBUSed on first touch), and validates log_capacity when mapping the log.
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
C++ tests: passed (concurrent: 326,991 consistent reads, 0 torn reads;
                   durable_seq == committed_seq after clean stop)
v2_opus tests: passed (338,166 consistent reads, 0 torn reads)
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
ingest_seconds=0.022502
ingest_throughput_ticks_per_sec=2.22202e+07
committed_seq=662060 (500,000 measured rows + 162,060 live-phase ticks)
durable_seq=662060 (== committed_seq: cursors converge after the drain)
tailer_drain_ms=0.0415
```

`on_md` latency:

```text
p50:   0 ns (clock granularity; see note)
p99:   125 ns
p999:  1,208 ns
max:   75,500 ns
```

Interpretation:

```text
Average measured ingest throughput is about 22.2M ticks/s (run-to-run range
on this machine: 19.7M - 24.6M).
This is about 22x above a 1M ticks/s design target.
This is about 11x above a 2M ticks/s burst target.
The p99 on_md latency is well below 2us (83-125 ns across runs).
p50 resolves to one 41.67ns tick of the macOS 24MHz steady-clock counter;
runs that land between ticks report p50=0.
The max value reflects scheduler/runtime noise and is not a production p9999.
```

## Query Benchmark (idle writer)

`query_latest_n(100)`:

```text
p50:   167 ns
p99:   250 ns
p999:  334 ns
max:   7,167 ns
```

`query_latest_n(1000)`:

```text
p50:   1,791 ns
p99:   2,333 ns
p999:  2,500 ns
max:   15,041 ns
```

Interpretation:

```text
latest-100 p99 is far below 5us.
latest-1000 p99 is 2.3us, far below the self-imposed 20us L1 benchmark.
Latencies are unchanged from the previous revision within run-to-run noise:
the atomic index load is relaxed and compiles to the same plain load.
The batched seqlock reader snapshots versions, copies payloads, then rechecks
versions in tiles, reducing acquire fences from O(n) to O(n/tile) on
weak-memory CPUs while preserving the per-slot seqlock correctness fallback.
```

## Query Benchmark (live writer)

A writer thread pushes out-of-window ticks into the queried symbol's ring at
full speed (162,060 ticks during the phase) while the reader queries the same
symbol. kErrOverwritten means the ring genuinely wrapped under the reader; the
benchmark counts it and samples successful snapshots.

`query_latest_n(100)` under live writer:

```text
p50:   208 ns
p99:   291 ns
p999:  334 ns
max:   334 ns
aborts: 0 of 2,000 (0%)
```

`query_latest_n(1000)` under live writer:

```text
p50:   1,916 ns
p99:   2,500 ns
p999:  2,625 ns
max:   2,625 ns
aborts: 822 vs 1,000 successes (45.1%; run-to-run range 35-50%)
```

Interpretation:

```text
latest-100 under a full-speed writer matches the idle numbers with zero
aborts: tile validation plus the per-slot fallback absorb the contention.
latest-1000 asks for 1000 of 1024 ring slots, so the writer only needs to
advance 24 positions (~1.1us at ~22M ticks/s into one symbol) to genuinely
overwrite the oldest requested slot before it is copied; the 35-50% abort
rate tracks writer speed and is that physical limit, not protocol overhead.
Before the reader revision the trailing write_seq recheck pushed the same
scenario to a 99.8% abort rate by also rejecting consistent snapshots.
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
real: 1.00 s
user: 0.84 s
sys:  0.07 s
```

Estimated conversion throughput:

```text
500,000 rows / 1.00s ~= 500,000 rows/s
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
C++ correctness tests: passed (incl. cursor convergence after clean stop)
v2_opus comparison tests: passed
Parquet conversion/read-back tests: passed
on_md throughput target: passed
on_md p99 target: passed
latest-100 p99 target: passed
latest-1000 <20us p99 target on macOS: passed
latest-100 under live writer: passed (0% aborts)
latest-1000 under live writer: successes within target; aborts bounded by
the n ~= ring_capacity physical overwrite limit (was 99.8% pre-revision)
durable_seq == committed_seq after drain: passed (no phantom lag)
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
