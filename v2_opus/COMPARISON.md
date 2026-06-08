# v2 (Opus clean-room) vs current implementation

An independent re-implementation built from `docs/DESIGN_SPEC.md`, not reusing the
existing code. Same problem, same shared-memory + seqlock + WAL architecture,
but re-derived and re-optimized. This documents the design differences and a
head-to-head benchmark on the same machine (Apple Silicon, arm64, mock profile,
`-O3`).

## What is the same (and why)

These are forced by the requirements, so both implementations converge on them:

- Direct `int32 index[1e6]` for O(1) id→slot (no hashing).
- Per-symbol seqlock ring for latest-n; payload stored in the slot so reads are
  sequential (an index-only ring would turn latest-1000 into 1000 random misses
  into a 32 GiB log, ~70 µs — it would fail the 20 µs read target).
- Append-only day log → tailer → batched WAL (fsync) → offline Parquet.
- Writer/tailer stat counters on separate cache lines; reader metrics are
  caller-owned because strategy processes map read-only.
- Read/durable cursor split: `durable_seq` advances only after fsync.

## What v2 does differently

| Area | current (`main`) | v2 | Rationale |
|---|---|---|---|
| Hot-path clock | `std::chrono::steady_clock::now()` per tick | raw counter (`mach_absolute_time` / `rdtsc` / `cntvct`) + offline ns conversion via superblock anchor | steady_clock is **15 ns**, the raw counter **5 ns** on this box (measured). On x86 the gap is larger (clock_gettime vDSO vs rdtsc). |
| Ring slot | `{version, local_seq, global_seq, order}` (3 scalar stores) | `{seq, global_seq, order}` (2 scalar stores) — the even seqlock witness `2*(pos+1)` already encodes the position | one fewer 8-byte store per tick; reader derives position from the witness |
| Heartbeat | stored every tick | refreshed every 256 ticks | liveness needs ms-scale freshness, not per-tick; removes a store from the contended header line |
| Instrument registration | lazy inside `on_md` | explicit `register_instrument()` warm-up; hot path is a pure lookup (still falls back to lazy) | keeps the very first tick per symbol off the slow path; matches "hot path only does lookup" |
| Huge pages | none | `madvise(MADV_HUGEPAGE)` on each region (no-op on macOS) | fewer TLB misses on the 4.9 GiB rings / 32 GiB log in production |
| Layout | 1 control struct, status sub-grouped | superblock (static, read-mostly) split from `WriterControl` (hot) onto distinct lines; clock anchor in superblock | static metadata never shares the writer's hot line |

## Benchmark (mock profile, 1,000,000 ticks, 5 runs, warmup discarded)

Aggregate ingest throughput — the stable metric (per-tick p50 is dominated by the
~30 ns the benchmark harness itself spends on two `steady_clock` reads per tick,
identical for both, so it masks the real difference):

```
                     throughput (ticks/s)        variance
current (main)       ~13.9 M                      ±9%   (12.6–15.3 M)
v2 (fast clock)      ~21.5 M                       ±1%  (21.5–21.6 M)
```

v2 is ~1.5× the throughput and far more stable.

### Attribution (controlled A-B test)

Recompiling v2 with `-DMDV2_FORCE_STEADY` (only the clock changes):

```
v2, raw counter      ~20.8 M ticks/s
v2, steady_clock     ~17.2 M ticks/s     <- isolates the clock: ~10 ns/tick
current (main)       ~13.9 M ticks/s     <- also steady_clock
```

So the win decomposes as:
- **~10 ns/tick from the hot-path clock** (direct microbench: 15 ns → 5 ns; and
  the A-B test: 20.8 M → 17.2 M).
- the rest (v2-steady ~17.2 M still > baseline ~13.9 M, and much lower jitter)
  from the leaner slot write and the periodic heartbeat removing stores from the
  contended header line.

Note the baseline here is the **already-optimized** `main` (cache-line isolation,
periodic stats, cached durable view, seqlock fences). v2's gains are on top of it.

## Correctness

Both pass the same checks. v2's suite:

- `functional`: latest-n ordering, unknown symbol, ring-wrap overwrite, WAL
  trading-window filter, durable cursor.
- `concurrent`: 1 writer + 4 read-only readers, ~365k queries/run, a
  sequence-encoded payload invariant that trips on any torn read. **0 torn reads**
  on arm64 (a weak memory model that would expose a missing fence), wrap path
  exercised (~220 overwrites/run).
- WAL→Parquet end-to-end: 50,000 rows round-trip, field invariant verified.

## How to run

```
cd v2_opus
make test                       # functional + concurrent
make bench ROWS=1000000         # throughput / latency
make PROFILE=-DMDV2_STANDARD v2_test   # production sizing (needs ~38 GiB RAM to run)
```
