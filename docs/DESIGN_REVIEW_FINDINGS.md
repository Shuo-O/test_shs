# Design Review Findings

This file records correctness and implementation risks found in `docs/DESIGN_SPEC.md`.

## P0: Seqlock Protocol Cannot Succeed

The design writes `begin_seq` as an odd value and `end_seq` as `odd_seq + 1`, then asks the reader to accept a slot only when `begin_seq == end_seq`.

That condition can never be true.

Required fix:

Use a single seqlock version field, or update both begin and end to the same completed even version. The first implementation uses one `version` field:

```text
writer:
  version = odd
  write payload
  version = even

reader:
  read version before payload
  copy payload
  read version after payload
  accept only if versions match and are even
```

## P0: `committed_seq` Meaning Is Ambiguous

The document sometimes treats `committed_seq` as the latest written sequence, but the tailer loop uses it as a half-open end sequence.

Required fix:

Define `committed_seq` as exclusive end:

```text
records available to readers/storage: [0, committed_seq)
latest record sequence: committed_seq - 1
```

## P1: Missing `instrumentId` Bounds Check

`query_latest_n` accesses `instrument_to_index[instrumentId]` directly.

Required fix:

Before indexing, validate:

```text
0 <= instrumentId < kIdArraySize
```

Otherwise a bad strategy input can read outside the shared-memory index.

## P1: Slot Sequence Must Be Verified

The design computes `slot_idx = seq & mask`, then only validates that the slot is internally consistent.

That is not enough after ring wrap. A reader can get a valid but newer record from the same slot.

Required fix:

Each slot must store the expected local sequence. Reader accepts a slot only when:

```text
slot.local_seq == expected_seq
```

## P1: Time-Window Filtering Is Contradictory

The document says the hot path should not filter by time, but the storage tailer filters using `FLAG_IN_WINDOW`.

If the hot path never sets the flag, WAL can be empty.

Required fix:

Either:

1. Set `FLAG_IN_WINDOW` in the writer, or
2. Let the storage tailer check `order.recvTime` directly.

The first implementation sets the flag in `on_md` because the check is a cheap integer comparison.

## P1: Parquet Batch Size Produces Small Files

The design converts each 4M-row WAL segment and then splits it into 16 buckets.

That gives only about 250k rows per bucket per flush, roughly 10MB raw payload, far below the desired 512MB-1GB row group size.

Required fix:

Accumulate per-bucket data across WAL segments and flush only when a bucket reaches the target row group size or at end of day.

## P1: `writev` Does Not Mean Durable

The design updates `durable_seq` immediately after `writev`.

That only means data was handed to the kernel page cache. It does not guarantee persistence after crash or power loss.

Required fix:

Track at least two sequence numbers:

```text
written_seq: written to file descriptor
durable_seq: persisted after fsync/fdatasync/O_DIRECT policy
```

The first implementation uses `fsync` on WAL flush in DEV_MOCK.

## P2: GlobalDayLog Overwrite Must Be Detected

`GlobalDayLog` is circular, but the design does not check whether storage has fallen more than log capacity behind.

Required fix:

Detect:

```text
committed_seq - durable_seq > kLogCapacity
```

and raise a hard data-loss alert. A production build should stop ingest or fail closed when full persistence is mandatory.

## P2: Shared-Memory Atomics Need Platform Constraints

The design uses `std::atomic` in shared memory. On the target Linux/macOS x86_64/arm64 platforms this is practical for lock-free integral atomics, but the C++ standard does not generally specify cross-process behavior.

Required fix:

Require:

```text
static_assert(std::atomic<uint64_t>::is_always_lock_free)
```

and document compiler/platform constraints.

## P2: Some Latency Numbers Need Benchmark Validation

Several cycle estimates are useful design targets but too optimistic as guaranteed values.

Required fix:

Keep the numbers as targets and validate with benchmark output. Do not present them as proofs.
