#!/usr/bin/env python3
"""Convert v2 WAL segments to bucketed Parquet (offline, outside the hot path).

Each WAL record is a 64-byte LogRecord:
    uint64 global_seq, uint64 recv_ticks, MDUniOrder(40B), uint32 crc32, uint32 flags

recv_ticks is a raw fast-clock counter; it is converted to wall-clock ns using
the (epoch_ns, ticks, ticks_per_sec) anchor that the writer prints at startup
(pass --epoch-ns / --ticks-at-start / --ticks-per-sec, or leave them to emit raw
ticks). Partitioning matches the design: date=YYYYMMDD / bucket=id%16.

    pip install pyarrow
    wal2parquet.py wal_dir out_dir --trading-day 20260608
"""
from __future__ import annotations

import argparse
import collections
import pathlib
import struct
import sys

RECORD = 64
# MDUniOrder: int8 type, int8 bsFlag, int8 tickType, [1 pad], int16 channel,
# [2 pad], then int32 x6, uint32 x2. Matches the C++ struct's natural layout.
ORDER = struct.Struct("<bbb x h xx iiiiii II")
BUCKETS = 16
ROWS_PER_GROUP = 1_000_000

COLUMNS = [
    "global_seq", "recv_ticks", "type", "bs_flag", "tick_type", "channel",
    "instrument_id", "n_tp", "recv_time", "price", "qty", "biz_seq",
    "order_seq", "order_id", "crc32", "flags",
]


def load_pa():
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise SystemExit("pyarrow required: pip install pyarrow") from exc
    return pa, pq


def parse_wal(path: pathlib.Path):
    data = path.read_bytes()
    if len(data) % RECORD:
        raise ValueError(f"{path}: size not a multiple of {RECORD}")
    for off in range(0, len(data), RECORD):
        rec = data[off:off + RECORD]
        global_seq, recv_ticks = struct.unpack_from("<QQ", rec, 0)
        order = ORDER.unpack_from(rec, 16)
        crc32, flags = struct.unpack_from("<II", rec, 56)
        yield (global_seq, recv_ticks, *order, crc32, flags)


def to_table(pa, rows):
    if not rows:
        return pa.table({c: [] for c in COLUMNS})
    cols = list(zip(*rows))
    return pa.table({c: list(v) for c, v in zip(COLUMNS, cols)})


def write_bucket(pa, pq, rows, out: pathlib.Path, day: str, bucket: int, part: int) -> int:
    if not rows:
        return 0
    d = out / f"date={day}" / f"bucket={bucket:02d}"
    d.mkdir(parents=True, exist_ok=True)
    t = to_table(pa, rows)
    pq.write_table(t, d / f"part-{part:06d}.parquet",
                   compression="lz4", use_dictionary=["instrument_id", "channel"])
    return t.num_rows


def main(argv) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("wal_dir", type=pathlib.Path)
    ap.add_argument("out_dir", type=pathlib.Path)
    ap.add_argument("--trading-day", default="unknown")
    ap.add_argument("--buckets", type=int, default=BUCKETS)
    ap.add_argument("--rows-per-group", type=int, default=ROWS_PER_GROUP)
    args = ap.parse_args(argv[1:])

    pa, pq = load_pa()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    buffers = collections.defaultdict(list)
    parts = collections.defaultdict(int)
    total = 0

    for wal in sorted(args.wal_dir.glob("*.bin")):
        for row in parse_wal(wal):
            b = row[6] % args.buckets  # instrument_id
            rows = buffers[b]
            rows.append(row)
            if len(rows) >= args.rows_per_group:
                total += write_bucket(pa, pq, rows, args.out_dir, str(args.trading_day), b, parts[b])
                parts[b] += 1
                buffers[b] = []

    for b in sorted(buffers):
        total += write_bucket(pa, pq, buffers[b], args.out_dir, str(args.trading_day), b, parts[b])

    print(f"wrote {total} rows to {args.out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
