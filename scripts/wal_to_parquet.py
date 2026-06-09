#!/usr/bin/env python3
"""Convert WAL segments to bucketed Parquet (requirement B, offline step).

Runs outside the C++ hot path. Each WAL record is a 64-byte LogRecord:
    uint64 global_seq, MDUniOrder(40B), uint32 flags, 12B padding

Partitioning matches the design: date=YYYYMMDD / bucket=instrument_id % 16.

    pip install pyarrow
    scripts/wal_to_parquet.py wal_dir out_dir --trading-day 20260608
"""
from __future__ import annotations

import argparse
import collections
import pathlib
import struct
import sys

RECORD_SIZE = 64
# MDUniOrder: int8 type, int8 bsFlag, int8 tickType, pad, int16 channel, pad,
# then int32 x6 and uint32 x2 -- 40 bytes, starting at offset 8 in the record.
ORDER_STRUCT = struct.Struct("<bbb x h xx iiiiii II")
DEFAULT_BUCKETS = 16
DEFAULT_ROW_GROUP_ROWS = 1_000_000

COLUMNS = [
    "global_seq",
    "type", "bs_flag", "tick_type", "channel",
    "instrument_id", "n_tp", "recv_time", "price", "qty", "biz_seq",
    "order_seq", "order_id",
    "flags",
]


def load_pyarrow():
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise SystemExit("pyarrow is required: pip install pyarrow") from exc
    return pa, pq


def parse_wal_file(path: pathlib.Path):
    data = path.read_bytes()
    if len(data) % RECORD_SIZE != 0:
        raise ValueError(f"{path} size is not a multiple of {RECORD_SIZE}")
    for offset in range(0, len(data), RECORD_SIZE):
        record = data[offset:offset + RECORD_SIZE]
        (global_seq,) = struct.unpack_from("<Q", record, 0)
        order = ORDER_STRUCT.unpack_from(record, 8)
        (flags,) = struct.unpack_from("<I", record, 48)
        yield (global_seq, *order, flags)


def rows_to_table(pa, rows):
    if not rows:
        return pa.table({c: [] for c in COLUMNS})
    cols = list(zip(*rows))
    return pa.table({c: list(v) for c, v in zip(COLUMNS, cols)})


def write_bucket(pa, pq, rows, out_dir, trading_day, bucket, part) -> int:
    if not rows:
        return 0
    d = out_dir / f"date={trading_day}" / f"bucket={bucket:02d}"
    d.mkdir(parents=True, exist_ok=True)
    table = rows_to_table(pa, rows)
    pq.write_table(table, d / f"part-{part:06d}.parquet",
                   compression="lz4", use_dictionary=["instrument_id", "channel"])
    return table.num_rows


def main(argv) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("wal_dir", type=pathlib.Path)
    ap.add_argument("out_dir", type=pathlib.Path)
    ap.add_argument("--trading-day", default="unknown")
    ap.add_argument("--buckets", type=int, default=DEFAULT_BUCKETS)
    ap.add_argument("--row-group-rows", type=int, default=DEFAULT_ROW_GROUP_ROWS)
    args = ap.parse_args(argv[1:])

    pa, pq = load_pyarrow()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    buffers = collections.defaultdict(list)
    parts = collections.defaultdict(int)
    total = 0

    for wal in sorted(args.wal_dir.glob("*.bin")):
        for row in parse_wal_file(wal):
            bucket = row[4] % args.buckets  # instrument_id is column index 4
            rows = buffers[bucket]
            rows.append(row)
            if len(rows) >= args.row_group_rows:
                total += write_bucket(pa, pq, rows, args.out_dir,
                                      str(args.trading_day), bucket, parts[bucket])
                parts[bucket] += 1
                buffers[bucket] = []

    for bucket in sorted(buffers):
        total += write_bucket(pa, pq, buffers[bucket], args.out_dir,
                              str(args.trading_day), bucket, parts[bucket])

    print(f"wrote {total} rows to {args.out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
