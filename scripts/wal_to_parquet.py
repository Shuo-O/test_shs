#!/usr/bin/env python3
"""Convert WAL files to bucketed Parquet with pyarrow.

This converter is intentionally outside the C++ hot path. It requires:

    python3 -m pip install pyarrow

Usage:

    scripts/wal_to_parquet.py wal_dir out_dir --trading-day 20260608
"""

from __future__ import annotations

import argparse
import collections
import pathlib
import struct
import sys


RECORD_SIZE = 64
# Must match C++ LogRecord: uint64 global_seq, uint64 recv_ticks (raw fast
# clock; convert to wall-clock ns offline via the superblock anchor), then the
# 40-byte MDUniOrder payload, then uint32 crc32 and uint32 flags.
ORDER_STRUCT = struct.Struct("<bbb x h xx iiiiii II")
DEFAULT_BUCKETS = 16
DEFAULT_ROW_GROUP_ROWS = 1_000_000


def load_pyarrow():
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise SystemExit("pyarrow is required: python3 -m pip install pyarrow") from exc
    return pa, pq


def parse_wal_file(path: pathlib.Path):
    data = path.read_bytes()
    if len(data) % RECORD_SIZE != 0:
        raise ValueError(f"{path} size is not a multiple of {RECORD_SIZE}")

    for offset in range(0, len(data), RECORD_SIZE):
        record = data[offset : offset + RECORD_SIZE]
        global_seq, recv_ticks = struct.unpack_from("<QQ", record, 0)
        order = ORDER_STRUCT.unpack_from(record, 16)
        crc32, flags = struct.unpack_from("<II", record, 56)
        yield (global_seq, recv_ticks, *order, crc32, flags)


def rows_to_table(pa, rows):
    columns = [
        "global_seq",
        "recv_ticks",
        "type",
        "bs_flag",
        "tick_type",
        "channel",
        "instrument_id",
        "n_tp",
        "recv_time",
        "price",
        "qty",
        "biz_seq",
        "order_seq",
        "order_id",
        "crc32",
        "flags",
    ]

    if not rows:
        return pa.table({name: [] for name in columns})
    arrays = list(zip(*rows))
    return pa.table({name: values for name, values in zip(columns, arrays)})


def write_bucket(pa, pq, rows, out_dir: pathlib.Path, trading_day: str, bucket: int, part: int) -> int:
    if not rows:
        return 0
    bucket_dir = out_dir / f"date={trading_day}" / f"bucket={bucket:02d}"
    bucket_dir.mkdir(parents=True, exist_ok=True)
    table = rows_to_table(pa, rows)
    pq.write_table(
        table,
        bucket_dir / f"part-{part:06d}.parquet",
        compression="lz4",
        use_dictionary=["instrument_id", "channel"],
    )
    return table.num_rows


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("wal_dir", type=pathlib.Path)
    parser.add_argument("out_dir", type=pathlib.Path)
    parser.add_argument("--trading-day", default="unknown")
    parser.add_argument("--buckets", type=int, default=DEFAULT_BUCKETS)
    parser.add_argument("--row-group-rows", type=int, default=DEFAULT_ROW_GROUP_ROWS)
    return parser.parse_args(argv[1:])


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    pa, pq = load_pyarrow()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    buffers = collections.defaultdict(list)
    part_index = collections.defaultdict(int)
    total_rows = 0

    for wal_file in sorted(args.wal_dir.glob("*.bin")):
        for row in parse_wal_file(wal_file):
            instrument_id = row[6]
            # Buffer per bucket across WAL segments so production-sized runs do
            # not produce tiny Parquet row groups.
            bucket = instrument_id % args.buckets
            bucket_rows = buffers[bucket]
            bucket_rows.append(row)
            if len(bucket_rows) >= args.row_group_rows:
                total_rows += write_bucket(
                    pa,
                    pq,
                    bucket_rows,
                    args.out_dir,
                    str(args.trading_day),
                    bucket,
                    part_index[bucket],
                )
                part_index[bucket] += 1
                buffers[bucket] = []

    for bucket in sorted(buffers):
        total_rows += write_bucket(
            pa,
            pq,
            buffers[bucket],
            args.out_dir,
            str(args.trading_day),
            bucket,
            part_index[bucket],
        )

    print(f"wrote {total_rows} rows to {args.out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
