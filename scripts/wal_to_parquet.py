#!/usr/bin/env python3
"""Convert DEV_MOCK WAL files to Parquet with pyarrow.

This converter is intentionally outside the C++ hot path. It requires:

    python3 -m pip install pyarrow

Usage:

    scripts/wal_to_parquet.py wal_dir out_dir
"""

from __future__ import annotations

import pathlib
import struct
import sys


RECORD_SIZE = 64
ORDER_STRUCT = struct.Struct("<bbb x h xx iiiiii II")


def load_pyarrow():
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise SystemExit("pyarrow is required: python3 -m pip install pyarrow") from exc
    return pa, pq


def parse_wal_file(path: pathlib.Path):
    rows = []
    data = path.read_bytes()
    if len(data) % RECORD_SIZE != 0:
        raise ValueError(f"{path} size is not a multiple of {RECORD_SIZE}")

    for offset in range(0, len(data), RECORD_SIZE):
        record = data[offset : offset + RECORD_SIZE]
        global_seq, recv_ns = struct.unpack_from("<QQ", record, 0)
        order = ORDER_STRUCT.unpack_from(record, 16)
        crc32, flags = struct.unpack_from("<II", record, 56)
        rows.append((global_seq, recv_ns, *order, crc32, flags))
    return rows


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print("usage: wal_to_parquet.py wal_dir out_dir", file=sys.stderr)
        return 2

    pa, pq = load_pyarrow()
    wal_dir = pathlib.Path(argv[1])
    out_dir = pathlib.Path(argv[2])
    out_dir.mkdir(parents=True, exist_ok=True)

    columns = [
        "global_seq",
        "recv_ns",
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

    all_rows = []
    for wal_file in sorted(wal_dir.glob("*.bin")):
        all_rows.extend(parse_wal_file(wal_file))

    if not all_rows:
        table = pa.table({name: [] for name in columns})
    else:
        arrays = list(zip(*all_rows))
        table = pa.table({name: values for name, values in zip(columns, arrays)})

    pq.write_table(table, out_dir / "ticks.parquet", compression="lz4")
    print(f"wrote {table.num_rows} rows to {out_dir / 'ticks.parquet'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
