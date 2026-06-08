#!/usr/bin/env python3
"""Verify row count and a few fields from generated Parquet files."""

from __future__ import annotations

import pathlib
import sys


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print("usage: verify_parquet.py parquet_dir expected_rows", file=sys.stderr)
        return 2

    try:
        import pyarrow.dataset as ds
    except ImportError as exc:
        raise SystemExit("pyarrow is required: python3 -m pip install pyarrow") from exc

    parquet_dir = pathlib.Path(argv[1])
    expected_rows = int(argv[2])
    dataset = ds.dataset(parquet_dir, format="parquet", partitioning="hive")
    table = dataset.to_table()

    if table.num_rows != expected_rows:
        raise SystemExit(f"expected {expected_rows} rows, got {table.num_rows}")

    instrument_ids = set(table.column("instrument_id").to_pylist())
    if instrument_ids != {600000, 600001}:
        raise SystemExit(f"unexpected instruments: {sorted(instrument_ids)}")

    prices = table.column("price").to_pylist()
    if 100000 not in prices or 101499 not in prices:
        raise SystemExit("expected sample prices were not found")

    print(f"verified {table.num_rows} parquet rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
