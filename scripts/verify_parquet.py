#!/usr/bin/env python3
"""Verify row count and a few fields from generated Parquet files."""

from __future__ import annotations

import pathlib
import sys


def main(argv: list[str]) -> int:
    if len(argv) < 3:
        print(
            "usage: verify_parquet.py parquet_dir expected_rows "
            "[--expected-instruments csv] [--require-prices csv]",
            file=sys.stderr,
        )
        return 2

    try:
        import pyarrow.dataset as ds
    except ImportError as exc:
        raise SystemExit("pyarrow is required: python3 -m pip install pyarrow") from exc

    parquet_dir = pathlib.Path(argv[1])
    expected_rows = int(argv[2])
    expected_instruments = None
    required_prices = None

    i = 3
    while i < len(argv):
        if argv[i] == "--expected-instruments":
            i += 1
            expected_instruments = {int(v) for v in argv[i].split(",") if v}
        elif argv[i] == "--require-prices":
            i += 1
            required_prices = {int(v) for v in argv[i].split(",") if v}
        else:
            raise SystemExit(f"unknown argument: {argv[i]}")
        i += 1

    dataset = ds.dataset(parquet_dir, format="parquet", partitioning="hive")
    table = dataset.to_table()

    if table.num_rows != expected_rows:
        raise SystemExit(f"expected {expected_rows} rows, got {table.num_rows}")

    if expected_instruments is not None:
        instrument_ids = set(table.column("instrument_id").to_pylist())
        if instrument_ids != expected_instruments:
            raise SystemExit(f"unexpected instruments: {sorted(instrument_ids)}")

    if required_prices is not None:
        prices = set(table.column("price").to_pylist())
        missing = required_prices - prices
        if missing:
            raise SystemExit(f"expected sample prices were not found: {sorted(missing)}")

    print(f"verified {table.num_rows} parquet rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
