#!/usr/bin/env python3
"""Inspect and export Parquet files.

Usage examples:
  - Inspect a file:
      python3 export_parquets.py --inspect data/crypto/.../part-00070-....parquet
  - Export all parquet files under a folder to CSVs mirroring structure:
      python3 export_parquets.py --root data/crypto --export-csv out_csv_dir
  - Export all parquet files under a folder into a single SQLite DB (requires pandas):
      python3 export_parquets.py --root data/crypto --export-sqlite out.db

The script prefers pyarrow APIs to avoid requiring pandas; SQLite export uses pandas if available.
"""
import argparse
import glob
import os
import sqlite3
import sys

import pyarrow.parquet as pq
import pyarrow.csv as pacsv


def inspect_file(path, max_rows=20):
    print(f"Inspecting: {path}")
    pf = pq.ParquetFile(path)
    print(pf.metadata)
    print(pf.schema)

    # Read entire file (these parquet files look small)
    table = pq.read_table(path)

    # Show a few rows using pyarrow -> dict
    data = table.to_pydict()
    # Build row-wise list for printing
    rows = []
    keys = list(data.keys())
    nrows = len(data[keys[0]]) if keys else 0
    for i in range(min(nrows, max_rows)):
        row = {k: data[k][i] for k in keys}
        rows.append(row)

    print(f"Showing up to {max_rows} rows (found {nrows}):")
    for r in rows:
        print(r)


def find_parquet_files(root):
    candidates = glob.glob(os.path.join(root, "**", "*.parquet*"), recursive=True)
    files = [p for p in candidates if not p.endswith(".crc")]
    return sorted(files)


def export_to_csv(root, out_dir):
    files = find_parquet_files(root)
    if not files:
        print("No parquet files found to export.")
        return
    for p in files:
        rel = os.path.relpath(p, root)
        out_path = os.path.join(out_dir, rel)
        out_folder = os.path.dirname(out_path)
        os.makedirs(out_folder, exist_ok=True)
        # Ensure extension .csv
        if not out_path.lower().endswith('.csv'):
            out_path = out_path + '.csv'

        try:
            table = pq.read_table(p)
            pacsv.write_csv(table, out_path)
            print(f"Wrote CSV: {out_path}")
        except Exception as e:
            print(f"Failed to export {p} -> {out_path}: {e}")


def export_to_sqlite(root, db_path):
    # This function uses pandas for convenience. If pandas not available, bail.
    try:
        import pandas as pd
    except Exception:
        print("Exporting to SQLite requires pandas. Install it (pip install pandas).")
        return

    files = find_parquet_files(root)
    if not files:
        print("No parquet files found to export.")
        return

    conn = sqlite3.connect(db_path)
    try:
        for p in files:
            # create a safe table name from relative path
            rel = os.path.relpath(p, root)
            table_name = rel.replace(os.sep, "_")
            table_name = table_name.replace('.', '_').replace('-', '_')
            df = pq.read_table(p).to_pandas()
            df.to_sql(table_name, conn, if_exists='append', index=False)
            print(f"Appended {len(df)} rows into table `{table_name}` in {db_path}")
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description='Inspect and export Parquet files.')
    parser.add_argument('--inspect', help='Path to a parquet file to inspect')
    parser.add_argument('--root', help='Root folder to search for parquet files')
    parser.add_argument('--export-csv', help='Export all found parquet files to CSV under this output dir')
    parser.add_argument('--export-sqlite', help='Export all found parquet files into a SQLite DB (requires pandas)')
    parser.add_argument('--max-rows', type=int, default=20, help='Max rows to show when inspecting')

    args = parser.parse_args()

    if args.inspect:
        if not os.path.exists(args.inspect):
            print(f"File not found: {args.inspect}")
            sys.exit(2)
        inspect_file(args.inspect, max_rows=args.max_rows)
        return

    if not args.root:
        print('Either --inspect or --root must be provided')
        parser.print_help()
        sys.exit(2)

    if args.export_csv:
        export_to_csv(args.root, args.export_csv)

    if args.export_sqlite:
        export_to_sqlite(args.root, args.export_sqlite)

    if not args.export_csv and not args.export_sqlite:
        files = find_parquet_files(args.root)
        print(f"Found {len(files)} parquet files under {args.root}")


if __name__ == '__main__':
    main()
