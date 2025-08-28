#!/usr/bin/env python3
"""
Export all (or selected) tables from a DuckDB database to individual Parquet files.
"""

import argparse
import os
from pathlib import Path
import sys
#import duckdb

try:
    import duckdb
except ModuleNotFoundError as e:
    sys.stderr.write(
        "ERROR: duckdb Python package is not installed.\n"
        "Install with: pip install duckdb\n"
    )
    raise


def discover_tables(con, schema: str = "main") -> list:
    q = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = ?
    ORDER BY table_name
    """
    return [r[0] for r in con.execute(q, [schema]).fetchall()]


def export_table(con, table: str, out_dir: Path, overwrite: bool = False) -> Path:
    out_path = out_dir / f"{table}.parquet"
    if out_path.exists() and not overwrite:
        print(f"⚠️  Skipping {table}: {out_path} already exists (use --overwrite to replace).")
        return out_path
    # inline a safely-quoted POSIX path; COPY doesn't support parameter binding for file paths
    out_literal = "'" + out_path.as_posix().replace("'", "''") + "'"
    sql = f'COPY (SELECT * FROM "{table}") TO {out_literal} (FORMAT PARQUET);'
    con.execute(sql)
    print(f"✅ Exported {table} -> {out_path}")
    return out_path




def rowcount(con, table: str) -> int:
    return con.execute(f"SELECT COUNT(*) FROM {duckdb.escape_identifier(table)}").fetchone()[0]


def main():
    parser = argparse.ArgumentParser(description="Export DuckDB tables to Parquet files.")
    parser.add_argument("--db", required=True, help="Path to DuckDB database file (e.g., olist.duckdb)")
    parser.add_argument("--out", default="parquet", help="Output folder for Parquet files (default: ./parquet)")
    parser.add_argument("--schema", default="main", help="DuckDB schema to read from (default: main)")
    parser.add_argument("--tables", nargs="*", help="Optional list of tables to export (default: export all)")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing Parquet files")
    parser.add_argument("--summary", action="store_true", help="Print row counts per table after export")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        sys.stderr.write(f"ERROR: Database not found: {db_path}\n")
        sys.exit(2)

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))

    if args.tables:
        existing = set(discover_tables(con, args.schema))
        missing = [t for t in args.tables if t not in existing]
        if missing:
            print(f"These tables do not exist in schema '{args.schema}': {', '.join(missing)}")
        tables = [t for t in args.tables if t in existing]
    else:
        tables = discover_tables(con, args.schema)

    if not tables:
        print(f"Nothing to export. No tables found in schema '{args.schema}'.")
        sys.exit(0)

    print(f"Found {len(tables)} table(s) in schema '{args.schema}': {', '.join(tables)}")
    for t in tables:
        export_table(con, t, out_dir, overwrite=args.overwrite)

    if args.summary:
        print("\nRow counts:")
        for t in tables:
            try:
                cnt = rowcount(con, t)
                print(f"  {t:30s} {cnt:10d}")
            except Exception as e:
                print(f"  {t:30s} (error counting rows: {e})")

    con.close()


if __name__ == "__main__":
    main()
