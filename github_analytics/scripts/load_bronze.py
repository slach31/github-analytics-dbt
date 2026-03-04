#!/usr/bin/env python3
"""
load_bronze.py (incremental, multi-file)
=========================================
Loads raw CSV files into DuckDB as Bronze tables.
Each table ingests ALL files matching its glob pattern (e.g. raw_issues_*.csv).

- First run: creates tables from all matching CSV files.
- Subsequent runs: inserts only NEW rows based on a dedup key.
- Use --full-refresh to force a complete reload.

Usage:
  cd github_analytics   # dbt project folder
  python ../scripts/load_bronze.py                # incremental
  python ../scripts/load_bronze.py --full-refresh  # full reload
"""

import duckdb
import glob
import os
import sys

DB_PATH = "github_analytics_dev.duckdb"
DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "data", "raw"
)

# Each table: glob pattern, dedup key, strategy
tables = {
    "raw_repositories": {
        "pattern": "raw_repositories_*.csv",
        "unique_key": "full_name",
        "strategy": "replace",
    },
    "raw_commits": {
        "pattern": "raw_commits_*.csv",
        "unique_key": "sha",
        "strategy": "append_new",
    },
    "raw_pull_requests": {
        "pattern": "raw_pull_requests_*.csv",
        "unique_key": ["repo_full_name", "pr_number"],
        "strategy": "append_new",
    },
    "raw_issues": {
        "pattern": "raw_issues_*.csv",
        "unique_key": ["repo_full_name", "issue_number"],
        "strategy": "append_new",
    },
}

full_refresh = "--full-refresh" in sys.argv


def _key_cols(key):
    return key if isinstance(key, list) else [key]


def _table_exists(con, schema, table):
    return con.execute(f"""
        SELECT count(*) FROM information_schema.tables
        WHERE table_schema = '{schema}' AND table_name = '{table}'
    """).fetchone()[0] > 0


def _find_files(pattern):
    """Return sorted list of CSV files matching the glob pattern."""
    full_pattern = os.path.join(DATA_DIR, pattern)
    files = sorted(glob.glob(full_pattern))
    return files


def _read_csv_union(con, files):
    """
    Create a SQL expression that reads and unions all CSV files.
    DuckDB's read_csv_auto accepts a list of files natively.
    """
    # DuckDB supports glob patterns and lists directly
    paths_sql = "[" + ", ".join(f"'{f}'" for f in files) + "]"
    return f"read_csv_auto({paths_sql}, union_by_name=true)"


def load_full(con, table_name, files):
    """Full refresh: drop and recreate from all matching CSVs."""
    read_expr = _read_csv_union(con, files)
    con.execute(f"""
        CREATE OR REPLACE TABLE bronze.{table_name} AS
        SELECT * FROM {read_expr}
    """)


def load_append_new(con, table_name, files, unique_key):
    """Append only rows whose key doesn't already exist."""
    key_cols = _key_cols(unique_key)
    read_expr = _read_csv_union(con, files)
    join_cond = " AND ".join(
        f"staging.{col} = existing.{col}" for col in key_cols
    )
    where_null = f"existing.{key_cols[0]} IS NULL"

    con.execute(f"""
        INSERT INTO bronze.{table_name}
        SELECT staging.*
        FROM {read_expr} AS staging
        LEFT JOIN bronze.{table_name} AS existing
            ON {join_cond}
        WHERE {where_null}
    """)


def load_upsert(con, table_name, files, unique_key):
    """Upsert via delete+insert pattern."""
    key_cols = _key_cols(unique_key)
    read_expr = _read_csv_union(con, files)

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _staging_{table_name} AS
        SELECT * FROM {read_expr}
    """)

    con.execute(f"""
        DELETE FROM bronze.{table_name}
        WHERE EXISTS (
            SELECT 1 FROM _staging_{table_name} AS staging
            WHERE {" AND ".join(
                f"bronze.{table_name}.{col} = staging.{col}"
                for col in key_cols
            )}
        )
    """)

    con.execute(f"""
        INSERT INTO bronze.{table_name}
        SELECT * FROM _staging_{table_name}
    """)

    con.execute(f"DROP TABLE IF EXISTS _staging_{table_name}")


def main():
    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")

    for table_name, cfg in tables.items():
        files = _find_files(cfg["pattern"])

        if not files:
            print(f"  [SKIP] No files matching {cfg['pattern']}")
            continue

        print(f"  Found {len(files)} file(s) for {table_name}:")
        for f in files:
            print(f"         - {os.path.basename(f)}")

        exists = _table_exists(con, "bronze", table_name)

        if full_refresh or not exists:
            mode = "FULL" if exists else "CREATE"
            load_full(con, table_name, files)

        elif cfg["strategy"] == "replace":
            load_full(con, table_name, files)
            mode = "REPLACE"

        elif cfg["strategy"] == "append_new":
            load_append_new(con, table_name, files, cfg["unique_key"])
            mode = "APPEND"

        elif cfg["strategy"] == "upsert":
            load_upsert(con, table_name, files, cfg["unique_key"])
            mode = "UPSERT"

        count = con.execute(
            f"SELECT count(*) FROM bronze.{table_name}"
        ).fetchone()[0]
        print(f"  [{mode:7s}] {table_name}: {count} rows")
        print()

    con.close()
    print("Bronze loading complete.")


if __name__ == "__main__":
    main()