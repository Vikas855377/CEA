from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.xactly_jdbc import XactlyJdbcClient


def main() -> int:
    parser = argparse.ArgumentParser(description="Test the Xactly JDBC connection.")
    parser.add_argument("--sql", default="select 1", help="SQL query to run.")
    parser.add_argument("--max-rows", type=int, default=20, help="Maximum rows to print.")
    parser.add_argument("--schema", default="", help="Filter result rows by schema_name.")
    parser.add_argument("--name-only", action="store_true", help="Print only the name column.")
    args = parser.parse_args()

    client = XactlyJdbcClient()
    try:
        frame = client.query_df(args.sql, max_rows=args.max_rows)
    except Exception as exc:
        print(f"Connection failed: {exc}")
        return 1
    finally:
        client.close()

    if args.schema:
        if "schema_name" not in frame.columns:
            print("Connection succeeded, but the result does not include a schema_name column.")
            return 1
        frame = frame[frame["schema_name"].astype(str).str.lower() == args.schema.lower()]

    if args.name_only:
        if "name" not in frame.columns:
            print("Connection succeeded, but the result does not include a name column.")
            return 1
        frame = frame[["name"]]

    print(f"Connected. Query returned {len(frame)} row(s) and {len(frame.columns)} column(s).")
    if not frame.empty:
        print(frame.head(args.max_rows).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
