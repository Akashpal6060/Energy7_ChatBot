#!/usr/bin/env python3
"""
Introspect the DB and write
  • schema_index.json   (full, machine-readable)
  • schema_summary.json (one-liners, human-readable)
"""

import json
import os
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text

# ── env / connection ─────────────────────────────────────────────────
load_dotenv()
URL = os.getenv("DATABASE_URL")
if not URL:
    raise RuntimeError("DATABASE_URL missing in .env")

engine = create_engine(URL, connect_args={"connect_timeout": 10}, echo=False)
insp = inspect(engine)

# ── helpers ─────────────────────────────────────────────────────────
def _sample_values(table: str, column: str, limit: int = 5) -> List[str]:
    """Collect up to `limit` non-null samples, already converted to str."""
    sql = text(
        f"SELECT TOP {limit} [{column}] "
        f"FROM [{table}] WHERE [{column}] IS NOT NULL"
    )
    try:
        with engine.begin() as conn:
            rows = conn.execute(sql).fetchall()
        return [str(r[0]) for r in rows]          # <-- stringify here
    except Exception:
        return []


def _collect_schema() -> Dict[str, Any]:
    schema: Dict[str, Any] = {}
    for table in insp.get_table_names():
        if table.startswith("sys"):          # skip system tables
            continue

        cols = []
        for col in insp.get_columns(table):
            cols.append(
                {
                    "name": col["name"],
                    "type": str(col["type"]),
                    "sample_values": _sample_values(table, col["name"]),
                }
            )
        schema[table] = {"columns": cols}
    return schema


# ── main ────────────────────────────────────────────────────────────
def main() -> None:
    schema_index = _collect_schema()

    # 1) full index ─── default=str => auto-serialize datetime, Decimal, etc.
    with open("schema_index.json", "w", encoding="utf-8") as fh:
        json.dump(schema_index, fh, indent=2, ensure_ascii=False, default=str)

    # 2) single-line summary
    summary = [
        f"{tbl}({', '.join(c['name'] for c in meta['columns'])})"
        for tbl, meta in schema_index.items()
    ]
    pd.Series(summary).to_json("schema_summary.json", indent=2)

    print(f"✅  wrote {len(schema_index):,} tables to schema_index.json")


if __name__ == "__main__":
    main()
