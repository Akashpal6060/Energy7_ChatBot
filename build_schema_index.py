# build_schema_index.py

import os
import json
from sqlalchemy import create_engine, inspect, text
from dotenv import load_dotenv

# ─── (1) Load environment and connect ──────────────────────────────────────────
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("Please set DATABASE_URL in your .env")

engine = create_engine(DATABASE_URL)
inspector = inspect(engine)

schema_index = {}

# ─── (2) Loop over all tables in the DB ────────────────────────────────────────
for table_name in inspector.get_table_names():
    # 2.1) Gather column metadata
    columns = []
    for col in inspector.get_columns(table_name):
        columns.append({
            "name": col["name"],
            "type": str(col["type"])
        })
    schema_index[table_name] = {
        "columns": columns,
        "sample_values": {}
    }

    # 2.2) For each “text-like” column, grab up to 10 distinct non-NULL values
    for col in columns:
        col_name = col["name"]
        col_type = col["type"].lower()

        # Only attempt to pull sample values for text-like columns
        if any(substr in col_type for substr in ["varchar", "text", "nvarchar", "char"]):
            # Use SQL Server’s TOP (10) syntax instead of LIMIT
            query = text(f"""
                SELECT DISTINCT TOP (10) "{col_name}"
                FROM "{table_name}"
                WHERE "{col_name}" IS NOT NULL
            """)
            try:
                results = engine.execute(query).fetchall()
                sample_list = [row[0] for row in results if row[0] is not None]
            except Exception:
                sample_list = []

            schema_index[table_name]["sample_values"][col_name] = sample_list

# ─── (3) Write out the JSON file ───────────────────────────────────────────────
with open("schema_index.json", "w", encoding="utf-8") as f:
    json.dump(schema_index, f, indent=2, ensure_ascii=False)

print("✅ Built schema_index.json")
