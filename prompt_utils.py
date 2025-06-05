# prompt_utils.py

import json

# ─── Load schema_index.json once ───────────────────────────────────────────────
with open("schema_index.json", "r", encoding="utf-8") as f:
    schema_index = json.load(f)

def build_schema_snippet(tables: list[str]) -> str:
    """
    Given a list of table names, produce a multiline string like:

        Table: Site
          - Id (INTEGER)
          - Name (NVARCHAR(200))
          - DivisionId (INTEGER)

        Table: AlertAudit
          - Id (INTEGER)
          - SiteId (INTEGER)
          - AlertId (INTEGER)
          - TimeStamp (DATETIME)

        Table: Alert
          - Id (INTEGER)
          - AlertName (NVARCHAR(200))

    That string will be injected into the LLM prompt so it knows exactly which columns exist.
    """
    lines: list[str] = []
    for table in tables:
        lines.append(f"Table: {table}")
        for col in schema_index[table]["columns"]:
            col_name = col["name"]
            col_type = col["type"]
            lines.append(f"  - {col_name} ({col_type})")
        lines.append("")  # blank line between tables

    return "\n".join(lines)
