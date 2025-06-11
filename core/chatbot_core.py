"""
Lightweight NL→SQL pipeline.

✓  Detect if a question targets the DB.
✓  Pick relevant tables (BM25).
✓  Build a compact schema snippet the SQL LLM can see.
✓  Ask the SQL model for a SELECT.
✓  Run it read-only and pretty-print the DataFrame.
"""
from __future__ import annotations

import logging
import os
import traceback
from textwrap import dedent
from typing import Any, Dict, List

from dotenv import load_dotenv

load_dotenv()

# ── local deps ────────────────────────────────────────────────────────────────
from llm.sql_generation import generate_sql_for_point_machines, SQLGenError
from semantic_schema import schema_retrieval as schema                    # NOTE
from semantic_schema.schema_retrieval import find_relevant_tables
from llm.plain_chat import chat_completion
from core.execute_query import run_sql_and_fetch

# ── logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    filename="chatbot_core.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

# ── 0. heuristics ─────────────────────────────────────────────────────────────
_DB_HINTS = (
    "select", "list", "show", "give", "count", "how many",
    "average", "avg", "mean", "max", "min", "sum",
)


def _looks_like_db_question(q: str) -> bool:
    qlow = q.lower()
    return any(k in qlow for k in _DB_HINTS)


# ── 1. schema helpers ─────────────────────────────────────────────────────────
FEW_SHOT = dedent(
    """
    ### You are an expert SQL generator for Microsoft SQL-Server.
    ### Return a *single* valid SELECT statement – no comments, no `GO`.
    """
).strip()


def _get_columns(table: str) -> List[str]:
    """
    Robust column lookup that works with either implementation:

      • If the module exposes `SCHEMA_INDEX` (dict), use that.
      • Else fall back to `schema.describe_table(table) -> list[dict]`
    """
    if hasattr(schema, "SCHEMA_INDEX"):
        cols_info: List[Dict[str, Any]] = schema.SCHEMA_INDEX[table]["columns"]  # type: ignore[attr-defined]
    else:
        cols_info = schema.describe_table(table)  # type: ignore[attr-defined]

    return [c["name"] for c in cols_info]


def _build_schema_snippet(tables: list[str]) -> str:
    parts = []
    for tbl in tables:
        try:
            cols = _get_columns(tbl)
            parts.append(f"-- {tbl}({', '.join(cols)})")
        except Exception as err:  # keep going if one table fails
            logging.warning("Could not fetch cols for %s: %s", tbl, err)
            parts.append(f"-- {tbl}")
    return "\n".join(parts)


# ── 2. main entry point ───────────────────────────────────────────────────────
def chatbot_answer(question: str) -> str:
    """
    CLI / API hook.
    """
    try:
        # fallback to chit-chat if it’s clearly not a data question
        if not _looks_like_db_question(question):
            return chat_completion(question)

        # 1️⃣  semantic search
        tables = find_relevant_tables(question, k=4)
        if not tables:
            return "⚠️ Sorry, I couldn’t map that to any database tables."

        logging.info("Selected tables for %s → %s", question, tables)

        # 2️⃣  prompt & SQL generation
        prompt = "\n\n".join(
            [
                FEW_SHOT,
                _build_schema_snippet(tables),
                f"-- Question: {question}",
                "### Answer\nSELECT",
            ]
        )
        sql = generate_sql_for_point_machines(prompt)
        logging.info("Generated SQL:\n%s", sql)

        if not sql.lower().lstrip().startswith("select"):
            raise SQLGenError("Model did not return a SELECT.")

        # 3️⃣  execute (read-only)
        df = run_sql_and_fetch(sql, limit=200)
        if df.empty:
            return "ℹ️ Query executed but returned no rows."

        # 4️⃣  pretty-print
        return "✅ Result:\n\n" + df.to_string(index=False)

    # expected errors
    except SQLGenError as e:
        logging.warning("SQL-generation error: %s", e)
        return f"⚠️ SQL-generation error: {e}"
    except RuntimeError as e:  # thrown by run_sql_and_fetch
        return f"⚠️ Database error: {e}"

    # defensive catch-all
    except Exception as e:  # noqa: BLE001
        logging.error("Unhandled error: %s\n%s", e, traceback.format_exc())
        return f"❌ Oops, something went wrong: {e}"
