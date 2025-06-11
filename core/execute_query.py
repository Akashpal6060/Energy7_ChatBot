# core/execute_query.py
from __future__ import annotations

import os
import re
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

# ── 1. Connection settings ────────────────────────────────────────────────────
DATABASE_URL: str | None = os.getenv("DATABASE_URL")          # mssql+pymssql://…
if not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL environment variable")

CONNECT_ARGS: dict = {}

timeout_env = os.getenv("DB_TIMEOUT")          # optional connection timeout
if timeout_env and timeout_env.isdigit():
    CONNECT_ARGS["timeout"] = int(timeout_env)

# ── 2. Helpers ────────────────────────────────────────────────────────────────
def _sanitize_sql(sql: str) -> str:
    """
    Keep only the first statement produced by the LLM and drop any trailing
    prose such as “…;assistant …”.
    """
    # 1) cut at first semicolon
    if ";" in sql:
        sql = sql.split(";", 1)[0]

    # 2) cut at first line that starts with a non-SQL word like “assistant”
    lines = sql.splitlines()
    clean_lines = []
    for ln in lines:
        if re.match(r"^\s*(assistant|system|user)\b", ln, re.IGNORECASE):
            break
        clean_lines.append(ln)
    return "\n".join(clean_lines).strip()

def _sqlserver_limit(sql: str, limit: int) -> str:
    """
    Inject a TOP <limit> clause in the correct position:

    • SELECT DISTINCT …     →  SELECT DISTINCT TOP <n> …
    • SELECT …              →  SELECT TOP <n> …
    • (anything else)       →  SELECT TOP <n> * FROM (<sql>) AS _sub
    """
    sql_strip = sql.lstrip()
    # 1) SELECT DISTINCT ...
    if re.match(r"(?i)^select\s+distinct\b", sql_strip):
        return sql_strip.replace(
            "SELECT DISTINCT",
            f"SELECT DISTINCT TOP {limit}",
            1,
        )
    # 2) Plain SELECT ...
    if sql_strip.lower().startswith("select"):
        return sql_strip.replace(
            "SELECT",
            f"SELECT TOP {limit}",
            1,
        )
    # 3) Fallback: wrap as sub-select
    return f"SELECT TOP {limit} * FROM ({sql}) AS _sub"


# --- NULLS FIRST/LAST → CASE-WHEN rewrite ------------------------------------
_NULLS_PATTERN = re.compile(
    r"""
    (?P<expr>        [\w\.\[\]]+ )           # column name or alias
    (?: \s+ (?P<dir>ASC|DESC) )?             # optional ASC | DESC
    \s+ NULLS \s+ (?P<where>FIRST|LAST)      # NULLS FIRST | LAST
    """,
    re.IGNORECASE | re.VERBOSE,
)

def _rewrite_nulls_sorting(sql: str) -> str:
    """Translate “… NULLS FIRST/LAST” into SQL-Server-safe CASE ordering."""
    def repl(m: re.Match) -> str:
        col   = m.group("expr")
        dir_  = (m.group("dir") or "").upper()         # keep ASC/DESC if present
        where = m.group("where").upper()

        if where == "LAST":
            return (
                f"CASE WHEN {col} IS NULL THEN 1 ELSE 0 END {dir_}, "
                f"{col} {dir_}"
            )
        # NULLS FIRST
        return (
            f"CASE WHEN {col} IS NULL THEN 0 ELSE 1 END {dir_}, "
            f"{col} {dir_}"
        )
    return _NULLS_PATTERN.sub(repl, sql)

# ── 3. Public API ─────────────────────────────────────────────────────────────
def run_sql_and_fetch(sql: str, limit: int = 200) -> pd.DataFrame:
    """
    Execute *read-only* SQL and return the first <limit> rows as a DataFrame.

    * Removes any LLM chatter after the real statement.
    * Rewrites ORDER BY … NULLS FIRST/LAST for SQL Server.
    * Adds TOP <limit> (SQL Server) or OFFSET/FETCH (other dialects).
    """
    try:
        engine: Engine = create_engine(
            DATABASE_URL,
            connect_args=CONNECT_ARGS,
            pool_pre_ping=True,
        )

        sql = _sanitize_sql(sql)

        if engine.name.startswith("mssql"):
            sql = _rewrite_nulls_sorting(sql)
            limited_sql = _sqlserver_limit(sql, limit)
        else:
            limited_sql = f"{sql} OFFSET 0 ROWS FETCH NEXT {limit} ROWS ONLY"

        with engine.connect() as conn:
            result = conn.execute(text(limited_sql))
            return pd.DataFrame(result.fetchall(), columns=result.keys())

    except SQLAlchemyError as exc:
        raise RuntimeError(str(exc)) from exc
