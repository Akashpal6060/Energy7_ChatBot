# execute_query.py

import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ─── Load environment and configure logging ────────────────────────────────────
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("Please set DATABASE_URL in your .env")

# Create a SQLAlchemy engine using the provided DATABASE_URL.
# It’s strongly recommended that this URL points to a read‐only user,
# or includes “ApplicationIntent=ReadOnly” in the connection string for SQL Server.
engine = create_engine(DATABASE_URL, connect_args={"connect_timeout": 10}, echo=False)

# Configure logging so any SQL errors are captured
logging.basicConfig(
    filename="chatbot_sql.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

def run_sql_and_fetch(sql: str, limit: int = 500) -> pd.DataFrame:
    """
    Execute the provided SQL query in read‐only mode.
    Only SELECT statements are permitted. Returns up to `limit` rows.

    Parameters:
    - sql (str): A raw SQL query string (must be a SELECT).
    - limit (int): Maximum number of rows to return (default: 500).

    Returns:
    - pd.DataFrame: DataFrame with the query results (capped at `limit` rows).

    Raises:
    - RuntimeError: If the query is not a SELECT or if execution fails.
    """
    # 1) Ensure this is a SELECT (no INSERT/UPDATE/DELETE/ALTER/etc.)
    cleaned = sql.lstrip().lower()
    if not cleaned.startswith("select"):
        raise RuntimeError("Only SELECT queries are allowed in read‐only mode.")

    # 2) Log the SQL for debugging/auditing (replace newlines to keep it on one line)
    logging.info("Executing SQL: %s", sql.replace("\n", " "))

    try:
        # Use a text object to safely execute raw SQL
        query = text(sql)
        with engine.connect() as conn:
            # Set a 30‐second timeout for long‐running queries
            conn = conn.execution_options(stream_results=True, timeout=30)
            # Read the results into a pandas DataFrame
            df = pd.read_sql(query, conn)

            # If the result exceeds the limit, truncate to the first `limit` rows
            if len(df) > limit:
                df = df.head(limit)

            return df

    except Exception as e:
        # Log the error details, then re‐raise as RuntimeError
        logging.error("SQL execution error: %s", str(e))
        raise RuntimeError(f"Database error: {e}")
