# chatbot_core.py

import os
import logging
import traceback
from dotenv import load_dotenv

# Load environment variables (.env) before any other imports that rely on them
load_dotenv()

from schema_retrieval import find_relevant_tables
from sql_generation     import generate_sql_for_point_machines
from execute_query      import run_sql_and_fetch

# Configure logging for debugging
logging.basicConfig(
    filename="chatbot_core.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

def chatbot_answer(question: str) -> str:
    """
    General pipeline for any NL→SQL question:
      1) Retrieve relevant tables (via find_relevant_tables).
      2) Generate SQL using the appropriate function.
      3) Execute SQL and fetch results.
      4) Format and return the output.
    """
    try:
        # 1) Find the top‐3 relevant tables for this question
        tables = find_relevant_tables(question, top_k=3)
        if not tables:
            return "⚠️ Sorry, I couldn’t find any tables to answer that question."

        # Log which tables were picked
        logging.info("Relevant tables for question '%s': %s", question, tables)

        # 2) Generate SQL via Hugging Face (hardcoded for point-machine questions)
        # Note: For non-point-machine queries, override this logic as needed.
        sql = generate_sql_for_point_machines(question)
        logging.info("Generated SQL:\n%s", sql)

        # 3) Execute the SQL in read-only mode
        df = run_sql_and_fetch(sql, limit=200)

        # 4) Format the DataFrame result
        if df.empty:
            return "ℹ️ No records matched your query."
        else:
            # Convert DataFrame to a plain-text table
            table_str = df.to_string(index=False)
            return f"✅ Query results:\n\n{table_str}"

    except Exception as e:
        # Log the full traceback for debugging
        logging.error("Error in chatbot_answer: %s\n%s", str(e), traceback.format_exc())
        return f"❌ Oops, something went wrong: {e}"
