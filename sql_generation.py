# sql_generation.py

import os
from dotenv import load_dotenv
from huggingface_hub import InferenceClient
from prompt_utils import build_schema_snippet

# ─── Load environment variables ────────────────────────────────────────────────
load_dotenv()
HF_TOKEN = os.getenv("HF_TOKEN")
if not HF_TOKEN:
    raise RuntimeError("Please set HF_TOKEN in your .env")

# ─── Initialize the Hugging Face Inference client ─────────────────────────────
client = InferenceClient(token=HF_TOKEN)

# ─── Choose your model ID ───────────────────────────────────────────────────────
# “tiiuae/falcon-7b-instruct” is a good middle-ground: free to call on HF,
# and fairly capable at NL→SQL. If you run out of free compute units, switch
# to a smaller 3B instruct model (e.g., “tiiuae/falcon-3b-instruct”).
MODEL_ID = "tiiuae/falcon-7b-instruct"

def generate_sql_for_point_machines(question: str) -> str:
    """
    Example function to generate a SQL query for “point machines” questions,
    using a hardcoded list of tables ["Site", "Asset", "PointMachineData"].
    """

    # 1) Identify relevant tables (hardcoded here for point‐machine queries)
    relevant_tables = ["Site", "Asset", "PointMachineData"]

    # 2) Build the mini‐schema snippet
    schema_snippet = build_schema_snippet(relevant_tables)

    # 3) Construct the prompt
    prompt = (
        "### System:\n"
        "You are an expert in T-SQL (SQL Server). Given the schema below,\n"
        "translate the user’s natural‐language question into exactly ONE valid SQL query.\n"
        "Return ONLY the raw SQL—no commentary, no markdown fences.\n\n"
        "### Schema:\n"
        f"{schema_snippet}\n\n"
        "### User Question:\n"
        f"{question}\n\n"
        "### Assistant (raw SQL):\n"
    )

    # 4) Ask HF Inference API to generate the SQL
    response = client.text_generation(
        model=MODEL_ID,
        inputs=prompt,
        parameters={
            "max_new_tokens": 256,
            "temperature": 0.0,
            "top_p": 0.95
        }
    )

    # 5) Extract the generated text (some endpoints return a list, others a dict)
    if isinstance(response, list):
        generated = response[0].get("generated_text", "")
    else:
        generated = response.get("generated_text", "")

    return generated.strip()
