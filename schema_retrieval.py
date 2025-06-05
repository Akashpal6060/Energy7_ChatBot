# schema_retrieval.py

import os
import json
import re
from dotenv import load_dotenv

# ─── Load environment variables (if needed) ────────────────────────────────────
load_dotenv()

# ─── Load the schema index once ────────────────────────────────────────────────
with open("schema_index.json", "r", encoding="utf-8") as f:
    schema_index = json.load(f)

def find_relevant_tables(question: str, top_k: int = 3) -> list[str]:
    """
    Return a list of up to top_k table names that seem relevant to `question`.
    We do a simple heuristic:
      • +3 if table name appears as a token
      • +2 for each column name that appears as a token
      • +5 for each sample value (exact, case-insensitive) that appears as a token
      • +4 boost for telemetry keywords (“current”, “voltage”) on known tables
      • +6 boost for failure/default keywords on SI24
      • +5 boost for “alert” keywords on Alert-related tables
    Finally, return the top_k tables sorted by descending score.
    """
    # 1) Tokenize user question into lowercase words
    tokens = re.findall(r"\b\w+\b", question.lower())

    table_scores: dict[str, int] = {}

    for table_name, info in schema_index.items():
        score = 0

        # a) Table name match
        if table_name.lower() in tokens:
            score += 3

        # b) Column name matches
        for col in info["columns"]:
            if col["name"].lower() in tokens:
                score += 2

        # c) Sample value matches
        for samples in info["sample_values"].values():
            for sample in samples:
                if sample and sample.lower() in tokens:
                    score += 5

        # d) Heuristic boosts for domain keywords
        # “current” or “voltage” → point‐machine/telemetry tables
        if any(k in tokens for k in ["current", "voltage"]):
            if table_name in ["PointMachineData", "SiteAttributeData", "Asset"]:
                score += 4

        # “failure” or “default” → SI24 table
        if any(k in tokens for k in ["failure", "default"]):
            if table_name == "SI24":
                score += 6

        # “alert” → any Alert‐prefixed table
        if "alert" in tokens:
            if table_name.lower().startswith("alert"):
                score += 5

        if score > 0:
            table_scores[table_name] = score

    # Sort tables by descending score, return up to top_k names
    ranked = sorted(table_scores.items(), key=lambda item: item[1], reverse=True)
    return [table for table, _ in ranked[:top_k]]
