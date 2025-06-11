"""
semantic_schema/schema_retrieval.py
───────────────────────────────────
Very small helper around the JSON you just generated.
"""

from __future__ import annotations
import json
import pathlib
import re
import difflib
from functools import lru_cache
from typing import Any, Dict, List

# ── 1. load once ──────────────────────────────────────────────────────────
_schema_path = pathlib.Path("schema_index.json")
if not _schema_path.exists():
    raise FileNotFoundError(
        "schema_index.json not found. Run tools/generate_schema_index.py first."
    )

with _schema_path.open(encoding="utf-8") as fh:
    SCHEMA_INDEX: Dict[str, Any] = json.load(fh)

# ── 2. public helpers ─────────────────────────────────────────────────────
def describe_table(table: str) -> List[Dict[str, Any]]:
    """
    Return the column list for a given table (raises KeyError if unknown).
    """
    return SCHEMA_INDEX[table]["columns"]


@lru_cache(maxsize=256)
def _tokenise(text: str) -> set[str]:
    return set(re.findall(r"[A-Za-z0-9_]+", text.lower()))


def _normalise(tok: str) -> str:
    """
    crude stemmer – strips a trailing 's' and lower-cases
    """
    tok = tok.lower()
    return tok[:-1] if tok.endswith("s") else tok

def _bm25_score(q_tokens: set[str], table: str) -> int:
    tbl_tok = {_normalise(table)}
    for col in describe_table(table):
        tbl_tok.add(_normalise(col["name"]))

    # allow fuzzy 1-edit matches (e.g. pt101 ↔ pt_101)
    score = 0
    for q in q_tokens:
        qn = _normalise(q)
        if qn in tbl_tok:
            score += 3
        elif difflib.get_close_matches(qn, tbl_tok, n=1, cutoff=0.8):
            score += 1
    return score


def find_relevant_tables(question: str, k: int = 100) -> List[str]:
    """
    Return top-k table names sorted by relevance.
    """
    q_tokens = _tokenise(question)
    scored = [
        (tbl, _bm25_score(q_tokens, tbl)) for tbl in SCHEMA_INDEX.keys()
    ]
    scored.sort(key=lambda t: t[1], reverse=True)
    # keep only those with a non-zero score
    return [tbl for tbl, score in scored[:k] if score > 0]
