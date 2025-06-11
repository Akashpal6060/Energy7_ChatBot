# llm/sql_generation.py
import os, re, torch
from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig

# ── 1. Model choice ───────────────────────────────────────────────────────────
SQL_MODEL = os.getenv("SQL_MODEL", "defog/llama-3-sqlcoder-8b")

# ── 2. Device & dtype ─────────────────────────────────────────────────────────
if torch.cuda.is_available():
    device = "cuda"
    major_cc, _ = torch.cuda.get_device_capability(0)
    dtype = torch.bfloat16 if major_cc >= 8 else torch.float16
else:
    device = "cpu"
    dtype = torch.float32

print(f"Loading {SQL_MODEL!r} on {device} ({dtype}) …", flush=True)
tok = AutoTokenizer.from_pretrained(SQL_MODEL, use_fast=True)
model = AutoModelForCausalLM.from_pretrained(
    SQL_MODEL,
    torch_dtype=dtype,
    device_map="auto" if device == "cuda" else None,
)

# ── 3. Exception ─────────────────────────────────────────────────────────────
class SQLGenError(RuntimeError):
    """Raised when no usable SELECT can be extracted."""

# ── 4. Generation helper ──────────────────────────────────────────────────────
@torch.inference_mode()
def generate_sql_for_point_machines(prompt: str) -> str:
    """
    Return a SQL string.  Salvage common failure modes:
    • leading prose → strip until first 'select'
    • missing leading keyword → auto-prepend 'SELECT ' if it looks like a column list
    """
    # Encode prompt
    inputs = tok(prompt, return_tensors="pt").to(device)

    # Generate continuation
    ids = model.generate(
        **inputs,
        max_new_tokens=192,
        do_sample=False,                       # greedy
        pad_token_id=tok.eos_token_id,
        eos_token_id=tok.eos_token_id,
    )

    # Decode only the new tokens
    # llm/sql_generation.py  – inside generate_sql_for_point_machines

    raw = tok.decode(ids[0, inputs["input_ids"].shape[-1]:],
                    skip_special_tokens=True).strip()
    print("▶ DEBUG raw model output =", repr(raw))

    # 1) already starts with SELECT
    if raw.lower().startswith("select"):
        return raw

    # 2) looks like "<cols> FROM …"  → prepend SELECT
    #    e.g. "spm.Name AS PointMachineName FROM SurveyPointMachine …"
    if re.match(r"^[\w\.\*].+\sfrom\s", raw, re.IGNORECASE):
        return "SELECT " + raw

    # 3) early chatter, then SELECT within first 15 chars → trim
    idx = raw.lower().find("select")
    if 0 <= idx <= 15:
        trimmed = raw[idx:].lstrip()
        if trimmed.lower().startswith("select"):
            return trimmed

    # 4) nothing salvageable
    raise SQLGenError(
        "Model did not return a SELECT statement.\n\n"
        f"Raw model output was:\n{raw}"
    )
