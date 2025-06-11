"""
Create schema.vx (≈3 MB) from schema_summary.txt
Usage:  python -m semantic_schema.build_schema_index
"""
import re, pickle, pathlib
from sentence_transformers import SentenceTransformer

MODEL = SentenceTransformer("all-MiniLM-L6-v2")           # 90 MB; CPU-friendly
text  = pathlib.Path(__file__).with_name("schema_summary.txt").read_text()
lines = [ln for ln in text.splitlines() if ln.strip().startswith("-")]

docs, meta = [], []
pat = re.compile(r"^- (\w+)\(([^)]+)\)")

for ln in lines:
    m = pat.match(ln)
    if not m:
        continue
    tbl, cols = m.group(1), [c.strip().split()[0] for c in m.group(2).split(",")][:8]
    docs.append(f"Table {tbl} with columns {', '.join(cols)}")
    meta.append({"table": tbl, "cols": cols})

emb = MODEL.encode(docs, show_progress_bar=True, batch_size=128)
pickle.dump({"emb": emb, "meta": meta}, open(pathlib.Path(__file__).with_name("schema.vx"), "wb"))
print(f"✅ Indexed {len(meta)} tables → schema.vx")
