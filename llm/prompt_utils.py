# llm/prompt_utils.py
from typing import List, Dict

# 3-shot prompt taken from the original paper / Defog examples
FEW_SHOT = """-- Example 1
-- Question: list all site names
SELECT Name FROM Site;

-- Example 2
-- Question: how many point machines are in each zone?
SELECT z.Name, COUNT(*) AS PointMachines
FROM Site s
JOIN Asset a  ON a.SiteId = s.Id
JOIN Zone  z  ON z.Id    = s.ZoneId
WHERE a.AssetTypeId = 123          -- id for point machine
GROUP BY z.Name;

-- Example 3
-- Question: give the max current at Surat
SELECT MAX(pmd.Current) 
FROM PointMachineData pmd
JOIN Site s ON s.Id = pmd.SiteId
WHERE s.Name ILIKE 'surat';
"""

def build_schema_snippet(tables: list[str]) -> str:
    """
    For each table include first 6 columns (otherwise prompt gets huge).
    Robust against tables with 0 columns.
    """
    lines = []
    for t in tables:
        cols = schema.describe_table(t)
        if not cols:                      # safety net
            continue
        col_names = ", ".join(c["name"] for c in cols[:6])
        lines.append(f"- {t}({col_names})")
    if not lines:
        raise ValueError("No usable columns found for chosen tables.")
    return "\n".join(lines)
