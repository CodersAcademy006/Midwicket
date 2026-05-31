"""
Research Study Template
=======================

QUESTION
--------
<One sentence. Must be answerable from Cricsheet data alone.>

METHODOLOGY
-----------
<What data is used, what query logic, what statistical method, what assumptions.>

EXPECTED RESULTS
----------------
<What shape of answer to expect. Not the number — the direction and magnitude.>

LIMITATIONS
-----------
<What this study cannot say. What confounders are not controlled for.>

DATA CUTOFF
-----------
<The approximate Cricsheet snapshot this was validated against, e.g. "May 2026".>
"""

import midwicket as md
from midwicket.datasets import load_dataset

# ── 1. Load data ──────────────────────────────────────────────────────────────
session = load_dataset("ipl")          # change to the appropriate dataset

# ── 2. Query ──────────────────────────────────────────────────────────────────
result = session.engine.execute_sql("""
    -- Replace with your query
    SELECT 1 AS placeholder
""").to_pandas()

# ── 3. Compute ────────────────────────────────────────────────────────────────
# Any Python/Pandas aggregation that follows the SQL query.

# ── 4. Output ─────────────────────────────────────────────────────────────────
print(result)

# ── 5. Chart note ─────────────────────────────────────────────────────────────
# If you produce a chart, describe what it shows and what file it is saved to.
# Chart generation is optional. If included, use matplotlib and save to:
#   research/charts/<study_number>_<short_name>.png
