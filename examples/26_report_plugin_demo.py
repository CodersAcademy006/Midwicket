"""Midwicket report plugin - scouting PDF, match PDF, custom styling, batch generation."""

import sys
from midwicket.api.session import MidwicketSession
try:
    from midwicket.report import create_scouting_report, create_match_report
    from midwicket.report.pdf import PDFGenerator, ChartConfig
except ImportError:
    print("install with: pip install midwicket[report]"); sys.exit(0)

session = MidwicketSession()

# 1. Scouting report
create_scouting_report(session, "virat_kohli", "scouting_report.pdf")

# 2. Match report
create_match_report(session, "ipl_2024_final", "match_report.pdf")

# 3. Custom styling
config = ChartConfig(figsize=(10, 8), dpi=150, colors={
    "primary": "#1a365d", "secondary": "#e53e3e", "success": "#38a169",
    "danger":  "#d69e2e", "warning":   "#3182ce"})
PDFGenerator(session, config)

# 4. Batch generation
for pid in ("virat_kohli", "rohit_sharma", "jasprit_bumrah"):
    create_scouting_report(session, pid, f"scouting_{pid}.pdf")
for mid in ("ipl_2024_01", "ipl_2024_02"):
    create_match_report(session, mid, f"match_{mid}.pdf")
