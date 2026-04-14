#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PyPitch Report Plugin Demo

Demonstrates professional PDF report generation for cricket analytics.
Shows how to create scouting reports and match summaries with charts.
"""

import sys
from pathlib import Path

# Ensure UTF-8 stdout on Windows (CP1252 would crash on non-ASCII)
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]

# Add pypitch to path
sys.path.insert(0, str(Path(__file__).parent))

from pypitch.api.session import PyPitchSession
try:
    from pypitch.report import create_scouting_report, create_match_report
except ImportError:
    print("[INFO] reportlab is not installed — install with 'pip install pypitch[report]' to run this demo.")
    sys.exit(0)


def demo_scouting_report():
    """Demonstrate scouting report generation."""
    print("[Search] Generating Scouting Report...")

    # Initialize session
    session = PyPitchSession()

    # Example player ID (you would get this from your data)
    player_id = "virat_kohli"  # This would be a real player ID from your registry

    # Generate report
    output_path = "scouting_report_virat_kohli.pdf"
    try:
        create_scouting_report(session, player_id, output_path)
        print(f"[OK] Scouting report saved to: {output_path}")
        print("   [Chart] Includes performance trends, recent form, and key statistics")
    except Exception as e:
        print(f"[FAIL] Failed to generate scouting report: {e}")
        print("   [Tip] Make sure you have player data loaded in your session")


def demo_match_report():
    """Demonstrate match report generation."""
    print("\n[Cricket] Generating Match Report...")

    # Initialize session
    session = PyPitchSession()

    # Example match ID (you would get this from your data)
    match_id = "ipl_2024_final"  # This would be a real match ID

    # Generate report
    output_path = "match_report_ipl_final.pdf"
    try:
        create_match_report(session, match_id, output_path)
        print(f"[OK] Match report saved to: {output_path}")
        print("   [Chart] Includes team comparison, top performers, and match analysis")
    except Exception as e:
        print(f"[FAIL] Failed to generate match report: {e}")
        print("   [Tip] Make sure you have match data loaded in your session")


def demo_custom_styling():
    """Demonstrate custom chart styling."""
    print("\n[Style] Custom Styling Demo...")

    from pypitch.report.pdf import PDFGenerator, ChartConfig

    # Custom color scheme
    custom_colors = {
        'primary': '#1a365d',    # Dark blue
        'secondary': '#e53e3e',  # Red
        'success': '#38a169',    # Green
        'danger': '#d69e2e',     # Orange
        'warning': '#3182ce'     # Blue
    }

    config = ChartConfig(
        figsize=(10, 8),
        dpi=150,
        colors=custom_colors
    )

    session = PyPitchSession()
    generator = PDFGenerator(session, config)

    print("[OK] Custom chart configuration created")
    print("   [Style] Professional color scheme applied")
    print("   [Size] High-resolution charts (150 DPI)")
    print("   [Layout] Larger figure size for better readability")


def demo_batch_reports():
    """Demonstrate batch report generation."""
    print("\n[Batch] Batch Report Generation...")

    session = PyPitchSession()

    # Example player IDs (in real usage, you'd get these from your data)
    players = ["virat_kohli", "rohit_sharma", "jasprit_bumrah"]
    matches = ["ipl_2024_01", "ipl_2024_02"]

    print("Generating reports for multiple players and matches...")

    # Generate player reports
    for player_id in players:
        output_path = f"scouting_{player_id}.pdf"
        try:
            create_scouting_report(session, player_id, output_path)
            print(f"[OK] {player_id} report: {output_path}")
        except Exception as e:
            print(f"[FAIL] {player_id} failed: {e}")

    # Generate match reports
    for match_id in matches:
        output_path = f"match_{match_id}.pdf"
        try:
            create_match_report(session, match_id, output_path)
            print(f"[OK] {match_id} report: {output_path}")
        except Exception as e:
            print(f"[FAIL] {match_id} failed: {e}")


def main():
    """Main demo function."""
    print("[Start] PyPitch Report Plugin Demo")
    print("=" * 50)

    # Check if we have data
    try:
        session = PyPitchSession()
        # Try to access some data to see if it's loaded
        session.get_player_stats("test")
    except Exception:
        print("[Warning]  Note: This demo requires cricket data to be loaded.")
        print("   Run the data ingestion examples first:")
        print("   python examples/01_setup_data.py")
        print("   python examples/03_ingest_world.py")
        print()

    # Run demos
    demo_scouting_report()
    demo_match_report()
    demo_custom_styling()
    demo_batch_reports()

    print("\n[Done] Demo Complete!")
    print("\n[Files] Generated Reports:")
    print("   - scouting_report_*.pdf (Player analysis)")
    print("   - match_report_*.pdf (Match summaries)")
    print("\n[Config] Features Demonstrated:")
    print("   [OK] Professional PDF generation")
    print("   [OK] Interactive charts and graphs")
    print("   [OK] Custom styling and branding")
    print("   [OK] Batch processing capabilities")
    print("   [OK] Error handling and validation")

    print("\n[Docs] Usage in Your Code:")
    print("""
from pypitch.api.session import PyPitchSession
from pypitch.report import create_scouting_report, create_match_report

# Initialize session
session = PyPitchSession()

# Generate player scouting report
create_scouting_report(session, "player_id", "scouting_report.pdf")

# Generate match summary report
create_match_report(session, "match_id", "match_report.pdf")
""")


if __name__ == "__main__":
    main()