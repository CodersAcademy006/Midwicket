"""
PyPitch Report Plugin: Professional Report Generation

Generate PDF scouting reports, match summaries, and player profiles.
Essential for coaches, scouts, and performance analysts.
"""

from .pdf import PDFGenerator, create_scouting_report, create_match_report

__all__ = [
    'PDFGenerator',
    'create_scouting_report',
    'create_match_report'
]