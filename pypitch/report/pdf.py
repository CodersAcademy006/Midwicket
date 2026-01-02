"""
PDF Report Generator for PyPitch

Generates professional scouting reports and match summaries with charts.
"""

import base64
import io
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle

import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
from matplotlib.figure import Figure

from ..api.session import PyPitchSession

# Type aliases for now - these should be imported from query.defs when available
PlayerStats = Any
MatchStats = Any


@dataclass
class ChartConfig:
    """Configuration for chart generation."""
    figsize: tuple = (8, 6)
    dpi: int = 100
    style: str = 'seaborn-v0_8'
    colors: Dict[str, str] = None

    def __post_init__(self):
        if self.colors is None:
            self.colors = {
                'primary': '#1f77b4',
                'secondary': '#ff7f0e',
                'success': '#2ca02c',
                'danger': '#d62728',
                'warning': '#ff9896'
            }


class PDFGenerator:
    """Professional PDF report generator with charts."""

    def __init__(self, session: PyPitchSession, config: Optional[ChartConfig] = None):
        self.session = session
        self.config = config or ChartConfig()

        # Set matplotlib style
        plt.style.use(self.config.style)

        # ReportLab styles
        self.styles = getSampleStyleSheet()
        self.styles.add(ParagraphStyle(
            name='CustomTitle',
            parent=self.styles['Heading1'],
            fontSize=24,
            spaceAfter=30,
            alignment=1  # Center
        ))
        self.styles.add(ParagraphStyle(
            name='SectionHeader',
            parent=self.styles['Heading2'],
            fontSize=18,
            spaceAfter=20,
            borderWidth=1,
            borderColor=colors.black,
            borderPadding=5
        ))

    def _generate_chart_image(self, fig: Figure) -> str:
        """Convert matplotlib figure to temporary image file."""
        buffer = io.BytesIO()
        fig.savefig(buffer, format='png', dpi=self.config.dpi, bbox_inches='tight')
        buffer.seek(0)
        plt.close(fig)

        # Save to temporary file and return path
        import tempfile
        import os
        temp_fd, temp_path = tempfile.mkstemp(suffix='.png')
        try:
            with os.fdopen(temp_fd, 'wb') as temp_file:
                temp_file.write(buffer.getvalue())
        finally:
            buffer.close()

        return temp_path

    def _create_performance_chart(self, player_stats: PlayerStats) -> str:
        """Create performance trend chart."""
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=self.config.figsize)

        # Batting average trend
        if player_stats.batting_stats and len(player_stats.batting_stats) > 1:
            dates = [stat.date for stat in player_stats.batting_stats]
            averages = [stat.average for stat in player_stats.batting_stats]
            ax1.plot(dates, averages, 'o-', color=self.config.colors['primary'], linewidth=2)
            ax1.set_title('Batting Average Trend')
            ax1.set_ylabel('Average')
            ax1.grid(True, alpha=0.3)

        # Bowling economy trend
        if player_stats.bowling_stats and len(player_stats.bowling_stats) > 1:
            dates = [stat.date for stat in player_stats.bowling_stats]
            economies = [stat.economy for stat in player_stats.bowling_stats]
            ax2.plot(dates, economies, 'o-', color=self.config.colors['secondary'], linewidth=2)
            ax2.set_title('Bowling Economy Trend')
            ax2.set_ylabel('Economy Rate')
            ax2.grid(True, alpha=0.3)

        # Runs scored vs wickets taken
        if player_stats.batting_stats and player_stats.bowling_stats:
            runs = sum(stat.runs for stat in player_stats.batting_stats)
            wickets = sum(stat.wickets for stat in player_stats.bowling_stats)
            ax3.bar(['Runs Scored', 'Wickets Taken'], [runs, wickets],
                   color=[self.config.colors['success'], self.config.colors['danger']])
            ax3.set_title('Contribution Summary')
            ax3.set_ylabel('Count')

        # Recent form (last 5 matches)
        if player_stats.recent_matches:
            recent_runs = [match.runs for match in player_stats.recent_matches[-5:]]
            ax4.plot(range(len(recent_runs)), recent_runs, 'o-',
                    color=self.config.colors['warning'], linewidth=2)
            ax4.set_title('Recent Form (Runs)')
            ax4.set_xlabel('Match')
            ax4.set_ylabel('Runs')
            ax4.grid(True, alpha=0.3)

        plt.tight_layout()
        return self._generate_chart_image(fig)

    def _create_match_comparison_chart(self, match_stats: MatchStats) -> str:
        """Create match comparison chart."""
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=self.config.figsize)

        # Team scores comparison
        teams = [match_stats.team1, match_stats.team2]
        scores = [match_stats.team1_score, match_stats.team2_score]
        ax1.bar(teams, scores, color=[self.config.colors['primary'], self.config.colors['secondary']])
        ax1.set_title('Team Scores')
        ax1.set_ylabel('Runs')

        # Key player performances
        if match_stats.top_performers:
            players = [p.name for p in match_stats.top_performers[:5]]
            runs = [p.runs for p in match_stats.top_performers[:5]]
            wickets = [p.wickets for p in match_stats.top_performers[:5]]

            x = range(len(players))
            ax2.bar(x, runs, width=0.35, label='Runs', color=self.config.colors['success'])
            ax2.bar([i + 0.35 for i in x], wickets, width=0.35, label='Wickets',
                   color=self.config.colors['danger'])
            ax2.set_xticks([i + 0.175 for i in x])
            ax2.set_xticklabels(players, rotation=45, ha='right')
            ax2.set_title('Top Performers')
            ax2.legend()

        plt.tight_layout()
        return self._generate_chart_image(fig)

    def create_scouting_report(self, player_id: str, output_path: str) -> None:
        """Generate comprehensive scouting report for a player."""
        # Get player data
        player_stats = self.session.get_player_stats(player_id)
        if not player_stats:
            raise ValueError(f"Player {player_id} not found")

        # Create PDF document (skip chart for now with simple stats)
        doc = SimpleDocTemplate(output_path, pagesize=A4)
        story = []

        # Title
        title = Paragraph(f"Scouting Report - {player_stats.name}", self.styles['CustomTitle'])
        story.append(title)
        story.append(Spacer(1, 12))

        # Overview stats
        story.append(Paragraph("Player Overview", self.styles['SectionHeader']))

        # Stats table
        batting_avg = player_stats.average or 0
        strike_rate = player_stats.strike_rate or 0
        economy = player_stats.economy or 0
        
        stats_data = [
            ['Matches', 'Total Runs', 'Batting Avg', 'Strike Rate'],
            [
                str(player_stats.matches),
                str(player_stats.runs),
                f"{batting_avg:.2f}",
                f"{strike_rate:.2f}"
            ],
            ['Wickets', 'Balls Bowled', 'Runs Conceded', 'Economy'],
            [
                str(player_stats.wickets),
                str(player_stats.balls_bowled),
                str(player_stats.runs_conceded),
                f"{economy:.2f}"
            ]
        ]

        stats_table = Table(stats_data)
        stats_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 14),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        story.append(stats_table)
        story.append(Spacer(1, 20))

        # Summary
        story.append(Paragraph("Summary", self.styles['SectionHeader']))
        summary_text = f"""
        {player_stats.name} has played {player_stats.matches} matches, scoring {player_stats.runs} runs 
        with a batting average of {batting_avg:.2f} and strike rate of {strike_rate:.2f}. 
        As a bowler, they have taken {player_stats.wickets} wickets with an economy rate of {economy:.2f}.
        """
        story.append(Paragraph(summary_text, self.styles['Normal']))
        story.append(Spacer(1, 20))

        # Footer
        story.append(Spacer(1, 30))
        footer_text = f"Report generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} by PyPitch Analytics"
        story.append(Paragraph(footer_text, self.styles['Normal']))

        # Build PDF
        doc.build(story)

    def create_match_report(self, match_id: str, output_path: str) -> None:
        """Generate detailed match summary report."""
        # Get match data
        match_stats = self.session.get_match_stats(match_id)
        if not match_stats:
            raise ValueError(f"Match {match_id} not found")

        # Generate comparison chart
        comparison_chart_path = self._create_match_comparison_chart(match_stats)

        # Create PDF document
        doc = SimpleDocTemplate(output_path, pagesize=A4)
        story = []

        # Title
        title = Paragraph(f"Match Summary - {match_stats.team1} vs {match_stats.team2}",
                         self.styles['CustomTitle'])
        story.append(title)
        story.append(Spacer(1, 12))

        # Match details
        story.append(Paragraph("Match Details", self.styles['SectionHeader']))

        details_data = [
            ['Venue', 'Date', 'Winner', 'Margin'],
            [
                match_stats.venue,
                match_stats.date.strftime('%d/%m/%Y'),
                match_stats.winner,
                match_stats.margin
            ]
        ]

        details_table = Table(details_data)
        details_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        story.append(details_table)
        story.append(Spacer(1, 20))

        # Scores
        scores_data = [
            ['Team', 'Score', 'Wickets'],
            [match_stats.team1, str(match_stats.team1_score), str(match_stats.team1_wickets)],
            [match_stats.team2, str(match_stats.team2_score), str(match_stats.team2_wickets)]
        ]

        scores_table = Table(scores_data)
        scores_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        story.append(scores_table)
        story.append(Spacer(1, 20))

        # Match analysis chart
        story.append(Paragraph("Match Analysis", self.styles['SectionHeader']))
        chart_img = Image(comparison_chart_path, 6*inch, 4*inch)
        story.append(chart_img)
        story.append(Spacer(1, 20))

        # Top performers
        story.append(Paragraph("Top Performers", self.styles['SectionHeader']))
        if match_stats.top_performers:
            performer_data = [['Player', 'Runs', 'Wickets']]
            for player in match_stats.top_performers[:5]:
                performer_data.append([player.name, str(player.runs), str(player.wickets)])

            performer_table = Table(performer_data)
            performer_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            story.append(performer_table)

        # Footer
        story.append(Spacer(1, 30))
        footer_text = f"Report generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} by PyPitch Analytics"
        story.append(Paragraph(footer_text, self.styles['Normal']))

        # Build PDF
        doc.build(story)

        # Clean up temporary chart file
        import os
        os.unlink(comparison_chart_path)


# Convenience functions
def create_scouting_report(session: PyPitchSession, player_id: str, output_path: str) -> None:
    """Convenience function to create scouting report."""
    generator = PDFGenerator(session)
    generator.create_scouting_report(player_id, output_path)


def create_match_report(session: PyPitchSession, match_id: str, output_path: str) -> None:
    """Convenience function to create match report."""
    generator = PDFGenerator(session)
    generator.create_match_report(match_id, output_path)