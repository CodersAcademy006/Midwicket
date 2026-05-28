"""
Midwicket Interactive Exploration UI

Provides ipywidgets-based interactive dashboards for Jupyter Notebooks.
"""

from typing import Any
import ipywidgets as widgets
from IPython.display import display, clear_output

from midwicket.api.session import MidwicketSession
import midwicket as md

def explore_player() -> None:
    """
    Renders an interactive player exploration dashboard in Jupyter.
    Allows searching players, selecting them, and viewing their career stats
    and wagon wheel interactively without writing code.
    """
    try:
        get_ipython()  # type: ignore
    except NameError:
        print("Interactive exploration is only available in Jupyter Notebooks/Lab.")
        return

    session = MidwicketSession.get()
    
    # Get top 100 players to populate initial dropdown
    players_query = session.engine.execute_sql("SELECT DISTINCT batter FROM ball_events WHERE batter IS NOT NULL LIMIT 100").fetchall()
    player_names = sorted([p[0] for p in players_query])
    
    if not player_names:
        print("No player data available. Please initialize Midwicket first.")
        return

    # Create widgets
    title = widgets.HTML("<h2>Midwicket Player Dashboard</h2>")
    
    player_dropdown = widgets.Dropdown(
        options=player_names,
        description='Player:',
        disabled=False,
    )
    
    type_toggle = widgets.ToggleButtons(
        options=['Batting', 'Bowling'],
        description='Stats Type:',
        disabled=False,
        button_style='', 
    )

    out = widgets.Output()

    def on_change(change: Any) -> None:
        if change['type'] == 'change' and change['name'] == 'value':
            render_dashboard(player_dropdown.value, type_toggle.value)

    player_dropdown.observe(on_change, names='value')
    type_toggle.observe(on_change, names='value')

    def render_dashboard(player_name: str, stat_type: str) -> None:
        with out:
            clear_output(wait=True)
            try:
                if stat_type == 'Batting':
                    stats = md.career_batting(player_name)
                else:
                    stats = md.career_bowling(player_name)
                    
                # The stats object is a MidwicketResultDict, which auto-renders HTML
                display(stats)
            except Exception as e:
                print(f"Error fetching data: {e}")

    # Initial render
    render_dashboard(player_dropdown.value, type_toggle.value)
    
    # Display layout
    controls = widgets.VBox([title, player_dropdown, type_toggle])
    display(widgets.VBox([controls, out]))
