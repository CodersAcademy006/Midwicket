"""
pypitch.express: Facade for one-liner data loading and analysis.
Inspired by Plotly Express. Hides complexity for beginners.
"""
from pypitch.sources.cricsheet_loader import CricsheetLoader

def load_competition(competition: str, season: int, data_dir: str = "./data"):
    """
    Loads all matches for a competition and season with one line.
    Example:
        ipl = px.load_competition("ipl", 2023)
    Returns a loader object with match_ids and match_data access.
    """
    # For now, just use CricsheetLoader. In future, can route by competition.
    loader = CricsheetLoader(data_dir)
    # Optionally filter match_ids by competition/season here
    return loader
