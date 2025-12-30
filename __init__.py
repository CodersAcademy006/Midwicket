# Expose the user-facing API modules
from pypitch.api import stats
from pypitch.api import fantasy
from pypitch.api import session

# Expose the Data Loader utility conveniently
from pypitch.data.loader import DataLoader

def setup(data_dir: str = None):
    """
    Explicitly initialize the PyPitch engine.
    Optional: PyPitch auto-initializes on the first query if this is skipped.
    """
    session.PyPitchSession(data_dir)

def update():
    """
    Convenience function to download and ingest latest data.
    """
    dl = DataLoader()
    dl.download()
    # In a real app, you would trigger the ingestion pipeline here
    print("Data downloaded. Run ingestion script to update DB.")

__version__ = "0.1.0"
__author__ = "PyPitch Team"