from pypitch.api import session
import pypitch.data as data

def init(source: str = None):
    """
    Initialize the PyPitch session.
    """
    session.PyPitchSession(data_dir=source)
