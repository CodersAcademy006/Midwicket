"""
PyPitch Serve Plugin: REST API Deployment

Deploy PyPitch as a production REST API with one command.
Perfect for enterprise integration and web applications.
"""

from .api import PyPitchAPI, serve, create_dockerfile

__all__ = [
    'PyPitchAPI',
    'serve',
    'create_dockerfile'
]