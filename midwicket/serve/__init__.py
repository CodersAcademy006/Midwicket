"""
Midwicket Serve Plugin: REST API Deployment

Deploy Midwicket as a production REST API with one command.
Perfect for enterprise integration and web applications.
"""

from .api import MidwicketAPI, serve, create_dockerfile

__all__ = [
    'MidwicketAPI',
    'serve',
    'create_dockerfile'
]