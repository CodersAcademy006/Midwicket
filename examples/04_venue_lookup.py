"""Resolve a venue name to an ID."""

from datetime import date
from midwicket.api.session import get_registry

print("M Chinnaswamy Stadium ->", get_registry().resolve_venue("M Chinnaswamy Stadium", date.today()))
