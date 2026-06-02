"""Resolve player names to IDs - aliases collapse to the same entity."""

from datetime import date
from midwicket.api.session import get_registry

reg, today = get_registry(), date.today()
print("V Kohli     ->", reg.resolve_player("V Kohli",     today))
print("Virat Kohli ->", reg.resolve_player("Virat Kohli", today))  # alias -> same ID
