"""Download dataset and populate the registry. Run once before name-based queries."""

from datetime import date
import midwicket as md
from midwicket.data.loader import DataLoader
from midwicket.data.pipeline import build_registry_stats

DataLoader("./data").download()
session = md.init(source="./data")
build_registry_stats(session.loader, session.registry)

today = date.today()
for name in ("V Kohli", "JJ Bumrah", "Wankhede Stadium"):
    try:
        print("player", name, "->", session.registry.resolve_player(name, today))
    except Exception:
        try:    print("venue ", name, "->", session.registry.resolve_venue(name, today))
        except Exception as e: print("not found:", name, e)
