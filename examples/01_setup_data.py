"""Force re-download / re-ingestion of the bundled dataset (optional, auto-runs on first use)."""

from midwicket.api.session import MidwicketSession

MidwicketSession.get()._setup_db()
