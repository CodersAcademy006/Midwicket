"""Inspect the Midwicket session singleton."""

from midwicket.api.session import MidwicketSession

s = MidwicketSession.get()
print("data_dir   :", s.data_dir)
print("db_path    :", s.db_path)
print("registry   :", s.registry_path)
print("snapshot_id:", s.engine.snapshot_id)
