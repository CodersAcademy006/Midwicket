import duckdb
import pyarrow as pa
from typing import Dict
from pypitch.schema.v1 import BALL_EVENT_SCHEMA

class StorageEngine:
    def __init__(self, db_path: str = ":memory:"):
        """
        Initializes the DuckDB engine.
        :memory: is fast but volatile. Use a path for persistence.
        """
        self.con = duckdb.connect(db_path)
        # Performance tuning for analytical workloads
        self.con.execute("PRAGMA threads=4;")
        self.con.execute("PRAGMA memory_limit='2GB';")
        
        # State tracking for deterministic hashing
        self._snapshot_id = "initial_empty"
        self._derived_versions = {"phase_stats": "v0"}

    @property
    def snapshot_id(self) -> str:
        return self._snapshot_id

    @property
    def derived_versions(self) -> Dict[str, str]:
        return self._derived_versions

    def ingest_events(self, arrow_table: pa.Table, snapshot_tag: str):
        """
        Ingests strict Schema V1 Arrow Tables.
        Rejects anything that doesn't match the contract.
        """
        if not arrow_table.schema.equals(BALL_EVENT_SCHEMA):
            # In a real system, we might diff the schemas to give a better error
            raise ValueError("Schema Violation: Input does not match BALL_EVENT_SCHEMA v1")

        # Registers the Arrow table as a queryable view in DuckDB
        # This is a zero-copy operation (pointers only)
        self.con.register('ball_events', arrow_table)
        self._snapshot_id = snapshot_tag

    def execute_sql(self, sql: str) -> pa.Table:
        """
        Executes raw SQL and returns an Arrow Table.
        """
        result = self.con.execute(sql).arrow()
        # Ensure we return a Table, not a RecordBatchReader
        if isinstance(result, pa.RecordBatchReader):
            return result.read_all()
        return result
