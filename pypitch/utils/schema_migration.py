"""
Schema Migration Utility for PyPitch.
Automatically heals local Parquet/DB files to match the latest schema.
"""
import pyarrow as pa
import pyarrow.parquet as pq
import os

def migrate_schema(parquet_path: str, latest_schema: pa.Schema):
    """
    Checks and updates the Parquet file to match the latest schema.
    Adds missing columns with nulls if needed.
    """
    if not os.path.exists(parquet_path):
        print(f"[SchemaMigration] File not found: {parquet_path}")
        return
    table = pq.read_table(parquet_path)
    current_schema = table.schema
    missing = [f for f in latest_schema.names if f not in current_schema.names]
    if not missing:
        print(f"[SchemaMigration] {parquet_path} is up to date.")
        return
    # Add missing columns with nulls
    for col in missing:
        table = table.append_column(col, pa.array([None] * table.num_rows, type=latest_schema.field(col).type))
    pq.write_table(table, parquet_path)
    print(f"[SchemaMigration] Migrated {parquet_path} to latest schema.")
