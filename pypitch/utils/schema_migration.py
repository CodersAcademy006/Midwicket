"""
Schema Migration Utility for PyPitch.
Automatically heals local Parquet/DB files to match the latest schema.
"""
import logging
import os

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


def migrate_schema(parquet_path: str, latest_schema: pa.Schema) -> None:
    """
    Checks and updates the Parquet file to match the latest schema.
    Adds missing columns with nulls if needed.
    """
    if not os.path.exists(parquet_path):
        logger.warning("File not found: %s", parquet_path)
        return
    table = pq.read_table(parquet_path)
    current_schema = table.schema
    missing = [f for f in latest_schema.names if f not in current_schema.names]
    if not missing:
        logger.debug("%s is up to date.", parquet_path)
        return
    # Add missing columns with nulls
    for col in missing:
        table = table.append_column(col, pa.array([None] * table.num_rows, type=latest_schema.field(col).type))
    pq.write_table(table, parquet_path)
    logger.info("Migrated %s to latest schema.", parquet_path)
