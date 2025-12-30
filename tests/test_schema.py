import unittest
import pyarrow as pa
from pypitch.schema.v1 import BALL_EVENT_SCHEMA, SCHEMA_META

class TestSchemaContract(unittest.TestCase):
    
    def test_schema_validity(self):
        """Ensure the defined schema is a valid PyArrow schema."""
        self.assertIsInstance(BALL_EVENT_SCHEMA, pa.Schema)
        
    def test_metadata_presence(self):
        """
        CRITICAL: The runtime hashing depends on 'version' and 'frozen_at'.
        If these are missing, caching breaks.
        """
        meta = BALL_EVENT_SCHEMA.metadata
        # PyArrow stores metadata keys as bytes
        self.assertIn(b'version', meta)
        self.assertIn(b'frozen_at', meta)
        self.assertIn(b'compatibility', meta)
        
    def test_required_fields(self):
        """Ensure core columns for analytics exist."""
        field_names = BALL_EVENT_SCHEMA.names
        required = ['match_id', 'date', 'batter_id', 'phase', 'runs_batter']
        
        for req in required:
            self.assertIn(req, field_names, f"Schema missing mandatory field: {req}")

    def test_types(self):
        """Ensure specific types are correct (e.g., date32 for dates)."""
        idx = BALL_EVENT_SCHEMA.get_field_index('date')
        field = BALL_EVENT_SCHEMA.field(idx)
        self.assertTrue(pa.types.is_date32(field.type), "Date field must be date32")

if __name__ == '__main__':
    unittest.main()