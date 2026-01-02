import unittest
import pyarrow as pa
from pypitch.schema.v1 import BALL_EVENT_SCHEMA, SCHEMA_META
from pypitch.core.match_config import MatchConfig

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

class TestMatchConfig(unittest.TestCase):
    
    def test_standard_configs(self):
        """Test that standard cricket formats have correct player counts."""
        t20 = MatchConfig.t20()
        self.assertEqual(t20.max_players_per_team, 11)
        self.assertEqual(t20.total_overs, 20)
        
        odi = MatchConfig.odi()
        self.assertEqual(odi.max_players_per_team, 11)
        self.assertEqual(odi.total_overs, 50)
    
    def test_impact_player_config(self):
        """Test that Impact Player configuration allows 12 players."""
        impact = MatchConfig.t20_impact_player()
        self.assertEqual(impact.max_players_per_team, 12)
        self.assertEqual(impact.total_overs, 20)
        self.assertEqual(impact.balls_per_over, 6)
    
    def test_custom_config(self):
        """Test that custom configurations work."""
        custom = MatchConfig(total_overs=10, balls_per_over=6, max_players_per_team=15)
        self.assertEqual(custom.max_players_per_team, 15)
        self.assertEqual(custom.total_balls, 60)

if __name__ == '__main__':
    unittest.main()