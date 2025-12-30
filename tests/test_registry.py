import unittest
from datetime import date
from pypitch.storage.registry import IdentityRegistry

class TestRegistry(unittest.TestCase):
    def setUp(self):
        self.reg = IdentityRegistry(":memory:")
        
    def test_resolution(self):
        d = date(2024, 1, 1)
        id1 = self.reg.resolve_player("V Kohli", d)
        id2 = self.reg.resolve_player("V Kohli", d)
        
        self.assertEqual(id1, id2, "ID should be persistent for same name")
        
        id3 = self.reg.resolve_player("R Sharma", d)
        self.assertNotEqual(id1, id3, "Different names should have different IDs")

if __name__ == '__main__':
    unittest.main()