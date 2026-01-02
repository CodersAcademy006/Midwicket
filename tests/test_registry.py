import pytest
from datetime import date
from pypitch.storage.registry import IdentityRegistry, EntityNotFoundError

@pytest.fixture
def registry():
    # Use in-memory DB for testing
    reg = IdentityRegistry(":memory:")
    yield reg
    reg.close()

def test_auto_ingest_disabled_by_default(registry):
    d1 = date(2020, 1, 1)
    # Should raise error by default
    with pytest.raises(EntityNotFoundError):
        registry.resolve_player("Unknown Player", d1)

def test_auto_ingest_explicit(registry):
    # 1. Resolve a new player with auto_ingest=True
    d1 = date(2020, 1, 1)
    id1 = registry.resolve_player("Virat Kohli", d1, auto_ingest=True)
    
    assert isinstance(id1, int)
    assert id1 > 0
    
    # 2. Resolve same player, same date -> Same ID (Cache hit or DB hit)
    # Should work without auto_ingest now because it exists
    id2 = registry.resolve_player("Virat Kohli", d1)
    assert id1 == id2

def test_temporal_resolution(registry):
    # 1. "Delhi Daredevils" exists in 2012
    # Note: We need to manually seed aliases for this test since auto-ingest 
    # just creates a new entity for every new name.
    
    # Manually insert a team with two aliases
    registry.con.execute("INSERT INTO entities (id, type, primary_name) VALUES (10, 'team', 'Delhi Capitals')")
    registry.con.execute("INSERT INTO aliases VALUES ('Delhi Daredevils', 10, '2008-01-01', '2018-12-31')")
    registry.con.execute("INSERT INTO aliases VALUES ('Delhi Capitals', 10, '2019-01-01', NULL)")
    
    # 2. Resolve "Delhi Daredevils" in 2012
    id_2012 = registry.resolve_team("Delhi Daredevils", date(2012, 5, 1))
    assert id_2012 == 10
    
    # 3. Resolve "Delhi Capitals" in 2020
    id_2020 = registry.resolve_team("Delhi Capitals", date(2020, 5, 1))
    assert id_2020 == 10
    
    # 4. Resolve "Delhi Daredevils" in 2020 -> Should fail
    with pytest.raises(EntityNotFoundError):
        registry.resolve_team("Delhi Daredevils", date(2020, 5, 1))

def test_cache_behavior(registry):
    d1 = date(2021, 1, 1)
    name = "Rishabh Pant"
    
    # First call: DB hit + Cache set
    id1 = registry.resolve_player(name, d1, auto_ingest=True)
    
    # Verify it"s in cache
    cache_key = f"P:{name}:{d1}"
    assert cache_key in registry._cache
    assert registry._cache[cache_key] == id1
    
    # Second call: Cache hit
    id2 = registry.resolve_player(name, d1)
    assert id1 == id2

