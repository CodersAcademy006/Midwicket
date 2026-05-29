import threading

import pytest
from datetime import date
from midwicket.storage.registry import IdentityRegistry, EntityNotFoundError
from midwicket.storage import registry as _registry_module

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
    cache_key = f"P:{registry._normalize_name(name)}:{d1}"
    assert cache_key in registry._cache
    assert registry._cache[cache_key] == id1

    # Second call: Cache hit
    id2 = registry.resolve_player(name, d1)
    assert id1 == id2


# ---------------------------------------------------------------------------
# Go-17 (Medium-5): Registry fail-fast contract when match_date is None
# These tests guard against accidental reintroduction of the today() fallback.
# ---------------------------------------------------------------------------

class TestRegistryDateContract:
    """Explicit match_date is required — None must raise ValueError immediately."""

    def test_resolve_player_none_date_raises(self, registry):
        with pytest.raises(ValueError, match="match_date is required"):
            registry.resolve_player("V Kohli", None)

    def test_resolve_venue_none_date_raises(self, registry):
        with pytest.raises(ValueError, match="match_date is required"):
            registry.resolve_venue("Wankhede", None)

    def test_resolve_team_none_date_raises(self, registry):
        with pytest.raises(ValueError, match="match_date is required"):
            registry.resolve_team("Mumbai Indians", None)

    def test_resolve_player_omitted_date_raises(self, registry):
        """Calling without match_date (uses default None) must also raise."""
        with pytest.raises(ValueError, match="match_date is required"):
            registry.resolve_player("MS Dhoni")

    def test_resolve_player_valid_date_does_not_raise_value_error(self, registry):
        """With a valid date, the call proceeds (may raise EntityNotFoundError, not ValueError)."""
        with pytest.raises(Exception) as exc_info:
            registry.resolve_player("Unknown XYZ Player", date(2023, 1, 1))
        # Must NOT be a ValueError from the date-guard
        assert not isinstance(exc_info.value, ValueError) or "match_date" not in str(exc_info.value)


# ---------------------------------------------------------------------------
# Thread-safety: read methods share a single DuckDB connection (not thread-safe)
# and must serialise access via self._lock. This stresses concurrent reads to
# guard against segfaults / corrupted rows under FastAPI worker threads.
# ---------------------------------------------------------------------------

class TestRegistryConcurrentReads:
    def test_concurrent_reads_are_consistent_and_exception_free(self, registry):
        # Populate deterministic stats for a set of player/venue ids.
        player_stats = {
            pid: {
                "matches": pid,
                "runs": pid * 100,
                "balls_faced": pid * 80,
                "wickets": pid * 2,
                "balls_bowled": pid * 10,
                "runs_conceded": pid * 50,
            }
            for pid in range(1, 21)
        }
        venue_stats = {
            vid: {
                "matches": vid,
                "total_runs": vid * 200,
                "first_innings_runs": vid * 120,
                "first_innings_count": vid,
            }
            for vid in range(1, 21)
        }
        registry.upsert_player_stats(player_stats)
        registry.upsert_venue_stats(venue_stats)

        errors: list[BaseException] = []
        mismatches: list[str] = []
        barrier = threading.Barrier(8)

        def hammer_players() -> None:
            try:
                barrier.wait()
                for _ in range(200):
                    for pid in range(1, 21):
                        res = registry.get_player_stats(pid)
                        if res is None or res["runs"] != pid * 100:
                            mismatches.append(f"player {pid}: {res}")
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        def hammer_venues() -> None:
            try:
                barrier.wait()
                for _ in range(200):
                    for vid in range(1, 21):
                        res = registry.get_venue_stats(vid)
                        if res is None or res["total_runs"] != vid * 200:
                            mismatches.append(f"venue {vid}: {res}")
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=hammer_players) for _ in range(4)]
        threads += [threading.Thread(target=hammer_venues) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == [], f"concurrent reads raised: {errors!r}"
        assert mismatches == [], f"inconsistent reads: {mismatches[:5]!r}"

    def test_concurrent_resolve_does_not_deadlock(self, registry):
        """resolve_* -> _resolve_generic acquires the lock once; concurrent
        resolves must not deadlock and must return a stable id per name."""
        d1 = date(2020, 1, 1)
        registry.resolve_player("Concurrent Player", d1, auto_ingest=True)

        results: list[int] = []
        errors: list[BaseException] = []
        barrier = threading.Barrier(8)

        def resolve() -> None:
            try:
                barrier.wait()
                for _ in range(100):
                    results.append(registry.resolve_player("Concurrent Player", d1))
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=resolve) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        assert not any(t.is_alive() for t in threads), "resolve threads deadlocked"
        assert errors == [], f"concurrent resolve raised: {errors!r}"
        assert len(set(results)) == 1, f"unstable ids: {set(results)!r}"


def test_name_normalization_and_compatibility(registry):
    d = date(2020, 1, 1)
    
    # 1. Register Virat Kohli
    id_virat = registry.resolve_player("Virat Kohli", d, auto_ingest=True)
    
    # 2. Spelling variants should resolve to Virat Kohli
    assert registry.resolve_player("virat kohli", d) == id_virat
    assert registry.resolve_player("Virat  Kohli", d) == id_virat
    assert registry.resolve_player("Kohli, Virat", d) == id_virat
    assert registry.resolve_player("v kohli", d) == id_virat
    assert registry.resolve_player("V. Kohli", d) == id_virat
    assert registry.resolve_player("Kohli, V.", d) == id_virat

    # 3. Add a distinct Kohli player: Vijay Kohli
    id_vijay = registry.resolve_player("Vijay Kohli", d, auto_ingest=True)
    assert id_vijay != id_virat

    # 4. Under ambiguity ("V Kohli"), it should no longer auto-resolve to Virat Kohli (since both Virat and Vijay are compatible)
    # Wait, but since "v kohli" was already added as an alias to Virat Kohli during step 2,
    # let's test a brand new ambiguous lookup on a new registry to be sure:
    reg2 = IdentityRegistry(":memory:")
    try:
        reg2.resolve_player("Virat Kohli", d, auto_ingest=True)
        reg2.resolve_player("Vijay Kohli", d, auto_ingest=True)
        
        # "V Kohli" is ambiguous, so it should not resolve automatically (raises EntityNotFoundError)
        with pytest.raises(EntityNotFoundError):
            reg2.resolve_player("V Kohli", d)
    finally:
        reg2.close()


