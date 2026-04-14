import threading

import duckdb
from datetime import date
from typing import Optional, Dict, cast, Any

class EntityNotFoundError(Exception):
    """Raised when an entity cannot be resolved and auto-ingest is disabled."""
    pass

class IdentityRegistry:
    def __init__(self, db_path: str = "pypitch_registry.db") -> None:
        self.path = db_path
        self._lock = threading.Lock()
        self._init_db()
        self._cache: Dict[str, int] = {}

    def _init_db(self) -> None:
        if self.path == ":memory:":
            self.con = duckdb.connect(":memory:")
        else:
            self.con = duckdb.connect(self.path)
            
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS entities (
                id INTEGER PRIMARY KEY,
                type VARCHAR, -- "player", "team", "venue"
                primary_name VARCHAR
            );
            CREATE SEQUENCE IF NOT EXISTS entity_id_seq START 1;
            
            CREATE TABLE IF NOT EXISTS aliases (
                alias VARCHAR,
                entity_id INTEGER,
                valid_from DATE,
                valid_to DATE,
                PRIMARY KEY (alias, valid_from)
            );
            
            -- Light Data: Player Career Stats
            CREATE TABLE IF NOT EXISTS player_stats (
                entity_id INTEGER PRIMARY KEY,
                matches INTEGER,
                runs INTEGER,
                balls_faced INTEGER,
                wickets INTEGER,
                balls_bowled INTEGER,
                runs_conceded INTEGER
            );

            -- Light Data: Venue Stats
            CREATE TABLE IF NOT EXISTS venue_stats (
                entity_id INTEGER PRIMARY KEY,
                matches INTEGER,
                total_runs INTEGER,
                first_innings_runs INTEGER,
                first_innings_count INTEGER
            );

            -- Head-to-Head Matchup Stats
            CREATE TABLE IF NOT EXISTS matchup_stats (
                batter_id  INTEGER,
                bowler_id  INTEGER,
                balls      INTEGER,
                runs       INTEGER,
                wickets    INTEGER,
                dot_balls  INTEGER,
                boundaries INTEGER,
                sixes      INTEGER,
                PRIMARY KEY (batter_id, bowler_id)
            );
        """)

    def get_player_stats(self, player_id: int) -> Optional[Dict[str, Any]]:
        res = self.con.execute("SELECT * FROM player_stats WHERE entity_id = ?", [player_id]).fetchone()
        if res:
            return {
                "matches": res[1],
                "runs": res[2],
                "balls_faced": res[3],
                "wickets": res[4],
                "balls_bowled": res[5],
                "runs_conceded": res[6]
            }
        return None

    def get_venue_stats(self, venue_id: int) -> Optional[Dict[str, Any]]:
        res = self.con.execute("SELECT * FROM venue_stats WHERE entity_id = ?", [venue_id]).fetchone()
        if res:
            return {
                "matches": res[1],
                "total_runs": res[2],
                "avg_first_innings": res[3] / res[4] if res[4] > 0 else 0
            }
        return None

    def upsert_player_stats(self, stats: Dict[int, Dict[str, int]]) -> None:
        """Bulk upsert player stats (thread-safe, parameterised)."""
        ids = list(stats.keys())
        if not ids:
            return

        data = [
            (pid, s['matches'], s['runs'], s['balls_faced'],
             s['wickets'], s['balls_bowled'], s['runs_conceded'])
            for pid, s in stats.items()
        ]

        with self._lock:
            # Parameterised DELETE – one placeholder per id
            placeholders = ", ".join("?" for _ in ids)
            self.con.execute(
                f"DELETE FROM player_stats WHERE entity_id IN ({placeholders})",  # nosec B608
                ids,
            )
            self.con.executemany(
                "INSERT INTO player_stats VALUES (?, ?, ?, ?, ?, ?, ?)", data
            )

    def upsert_venue_stats(self, stats: Dict[int, Dict[str, int]]) -> None:
        """Bulk upsert venue stats (thread-safe, parameterised)."""
        ids = list(stats.keys())
        if not ids:
            return

        data = [
            (vid, s['matches'], s['total_runs'],
             s['first_innings_runs'], s['first_innings_count'])
            for vid, s in stats.items()
        ]

        with self._lock:
            placeholders = ", ".join("?" for _ in ids)
            self.con.execute(
                f"DELETE FROM venue_stats WHERE entity_id IN ({placeholders})",  # nosec B608
                ids,
            )
            self.con.executemany(
                "INSERT INTO venue_stats VALUES (?, ?, ?, ?, ?)", data
            )


    def get_matchup_stats(self, batter_id: int, bowler_id: int) -> Optional[Dict[str, Any]]:
        """Return head-to-head stats for a batter/bowler pair, or None if unknown."""
        res = self.con.execute(
            "SELECT balls, runs, wickets, dot_balls, boundaries, sixes "
            "FROM matchup_stats WHERE batter_id = ? AND bowler_id = ?",
            [batter_id, bowler_id],
        ).fetchone()
        if res:
            return {
                "balls": res[0], "runs": res[1], "wickets": res[2],
                "dot_balls": res[3], "boundaries": res[4], "sixes": res[5],
            }
        return None

    def upsert_matchup_stats(self, stats: Dict) -> None:
        """
        Bulk upsert head-to-head matchup stats.

        ``stats`` maps ``(batter_id, bowler_id)`` tuples to dicts with keys:
        balls, runs, wickets, dot_balls, boundaries, sixes.
        """
        if not stats:
            return
        pairs = list(stats.keys())
        data = [
            (b, bo, s["balls"], s["runs"], s["wickets"],
             s["dot_balls"], s["boundaries"], s["sixes"])
            for (b, bo), s in stats.items()
        ]
        with self._lock:
            placeholders = ", ".join("(?, ?)" for _ in pairs)
            flat_pairs = [v for pair in pairs for v in pair]
            self.con.execute(
                f"DELETE FROM matchup_stats WHERE (batter_id, bowler_id) IN ({placeholders})",  # nosec B608
                flat_pairs,
            )
            self.con.executemany(
                "INSERT INTO matchup_stats VALUES (?, ?, ?, ?, ?, ?, ?, ?)", data
            )

    def _resolve_generic(self, name: str, entity_type: str, match_date: date, auto_ingest: bool = False) -> int:
        prefix = entity_type[0].upper()
        cache_key = f"{prefix}:{name}:{match_date}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        with self._lock:
            # Double-check after acquiring lock
            if cache_key in self._cache:
                return self._cache[cache_key]

            # Check Aliases
            res = self.con.execute("""
                SELECT entity_id
                FROM aliases
                WHERE alias = ?
                  AND valid_from <= ?
                  AND (valid_to IS NULL OR valid_to >= ?)
            """, [name, match_date, match_date]).fetchone()

            if res:
                entity_id = cast(int, res[0])
                self._cache[cache_key] = entity_id
                return entity_id

            if not auto_ingest:
                raise EntityNotFoundError(
                    f"Entity '{name}' of type '{entity_type}' "
                    f"not found for date {match_date}"
                )

            # Auto-Ingest
            res_seq = self.con.execute("SELECT nextval('entity_id_seq')").fetchone()
            if not res_seq:
                raise RuntimeError("Failed to generate entity ID")
            entity_id = cast(int, res_seq[0])

            self.con.execute("INSERT INTO entities VALUES (?, ?, ?)",
                             [entity_id, entity_type, name])
            self.con.execute("""
                INSERT INTO aliases (alias, entity_id, valid_from, valid_to)
                VALUES (?, ?, ?, NULL)
            """, [name, entity_id, match_date])

            self._cache[cache_key] = entity_id
            return entity_id

    def resolve_player(self, name: str, match_date: Optional[date] = None, auto_ingest: bool = False) -> int:
        # C2: defaulting to today() for historical queries returns wrong IDs
        # when a player/team was renamed between the match date and today.
        # Callers should always pass an explicit match_date.
        if match_date is None:
            logger.warning(
                "resolve_player called without match_date for %r — "
                "defaulting to today() may return wrong ID for historical queries. "
                "Pass an explicit match_date.",
                name,
            )
            match_date = date.today()
        return self._resolve_generic(name, "player", match_date, auto_ingest)

    def resolve_venue(self, name: str, match_date: Optional[date] = None, auto_ingest: bool = False) -> int:
        if match_date is None:
            logger.warning(
                "resolve_venue called without match_date for %r — "
                "defaulting to today() may return wrong ID for historical queries. "
                "Pass an explicit match_date.",
                name,
            )
            match_date = date.today()
        return self._resolve_generic(name, "venue", match_date, auto_ingest)

    def resolve_team(self, name: str, match_date: Optional[date] = None, auto_ingest: bool = False) -> int:
        if match_date is None:
            logger.warning(
                "resolve_team called without match_date for %r — "
                "defaulting to today() may return wrong ID for historical queries. "
                "Pass an explicit match_date.",
                name,
            )
            match_date = date.today()
        return self._resolve_generic(name, "team", match_date, auto_ingest)

    def close(self) -> None:
        self.con.close()

# Alias for cleaner typing
Registry = IdentityRegistry

