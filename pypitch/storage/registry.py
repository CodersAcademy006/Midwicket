import duckdb
from datetime import date
from typing import Optional

class EntityNotFoundError(Exception):
    """Raised when an entity cannot be resolved and auto-ingest is disabled."""
    pass

class IdentityRegistry:
    def __init__(self, db_path: str = "pypitch_registry.db"):
        self.path = db_path
        self._init_db()
        self._cache = {}

    def _init_db(self):
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
        """)

    def _resolve_generic(self, name: str, entity_type: str, match_date: date, auto_ingest: bool = False) -> int:
        prefix = entity_type[0].upper()
        cache_key = f"{prefix}:{name}:{match_date}"
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
            entity_id = res[0]
            self._cache[cache_key] = entity_id
            return entity_id

        if not auto_ingest:
            raise EntityNotFoundError(f"Entity '{name}' of type '{entity_type}' not found for date {match_date}")

        # Auto-Ingest
        entity_id = self.con.execute("SELECT nextval('entity_id_seq')").fetchone()[0]
        
        self.con.execute("INSERT INTO entities VALUES (?, ?, ?)", [entity_id, entity_type, name])
        self.con.execute("""
            INSERT INTO aliases (alias, entity_id, valid_from, valid_to)
            VALUES (?, ?, ?, NULL)
        """, [name, entity_id, match_date])
        
        self._cache[cache_key] = entity_id
        return entity_id

    def resolve_player(self, name: str, match_date: date, auto_ingest: bool = False) -> int:
        return self._resolve_generic(name, "player", match_date, auto_ingest)

    def resolve_venue(self, name: str, match_date: Optional[date] = None, auto_ingest: bool = False) -> int:
        if match_date is None:
            match_date = date.today()
        return self._resolve_generic(name, "venue", match_date, auto_ingest)

    def resolve_team(self, name: str, match_date: Optional[date] = None, auto_ingest: bool = False) -> int:
        if match_date is None:
            match_date = date.today()
        return self._resolve_generic(name, "team", match_date, auto_ingest)

    def close(self):
        self.con.close()

