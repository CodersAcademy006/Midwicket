import sqlite3
from datetime import date
from typing import Optional, Tuple

class IdentityRegistry:
    def __init__(self, db_path: str = "pypitch_registry.db"):
        self.conn = sqlite3.connect(db_path)
        self._init_db()
        # Cache to avoid millions of SQL hits during ingestion
        self._cache = {}

    def _init_db(self):
        """Schema for Identity Management"""
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS players (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                primary_name TEXT UNIQUE
            );
            CREATE TABLE IF NOT EXISTS teams (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                primary_name TEXT UNIQUE
            );
            CREATE TABLE IF NOT EXISTS venues (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                primary_name TEXT UNIQUE
            );
            -- Aliases handle "R Pant" -> "Rishabh Pant"
            CREATE TABLE IF NOT EXISTS aliases (
                name TEXT PRIMARY KEY,
                entity_id INTEGER,
                entity_type TEXT -- 'player', 'team', 'venue'
            );
        """)
        self.conn.commit()

    def _get_cached_id(self, key: str) -> Optional[int]:
        return self._cache.get(key)

    def resolve_player(self, name: str, match_date: date) -> int:
        """
        Resolves a player name to an ID.
        If the player is new, creates a new ID (Auto-Ingest).
        """
        # 1. Check Memory Cache
        cache_key = f"P:{name}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        cursor = self.conn.cursor()
        
        # 2. Check Aliases/Exact Match (Simplified for Stage 2)
        # In a full system, we'd check the 'aliases' table first.
        cursor.execute("SELECT id FROM players WHERE primary_name = ?", (name,))
        row = cursor.fetchone()
        
        if row:
            p_id = row[0]
        else:
            # 3. Create New (Write on Read for Ingestion)
            cursor.execute("INSERT INTO players (primary_name) VALUES (?)", (name,))
            p_id = cursor.lastrowid
            self.conn.commit()
        
        # 4. Update Cache
        self._cache[cache_key] = p_id
        return p_id

    def resolve_team(self, name: str) -> int:
        cache_key = f"T:{name}"
        if cache_key in self._cache: return self._cache[cache_key]
        
        cursor = self.conn.cursor()
        cursor.execute("SELECT id FROM teams WHERE primary_name = ?", (name,))
        row = cursor.fetchone()
        
        if row:
            t_id = row[0]
        else:
            cursor.execute("INSERT INTO teams (primary_name) VALUES (?)", (name,))
            t_id = cursor.lastrowid
            self.conn.commit()
            
        self._cache[cache_key] = t_id
        return t_id

    def resolve_venue(self, name: str) -> int:
        cache_key = f"V:{name}"
        if cache_key in self._cache: return self._cache[cache_key]

        cursor = self.conn.cursor()
        cursor.execute("SELECT id FROM venues WHERE primary_name = ?", (name,))
        row = cursor.fetchone()

        if row:
            v_id = row[0]
        else:
            cursor.execute("INSERT INTO venues (primary_name) VALUES (?)", (name,))
            v_id = cursor.lastrowid
            self.conn.commit()
        self._cache[cache_key] = v_id
        return v_id
