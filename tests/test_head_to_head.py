import pytest
import duckdb
import pyarrow as pa
from unittest.mock import MagicMock, patch
from datetime import date

from midwicket.api.head_to_head import head_to_head, HeadToHeadSummary
from midwicket.api.session import MidwicketSession

_SCHEMA = """
CREATE TABLE ball_events (
    match_id        VARCHAR,
    date            DATE DEFAULT '2023-01-01',
    venue_id        INTEGER DEFAULT 0,
    inning          INTEGER,
    over            INTEGER,
    ball            INTEGER,
    batter_id       INTEGER DEFAULT 0,
    bowler_id       INTEGER DEFAULT 0,
    non_striker_id  INTEGER DEFAULT 0,
    batting_team_id SMALLINT DEFAULT 0,
    bowling_team_id SMALLINT DEFAULT 0,
    runs_batter     INTEGER DEFAULT 0,
    runs_extras     INTEGER DEFAULT 0,
    extras_type     VARCHAR DEFAULT NULL,
    is_wicket       BOOLEAN DEFAULT FALSE,
    wicket_type     VARCHAR DEFAULT NULL,
    phase           VARCHAR DEFAULT 'Middle',
    batter          VARCHAR DEFAULT '',
    bowler          VARCHAR DEFAULT '',
    venue           VARCHAR DEFAULT '',
    player_dismissed VARCHAR DEFAULT NULL
)
"""

def test_head_to_head_fast_path():
    # 1. Mock registry matchup_stats response
    mock_reg = MagicMock()
    mock_reg.resolve_player.side_effect = lambda name, date_ctx: 10 if "Kohli" in name else 20
    mock_reg.get_matchup_stats.return_value = {
        "balls": 30,
        "runs": 45,
        "wickets": 2,
        "dot_balls": 12,
        "boundaries": 4,
        "sixes": 1
    }
    
    with patch("midwicket.api.head_to_head.get_registry", return_value=mock_reg), \
         patch("midwicket.api.head_to_head.get_executor") as mock_exc:
        res = head_to_head("V Kohli", "JJ Bumrah")
        
    assert res.batter == "V Kohli"
    assert res.bowler == "JJ Bumrah"
    assert res.runs == 45
    assert res.balls == 30
    assert res.innings == 0  # Fast-path sets innings to 0 to avoid balls proxy bug
    assert res.dismissals == 2
    assert res.dot_balls == 12
    assert res.boundaries == 4
    assert res.sixes == 1
    assert res.strike_rate == 150.0
    assert res.average == 22.5


def test_head_to_head_slow_path_real_db():
    # Seed real in-memory DuckDB for slow path test
    con = duckdb.connect(":memory:")
    con.execute(_SCHEMA)
    
    # 10 runs, 6 balls faced, 1 dot ball, 1 four, 1 six, 1 wide (should be ignored for balls faced/dots)
    # Batter: 10, Bowler: 20
    rows = [
        # match_id, date, venue_id, inning, over, ball, batter_id, bowler_id, non_striker_id, bat_team, bowl_team, runs_batter, runs_extras, extras_type, is_wicket, wicket_type, phase, batter, bowler, venue, player_dismissed
        ("M1", "2023-05-21", 1, 1, 0, 1, 10, 20, 11, 1, 2, 4, 0, None, False, None, "Middle", "V Kohli", "JJ Bumrah", "Wankhede", None), # 4 runs
        ("M1", "2023-05-21", 1, 1, 0, 2, 10, 20, 11, 1, 2, 6, 0, None, False, None, "Middle", "V Kohli", "JJ Bumrah", "Wankhede", None), # 6 runs
        ("M1", "2023-05-21", 1, 1, 0, 3, 10, 20, 11, 1, 2, 0, 0, None, False, None, "Middle", "V Kohli", "JJ Bumrah", "Wankhede", None), # 0 runs (dot)
        ("M1", "2023-05-21", 1, 1, 0, 4, 10, 20, 11, 1, 2, 0, 1, "wides", False, None, "Middle", "V Kohli", "JJ Bumrah", "Wankhede", None), # Wide (ignored for balls, dots)
        ("M1", "2023-05-21", 1, 1, 0, 5, 10, 20, 11, 1, 2, 0, 0, None, True, "BOWLED", "Middle", "V Kohli", "JJ Bumrah", "Wankhede", "V Kohli"), # Wicket, 0 runs (ball faced, dot, dismissal)
    ]
    con.executemany("INSERT INTO ball_events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    
    mock_reg = MagicMock()
    mock_reg.resolve_player.side_effect = lambda name, date_ctx: 10 if "Kohli" in name else 20
    mock_reg.get_matchup_stats.return_value = None  # Force slow path
    
    mock_engine = MagicMock()
    mock_engine.execute_sql = lambda sql, params=None, read_only=True: pa.Table.from_pandas(con.execute(sql, params).df())
    
    session = MidwicketSession(data_dir=":memory:")
    session.engine = mock_engine
    
    with patch("midwicket.api.head_to_head.get_registry", return_value=mock_reg), \
         patch.object(MidwicketSession, "get", return_value=session):
        res = head_to_head("V Kohli", "JJ Bumrah")
        
    assert res.batter == "V Kohli"
    assert res.bowler == "JJ Bumrah"
    assert res.runs == 10
    assert res.balls == 4  # 5 rows total, 1 wide -> 4 balls faced
    assert res.innings == 1
    assert res.dismissals == 1
    assert res.dot_balls == 2  # 1 normal 0-run ball + 1 wicket ball (runs=0) = 2 dots
    assert res.boundaries == 1
    assert res.sixes == 1
    assert res.strike_rate == 250.0
    assert res.average == 10.0
