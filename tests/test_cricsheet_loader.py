"""
Test cases for CricsheetLoader zero-config ingestion.
"""
import os
import pytest
from pypitch.sources.cricsheet_loader import CricsheetLoader

def test_cricsheet_loader_match_ids(tmp_path):
    # Setup: create fake data dir with one match
    data_dir = tmp_path / "ipl"
    data_dir.mkdir(parents=True)
    match_file = data_dir / "12345.json"
    match_file.write_text('{"info": {"venue": "Test Stadium"}, "innings": [{"team": "A"}]}' )
    loader = CricsheetLoader(str(data_dir))
    match_ids = loader.get_match_ids()
    assert match_ids == ["12345"]

def test_cricsheet_loader_match_data(tmp_path):
    data_dir = tmp_path / "ipl"
    data_dir.mkdir(parents=True)
    match_file = data_dir / "54321.json"
    match_file.write_text('{"info": {"venue": "Test Stadium"}, "innings": [{"team": "B"}]}' )
    loader = CricsheetLoader(str(data_dir))
    data = loader.get_match_data("54321")
    assert data["match_id"] == "54321"
    assert data["format"] == "cricsheet"
    assert data["info"]["venue"] == "Test Stadium"
    assert data["events"][0]["team"] == "B"
    assert "raw" in data
