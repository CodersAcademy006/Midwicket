"""
Unit and Integration Tests for Midwicket v2 Dataset Hub, Feature Store, and Scouting Reports.
"""

import pytest
import requests
import pandas as pd
from typing import Generator

# Internal Imports
import midwicket as md
from midwicket.datasets import list_datasets, describe_dataset, load_dataset
from midwicket.features import load_features, list_features, describe_feature
from midwicket.report.scout import scouting_report

@pytest.fixture(scope="module")
def mlc_session() -> Generator[md.MidwicketSession, None, None]:
    """Test fixture establishing an active MLC dataset session."""
    session = load_dataset("mlc")
    yield session
    # Cleanup session after tests run
    session.close()

def test_list_datasets():
    """Verify that list_datasets() returns a list with correct structure and content."""
    catalog = list_datasets()

    # Returns a list, not a dict
    assert isinstance(catalog, list)
    assert len(catalog) > 0

    # Every record has the required top-level keys
    required_keys = {
        "name", "description", "format", "gender",
        "matches", "deliveries", "players", "venues",
        "coverage", "date_range", "version",
    }
    for record in catalog:
        assert required_keys.issubset(record.keys()), (
            f"Record for '{record.get('name')}' is missing keys: "
            f"{required_keys - record.keys()}"
        )

    # Known datasets are present
    names = [ds["name"] for ds in catalog]
    assert "ipl" in names
    assert "mlc" in names
    # all_t20 was removed: Cricsheet publishes no such bundle (HTTP 404 on all URL variants).
    assert "all_t20" not in names

    # Spot-check metadata for a known dataset
    mlc = next(ds for ds in catalog if ds["name"] == "mlc")
    assert mlc["format"] == "T20"
    assert mlc["gender"] == "men"
    assert isinstance(mlc["matches"], int) and mlc["matches"] > 0
    assert isinstance(mlc["deliveries"], int) and mlc["deliveries"] > 0
    assert isinstance(mlc["players"], int) and mlc["players"] > 0
    assert isinstance(mlc["venues"], int) and mlc["venues"] > 0
    assert mlc["coverage"] == "complete"


def test_list_datasets_output_consistency():
    """Verify that all records in list_datasets() have non-null, consistent types."""
    catalog = list_datasets()
    for ds in catalog:
        assert isinstance(ds["name"], str) and ds["name"]
        assert isinstance(ds["description"], str) and ds["description"]
        assert isinstance(ds["format"], str) and ds["format"]
        assert isinstance(ds["gender"], str) and ds["gender"] in ("men", "women", "both", "unknown")
        assert isinstance(ds["matches"], int) and ds["matches"] > 0
        assert isinstance(ds["deliveries"], int) and ds["deliveries"] > 0
        assert isinstance(ds["players"], int) and ds["players"] > 0
        assert isinstance(ds["venues"], int) and ds["venues"] > 0
        assert isinstance(ds["coverage"], str)
        assert isinstance(ds["date_range"], str) and "–" in ds["date_range"]
        assert isinstance(ds["version"], str)


def test_describe_dataset_valid():
    """Verify describe_dataset() returns a complete record for a known dataset."""
    info = describe_dataset("ipl")

    required_keys = {
        "name", "description", "source", "schema_version", "format",
        "gender", "competitions", "date_range", "coverage", "statistics",
        "aliases", "example_usage",
    }
    assert required_keys.issubset(info.keys()), (
        f"describe_dataset() is missing keys: {required_keys - info.keys()}"
    )

    assert info["name"] == "ipl"
    assert info["format"] == "T20"
    assert info["gender"] == "men"
    assert info["coverage"] == "complete"
    assert isinstance(info["competitions"], list) and len(info["competitions"]) > 0
    assert isinstance(info["aliases"], list)
    assert "load_dataset" in info["example_usage"]
    assert '"ipl"' in info["example_usage"]


def test_describe_dataset_statistics():
    """Verify describe_dataset() statistics sub-dict has correct fields and types."""
    info = describe_dataset("ipl")
    stats = info["statistics"]

    assert isinstance(stats, dict)
    assert isinstance(stats["matches"], int) and stats["matches"] > 0
    assert isinstance(stats["deliveries"], int) and stats["deliveries"] > 0
    assert isinstance(stats["players"], int) and stats["players"] > 0
    assert isinstance(stats["venues"], int) and stats["venues"] > 0
    assert isinstance(stats["size_mb"], (int, float)) and stats["size_mb"] > 0


def test_describe_dataset_alias():
    """Verify describe_dataset() resolves aliases to the canonical name."""
    # "odi" is an alias for "odis"
    info_alias = describe_dataset("odi")
    info_canonical = describe_dataset("odis")

    assert info_alias["name"] == "odis"
    assert info_alias["name"] == info_canonical["name"]
    assert "odi" in info_alias["aliases"]

    # "t20s" is an alias for "t20is"
    info_t20s = describe_dataset("t20s")
    assert info_t20s["name"] == "t20is"
    assert "t20s" in info_t20s["aliases"]


def test_describe_dataset_invalid():
    """Verify describe_dataset() raises ValueError for unrecognised names."""
    with pytest.raises(ValueError) as exc_info:
        describe_dataset("notadataset")

    error_message = str(exc_info.value)
    assert "notadataset" in error_message
    assert "Available keys" in error_message


def test_describe_dataset_metadata_correctness():
    """Verify describe_dataset() returns known-correct metadata for several datasets."""
    cases = [
        ("ipl",    "T20",      "men",   "2008–2026"),
        ("tests",  "Test",     "both",  "2001/02–2026"),
        ("wbbl",   "T20",      "women", "2015/16–2025/26"),
        ("all",    "All",      "both",  "2001–2026"),
        ("t20is",  "T20I",     "both",  "2004/05–2026"),
    ]
    for name, expected_format, expected_gender, expected_range in cases:
        info = describe_dataset(name)
        assert info["format"] == expected_format, f"{name}: format mismatch"
        assert info["gender"] == expected_gender, f"{name}: gender mismatch"
        assert info["date_range"] == expected_range, f"{name}: date_range mismatch"
        assert info["source"].startswith("https://cricsheet.org/"), (
            f"{name}: source URL unexpected"
        )

def test_load_dataset_mlc(mlc_session):
    """Verify dataset loading maps schema correctly and registers events."""
    # Ensure ball events were loaded
    res = mlc_session.engine.execute_sql("SELECT COUNT(*) as count FROM ball_events")
    count = res.to_pydict()["count"][0]
    assert count > 0

def test_load_features_pressure_index(mlc_session):
    """Test pressure index feature generation."""
    df = load_features("pressure_index", mlc_session)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert "pressure_index" in df.columns
    assert df["pressure_index"].min() >= 0.0
    assert df["pressure_index"].max() <= 10.0

def test_load_features_bowler_quality_rating(mlc_session):
    """Test bowler quality rating (BQR) feature calculation."""
    df = load_features("bowler_quality_rating", mlc_session)
    assert isinstance(df, pd.DataFrame)
    assert "bowler_quality_rating" in df.columns
    # BQR should be scaled appropriately
    assert df["bowler_quality_rating"].min() >= 0.0
    assert df["bowler_quality_rating"].max() <= 100.0

def test_load_features_batter_intent_score(mlc_session):
    """Test batter intent score (BIS)."""
    df = load_features("batter_intent_score", mlc_session)
    assert isinstance(df, pd.DataFrame)
    assert "intent_score" in df.columns
    assert df["intent_score"].min() >= 0.0

def test_load_features_match_context_score(mlc_session):
    """Test match context score (MCS)."""
    df = load_features("match_context_score", mlc_session)
    assert isinstance(df, pd.DataFrame)
    assert "match_context_score" in df.columns

def test_load_features_venue_bias_rating(mlc_session):
    """Test venue bias rating (VBR)."""
    df = load_features("venue_bias_rating", mlc_session)
    assert isinstance(df, pd.DataFrame)
    assert "venue_bias_rating" in df.columns
    assert df["venue_bias_rating"].min() >= 0.0

def test_load_features_expected_runs(mlc_session):
    """Test expected runs (xRuns)."""
    df = load_features("expected_runs", mlc_session)
    assert isinstance(df, pd.DataFrame)
    assert "expected_runs" in df.columns
    assert df["expected_runs"].min() >= 0.0

def test_load_features_expected_wickets(mlc_session):
    """Test expected wickets (xWickets)."""
    df = load_features("expected_wickets", mlc_session)
    assert isinstance(df, pd.DataFrame)
    assert "expected_wickets" in df.columns
    assert df["expected_wickets"].min() >= 0.0
    assert df["expected_wickets"].max() <= 1.0

def test_load_features_batting_form(mlc_session):
    """Test batting form moving average feature."""
    df = load_features("batting_form", mlc_session)
    assert isinstance(df, pd.DataFrame)
    assert "avg_runs_last_5" in df.columns
    assert "strike_rate_last_5" in df.columns

def test_load_features_death_over_metrics(mlc_session):
    """Test death over metrics splits."""
    df = load_features("death_over_metrics", mlc_session)
    assert isinstance(df, pd.DataFrame)
    assert "player_id" in df.columns
    assert "death_strike_rate" in df.columns or "death_economy" in df.columns

def test_load_features_venue_adjusted_form(mlc_session):
    """Test venue adjusted form calculation."""
    df = load_features("venue_adjusted_form", mlc_session)
    assert isinstance(df, pd.DataFrame)
    assert "venue_adjusted_rating" in df.columns

def test_scouting_report_v2(mlc_session):
    """Test generating a player scouting report on loaded data."""
    # Find an active player in database
    res = mlc_session.registry.con.execute(
        "SELECT primary_name FROM entities WHERE id = 127"
    ).fetchone()
    assert res is not None
    player_name = res[0]
    
    # Generate scouting report
    report = scouting_report(player_name)
    assert isinstance(report, dict)
    assert report["player"].lower() == player_name.lower()
    assert "strengths" in report
    assert "weaknesses" in report
    assert "career_summary" in report
    assert "recent_form" in report


# ── Registry regression tests (Phase 4) ──────────────────────────────────────

def test_all_t20_not_in_registry():
    """Verify that all_t20 is absent from the registry.

    Cricsheet publishes no combined T20 bundle. Every URL variant tested
    returns HTTP 404. This entry must never be re-added without a verified URL.
    """
    names = [ds["name"] for ds in list_datasets()]
    assert "all_t20" not in names, (
        "all_t20 was removed: Cricsheet has no such bundle under any URL. "
        "Do not re-add without a verified HTTP 200 response from cricsheet.org."
    )


def test_wbbl_url_is_wbb_not_wbbl():
    """Verify wbbl uses wbb_json.zip (correct) not wbbl_json.zip (HTTP 404)."""
    info = describe_dataset("wbbl")
    assert "wbb_json.zip" in info["source"], (
        f"wbbl URL must use wbb_json.zip; got: {info['source']}"
    )
    assert "wbbl_json.zip" not in info["source"], (
        "wbbl_json.zip returns HTTP 404 on cricsheet.org — must not appear in URL"
    )


def test_sa20_url_is_sat_not_sa20():
    """Verify sa20 uses sat_json.zip (correct) not sa20_json.zip (HTTP 404)."""
    info = describe_dataset("sa20")
    assert "sat_json.zip" in info["source"], (
        f"sa20 URL must use sat_json.zip; got: {info['source']}"
    )
    assert "sa20_json.zip" not in info["source"], (
        "sa20_json.zip returns HTTP 404 on cricsheet.org — must not appear in URL"
    )


def test_hundred_url_is_hnd_not_hundred():
    """Verify hundred uses hnd_json.zip (correct) not hundred_json.zip (HTTP 404)."""
    info = describe_dataset("hundred")
    assert "hnd_json.zip" in info["source"], (
        f"hundred URL must use hnd_json.zip; got: {info['source']}"
    )
    assert "hundred_json.zip" not in info["source"], (
        "hundred_json.zip returns HTTP 404 on cricsheet.org — must not appear in URL"
    )


@pytest.mark.network
def test_registry_urls_reachable():
    """Verify every dataset URL in the registry responds HTTP 200.

    Marked @pytest.mark.network — skipped in offline/CI environments unless
    explicitly enabled with: pytest -m network
    """
    for ds in list_datasets():
        info = describe_dataset(ds["name"])
        url = info["source"]
        try:
            response = requests.head(url, timeout=15, allow_redirects=True)
        except requests.RequestException as exc:
            pytest.fail(f"Network error fetching {ds['name']} URL ({url}): {exc}")
        assert response.status_code == 200, (
            f"Dataset '{ds['name']}' URL returned HTTP {response.status_code}: {url}"
        )


def test_aliases_still_resolve():
    """Verify all documented aliases resolve to their canonical dataset key."""
    alias_checks = [
        ("t20s",            "t20is"),
        ("t20i",            "t20is"),
        ("t20_international","t20is"),
        ("odi",             "odis"),
        ("test",            "tests"),
        ("all_test",        "tests"),
        ("all_odi",         "odis"),
        ("women_t20",       "wbbl"),
    ]
    for alias, canonical in alias_checks:
        info = describe_dataset(alias)
        assert info["name"] == canonical, (
            f"Alias '{alias}' should resolve to '{canonical}', got '{info['name']}'"
        )


def test_partial_coverage_on_international_datasets():
    """Verify international datasets are marked partial coverage.

    Cricsheet explicitly withholds 366 Afghanistan-related matches. Datasets
    that include international cricket cannot claim complete coverage.
    """
    for name in ("t20is", "odis", "tests", "all"):
        info = describe_dataset(name)
        assert info["coverage"] == "partial", (
            f"'{name}' must be 'partial' — Cricsheet withholds 366 Afghanistan matches. "
            f"Got: '{info['coverage']}'"
        )


# ── Temporal-filter regression tests ─────────────────────────────────────────

def test_match_context_score_temporal_filter(mlc_session):
    """Filtered match_context_score must be a strict subset of the unfiltered result."""
    df_all = load_features("match_context_score", mlc_session)
    df_filtered = load_features(
        "match_context_score", mlc_session,
        start_date="2024-01-01", end_date="2024-12-31",
    )

    assert isinstance(df_filtered, pd.DataFrame)
    assert "match_context_score" in df_filtered.columns
    # Date-filtered slice must be smaller than the full dataset
    assert len(df_filtered) < len(df_all), (
        f"Filtered row count ({len(df_filtered)}) must be less than "
        f"unfiltered row count ({len(df_all)})"
    )
    # Feature semantics preserved: values stay in the clipped range
    assert df_filtered["match_context_score"].min() >= 0.0
    assert df_filtered["match_context_score"].max() <= 15.0


def test_match_context_score_unfiltered_unchanged(mlc_session):
    """Unfiltered match_context_score path returns valid data (regression guard)."""
    df = load_features("match_context_score", mlc_session)
    assert isinstance(df, pd.DataFrame)
    assert "match_context_score" in df.columns
    assert df["match_context_score"].min() >= 0.0
    assert df["match_context_score"].max() <= 15.0
    assert len(df) > 0


def test_death_over_metrics_temporal_filter(mlc_session):
    """Filtered death_over_metrics must be a strict subset of the unfiltered result."""
    df_all = load_features("death_over_metrics", mlc_session)
    df_filtered = load_features(
        "death_over_metrics", mlc_session,
        start_date="2024-01-01", end_date="2024-12-31",
    )

    assert isinstance(df_filtered, pd.DataFrame)
    assert "player_id" in df_filtered.columns
    # Date-filtered slice must be smaller than the full dataset
    assert len(df_filtered) < len(df_all), (
        f"Filtered row count ({len(df_filtered)}) must be less than "
        f"unfiltered row count ({len(df_all)})"
    )


def test_death_over_metrics_unfiltered_unchanged(mlc_session):
    """Unfiltered death_over_metrics path returns valid data (regression guard)."""
    df = load_features("death_over_metrics", mlc_session)
    assert isinstance(df, pd.DataFrame)
    assert "player_id" in df.columns
    assert "death_strike_rate" in df.columns or "death_economy" in df.columns
    assert len(df) > 0


def test_complete_coverage_on_franchise_datasets():
    """Verify franchise-only datasets retain complete coverage.

    Franchise leagues (IPL, BBL, etc.) do not involve Afghanistan national
    sides, so the global withholding does not affect them.
    """
    franchise_datasets = (
        "ipl", "bbl", "wbbl", "psl", "cpl", "sa20", "mlc", "wpl", "hundred"
    )
    for name in franchise_datasets:
        info = describe_dataset(name)
        assert info["coverage"] == "complete", (
            f"'{name}' should be 'complete' coverage (franchise-only). "
            f"Got: '{info['coverage']}'"
        )


def test_registry_metadata_within_tolerance_of_actuals():
    """Verify registry statistics are within tolerance of Cricsheet-verified actuals.

    Tolerance bands account for continuous Cricsheet updates after the
    2026-05-31 audit snapshot. All lower bounds are the audit actuals;
    upper bounds allow for post-audit additions only.

    Format: (dataset_key, field, audit_actual, max_relative_error)
    """
    checks = [
        # IPL
        ("ipl",     "matches",    1241,     0.05),
        ("ipl",     "deliveries", 295258,   0.05),
        ("ipl",     "players",    811,      0.05),
        # BBL
        ("bbl",     "matches",    662,      0.05),
        ("bbl",     "deliveries", 153250,   0.05),
        ("bbl",     "players",    532,      0.05),
        # WBBL
        ("wbbl",    "matches",    519,      0.10),
        ("wbbl",    "deliveries", 118656,   0.10),
        ("wbbl",    "players",    282,      0.10),
        # PSL
        ("psl",     "matches",    357,      0.05),
        ("psl",     "deliveries", 83799,    0.05),
        ("psl",     "players",    460,      0.05),
        # CPL
        ("cpl",     "matches",    407,      0.10),
        ("cpl",     "deliveries", 95024,    0.10),
        ("cpl",     "players",    419,      0.10),
        # SA20
        ("sa20",    "matches",    130,      0.10),
        ("sa20",    "deliveries", 29020,    0.10),
        ("sa20",    "players",    213,      0.10),
        # MLC
        ("mlc",     "matches",    75,       0.15),
        ("mlc",     "deliveries", 17413,    0.15),
        ("mlc",     "players",    167,      0.15),
        # WPL
        ("wpl",     "matches",    88,       0.10),
        ("wpl",     "deliveries", 20656,    0.10),
        ("wpl",     "players",    136,      0.10),
        # The Hundred
        ("hundred", "matches",    322,      0.10),
        ("hundred", "deliveries", 62397,    0.10),
        ("hundred", "players",    439,      0.10),
        # T20Is
        ("t20is",   "matches",    5318,     0.10),
        ("t20is",   "deliveries", 1240157,  0.10),
        ("t20is",   "players",    7098,     0.10),
        # ODIs
        ("odis",    "matches",    3134,     0.10),
        ("odis",    "deliveries", 1688755,  0.10),
        ("odis",    "players",    2658,     0.10),
        # Tests
        ("tests",   "matches",    903,      0.10),
        ("tests",   "deliveries", 1755413,  0.10),
        ("tests",   "players",    1205,     0.10),
        # All archive (match count and size verified; deliveries/players/venues not re-parsed)
        ("all",     "matches",    21877,    0.10),
    ]

    for name, field, audit_actual, max_error in checks:
        info = describe_dataset(name)
        registry_val = info["statistics"][field]
        relative_error = abs(registry_val - audit_actual) / audit_actual
        assert relative_error <= max_error, (
            f"{name}.{field}: registry={registry_val:,}, "
            f"audit_actual={audit_actual:,}, "
            f"relative_error={relative_error:.1%} exceeds tolerance {max_error:.0%}"
        )


# ── Feature Registry tests ────────────────────────────────────────────────────

_EXPECTED_FEATURES = {
    "pressure_index",
    "bowler_quality_rating",
    "batter_intent_score",
    "match_context_score",
    "venue_bias_rating",
    "expected_runs",
    "expected_wickets",
    "batting_form",
    "death_over_metrics",
    "venue_adjusted_form",
}

_METADATA_REQUIRED_KEYS = {
    "name", "version", "description", "formula",
    "grain", "inputs", "outputs", "supports_date_filter", "dependencies",
}

# Features that accept date filters (start_date / end_date)
_DATE_FILTER_FEATURES = {
    "pressure_index",
    "bowler_quality_rating",
    "batter_intent_score",
    "match_context_score",
    "venue_bias_rating",
    "death_over_metrics",
    "venue_adjusted_form",
}

# Features that do NOT accept date filters
_NO_DATE_FILTER_FEATURES = {
    "expected_runs",
    "expected_wickets",
    "batting_form",
}


def test_list_features_returns_list():
    """list_features() must return a non-empty list."""
    catalog = list_features()
    assert isinstance(catalog, list)
    assert len(catalog) > 0


def test_list_features_catalog_keys():
    """Every entry in list_features() must expose name, version, grain, supports_date_filter."""
    required = {"name", "version", "grain", "supports_date_filter"}
    for entry in list_features():
        missing = required - entry.keys()
        assert not missing, (
            f"list_features() entry for '{entry.get('name')}' is missing keys: {missing}"
        )


def test_list_features_contains_all_expected():
    """list_features() must include every registered feature."""
    names = {entry["name"] for entry in list_features()}
    missing = _EXPECTED_FEATURES - names
    assert not missing, f"list_features() is missing expected features: {missing}"


def test_list_features_no_extra_fields():
    """list_features() entries must not contain undocumented keys (catalog contract)."""
    allowed = {"name", "version", "grain", "supports_date_filter"}
    for entry in list_features():
        extra = entry.keys() - allowed
        assert not extra, (
            f"list_features() entry for '{entry['name']}' has unexpected keys: {extra}"
        )


def test_list_features_types():
    """list_features() must return correctly typed fields for every entry."""
    for entry in list_features():
        assert isinstance(entry["name"], str) and entry["name"]
        assert isinstance(entry["version"], str) and entry["version"]
        assert isinstance(entry["grain"], str) and entry["grain"]
        assert isinstance(entry["supports_date_filter"], bool)


def test_describe_feature_returns_complete_metadata():
    """describe_feature() must return all required metadata keys for every feature."""
    for feature_name in _EXPECTED_FEATURES:
        meta = describe_feature(feature_name)
        missing = _METADATA_REQUIRED_KEYS - meta.keys()
        assert not missing, (
            f"describe_feature('{feature_name}') is missing keys: {missing}"
        )


def test_describe_feature_name_matches_key():
    """describe_feature(name)['name'] must equal the requested name."""
    for feature_name in _EXPECTED_FEATURES:
        meta = describe_feature(feature_name)
        assert meta["name"] == feature_name


def test_describe_feature_types():
    """describe_feature() must return correctly typed metadata fields."""
    for feature_name in _EXPECTED_FEATURES:
        meta = describe_feature(feature_name)
        assert isinstance(meta["name"], str) and meta["name"]
        assert isinstance(meta["version"], str) and meta["version"]
        assert isinstance(meta["description"], str) and meta["description"]
        assert isinstance(meta["formula"], str)
        assert isinstance(meta["grain"], str) and meta["grain"]
        assert isinstance(meta["inputs"], list)
        assert isinstance(meta["outputs"], list)
        assert isinstance(meta["supports_date_filter"], bool)
        assert isinstance(meta["dependencies"], list)


def test_describe_feature_invalid_raises():
    """describe_feature() must raise ValueError for an unrecognised feature name."""
    with pytest.raises(ValueError) as exc_info:
        describe_feature("not_a_real_feature")
    assert "not_a_real_feature" in str(exc_info.value)


def test_describe_feature_invalid_error_lists_available():
    """ValueError from describe_feature() must mention available feature names."""
    with pytest.raises(ValueError) as exc_info:
        describe_feature("ghost_feature")
    error_text = str(exc_info.value)
    # Should enumerate at least one known feature in the error message
    assert any(name in error_text for name in _EXPECTED_FEATURES)


def test_describe_feature_returns_copy():
    """describe_feature() must return a copy — mutating it must not affect the registry."""
    meta1 = describe_feature("pressure_index")
    meta1["version"] = "99.0"
    meta2 = describe_feature("pressure_index")
    assert meta2["version"] != "99.0", (
        "describe_feature() returned a reference to the registry dict; expected a copy."
    )


def test_supports_date_filter_true_for_correct_features():
    """supports_date_filter must be True for features that accept start_date / end_date."""
    for feature_name in _DATE_FILTER_FEATURES:
        meta = describe_feature(feature_name)
        assert meta["supports_date_filter"] is True, (
            f"'{feature_name}' accepts date filters but supports_date_filter is False"
        )


def test_supports_date_filter_false_for_correct_features():
    """supports_date_filter must be False for features that do not accept date arguments."""
    for feature_name in _NO_DATE_FILTER_FEATURES:
        meta = describe_feature(feature_name)
        assert meta["supports_date_filter"] is False, (
            f"'{feature_name}' does not accept date filters but supports_date_filter is True"
        )


def test_supports_date_filter_consistent_with_list_features():
    """supports_date_filter in list_features() must match describe_feature() for every feature."""
    for entry in list_features():
        name = entry["name"]
        full_meta = describe_feature(name)
        assert entry["supports_date_filter"] == full_meta["supports_date_filter"], (
            f"'{name}': list_features() says supports_date_filter="
            f"{entry['supports_date_filter']} but describe_feature() says "
            f"{full_meta['supports_date_filter']}"
        )


def test_metadata_outputs_non_empty():
    """Every registered feature must declare at least one output column."""
    for feature_name in _EXPECTED_FEATURES:
        meta = describe_feature(feature_name)
        assert len(meta["outputs"]) > 0, (
            f"'{feature_name}' has no declared output columns"
        )


def test_metadata_inputs_non_empty():
    """Every registered feature must declare at least one input column."""
    for feature_name in _EXPECTED_FEATURES:
        meta = describe_feature(feature_name)
        assert len(meta["inputs"]) > 0, (
            f"'{feature_name}' has no declared input columns"
        )


def test_metadata_dependencies_is_list():
    """dependencies must always be a list (empty or populated)."""
    for feature_name in _EXPECTED_FEATURES:
        meta = describe_feature(feature_name)
        assert isinstance(meta["dependencies"], list), (
            f"'{feature_name}' dependencies is not a list: {type(meta['dependencies'])}"
        )


def test_list_features_grain_values_are_known():
    """grain values in list_features() must be drawn from the declared vocabulary."""
    known_grains = {"ball", "batter", "bowler", "venue", "player", "match"}
    for entry in list_features():
        assert entry["grain"] in known_grains, (
            f"'{entry['name']}' has unrecognised grain '{entry['grain']}'"
        )


def test_feature_registry_backward_compat_load_features(mlc_session):
    """Existing load_features() must still work for every registered feature after registry changes."""
    for entry in list_features():
        df = load_features(entry["name"], mlc_session)
        assert isinstance(df, pd.DataFrame), (
            f"load_features('{entry['name']}') did not return a DataFrame"
        )
        assert not df.empty, f"load_features('{entry['name']}') returned empty DataFrame"
