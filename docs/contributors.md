# Contributor Issue Guide

60 open tasks across three difficulty tiers. Pick one, open the
corresponding GitHub issue, and link it in your PR.

Read `CONTRIBUTING.md` before starting. Every issue listed here
has a clear scope — do not expand it without discussion.

---

## Good First Issues (25 issues)
*Self-contained. Under 2 hours. No prior codebase knowledge required.*

---

### GFI-01: Add `list_datasets()` type annotation to `__all__`

**File:** `midwicket/datasets.py`
**Task:** `list_datasets` is not in `__all__`. Add it, and add a corresponding
import to `midwicket/__init__.py`.
**Test to write:** `assert "list_datasets" in dir(midwicket)`

---

### GFI-02: Document `load_dataset()` aliases in docstring

**File:** `midwicket/datasets.py`
**Task:** The `load_dataset()` docstring says "e.g. 'ipl', 'bbl'" but does not
list all accepted aliases. Add the complete alias table from `_ALIASES` to the
docstring.
**Test to write:** None — documentation only.

---

### GFI-03: Add missing docstring to `_download_file`

**File:** `midwicket/datasets.py`, line 260 approx.
**Task:** `_download_file` has no docstring. Add one explaining parameters
and exceptions raised.
**Test to write:** None — documentation only.

---

### GFI-04: Fix typo in `getting_started.md`

**File:** `docs/getting_started.md`
**Task:** Audit the file for typos, broken code block formatting, and
outdated function names. Fix any found.
**Test to write:** None — documentation only.

---

### GFI-05: Add `est_size_mb` to `list_datasets()` output

**File:** `midwicket/datasets.py`
**Task:** `list_datasets()` currently omits `est_size_mb`. Add it to the
returned dict for each dataset. Update `docs/datasets.md` to reflect.
**Test to write:** Assert `"est_size_mb"` present in each record from `list_datasets()`.

---

### GFI-06: Write test for `list_datasets()` return shape

**File:** `tests/test_data_loader.py` or new `tests/test_datasets.py`
**Task:** Write three tests: (a) `list_datasets()` returns at least 10 items,
(b) every item has required keys, (c) `"ipl"` is present in the returned names.
**Prerequisite:** None — test only.

---

### GFI-07: Write test for `load_dataset()` invalid key

**File:** `tests/test_datasets.py`
**Task:** Assert that `load_dataset("nonexistent_league")` raises `ValueError`
with a message that includes the word "registered".
**Prerequisite:** None — test only.

---

### GFI-08: Write test for `load_dataset()` alias acceptance

**File:** `tests/test_datasets.py`
**Task:** Mock the download step. Assert that `load_dataset("t20s")` and
`load_dataset("t20is")` resolve to the same canonical dataset key without error.

---

### GFI-09: Fix `examples/showcase_01_kohli_bumrah.py` exception handling

**File:** `examples/showcase_01_kohli_bumrah.py`
**Task:** The except clause catches `Exception` broadly. Narrow it to
`ValueError` and `RuntimeError`. Add a comment explaining why.

---

### GFI-10: Add `format` filter to `list_datasets()`

**File:** `midwicket/datasets.py`
**Task:** Add an optional `format: Optional[str] = None` parameter to
`list_datasets()`. If provided, return only datasets matching that format
(e.g., `list_datasets(format="T20")`).
**Test to write:** Assert `list_datasets(format="ODI")` returns only ODI entries.

---

### GFI-11: Add `gender` filter to `list_datasets()`

**File:** `midwicket/datasets.py`
**Task:** Add an optional `gender: Optional[str] = None` parameter.
If provided, return only `"men"`, `"women"`, or `"both"` datasets.
**Test to write:** Assert `list_datasets(gender="women")` includes `"wbbl"`.

---

### GFI-12: Fix `docs/api.md` redirect message

**File:** `docs/api.md`
**Task:** The current file says the API reference "now lives at"
`midwicket/docs/api.md` which does not exist. Either create the reference or
point to the correct location. Do not delete the redirect.

---

### GFI-13: Add `__repr__` to `HeadToHeadSummary`

**File:** `midwicket/api/head_to_head.py` (or wherever `HeadToHeadSummary` is defined)
**Task:** `HeadToHeadSummary` currently has no `__repr__`. Add one that prints
all fields in a readable format.
**Test to write:** `assert "batter" in repr(summary)`.

---

### GFI-14: Add Colab badge to `docs/getting_started.md`

**File:** `docs/getting_started.md`
**Task:** The README has a Colab badge but `getting_started.md` does not.
Add the badge at the top of the file, pointing to `notebooks/quickstart.ipynb`.

---

### GFI-15: Document `MatchConfig` fields

**File:** `midwicket/core/match_config.py`
**Task:** Every field in `MatchConfig` needs a one-line docstring comment.
Audit the class and add missing field descriptions.

---

### GFI-16: Write test for `express.predict_win` returns valid probability

**File:** `tests/`
**Task:** Call `express.predict_win(venue="Wankhede Stadium", target=180,
current_score=95, wickets_down=3, overs_done=10.0)` and assert the result
contains `"win_prob"` in [0.0, 1.0].
**Prerequisite:** No data download required — uses in-memory model.

---

### GFI-17: Add `DATASETS` count to `README.md`

**File:** `README.md`
**Task:** Update the README to mention the 14 registered datasets by name
(or link to `docs/datasets.md`). The current README mentions datasets but
does not link to the catalog.

---

### GFI-18: Write docstring for `career_batting()`

**File:** `midwicket/api/player_analytics.py`
**Task:** `career_batting` has either no docstring or an incomplete one.
Add a full NumPy-style docstring: parameters, returns, raises, and an example.

---

### GFI-19: Remove `ci_error.log` from repository

**File:** `ci_error.log` (project root)
**Task:** This file should not be tracked. Add it to `.gitignore` and remove it
from git history using `git rm --cached ci_error.log`.
**Note:** Coordinate with maintainer before git history rewrite.

---

### GFI-20: Write a one-paragraph description for each research study in `research/README.md`

**File:** `research/README.md`
**Task:** The table of studies has one-line descriptions. Add a two-sentence
description for each of the 25 studies explaining what the finding is and
why it matters, below the table.

---

### GFI-21: Add `research/charts/` directory with placeholder

**File:** `research/charts/.gitkeep`
**Task:** Study scripts reference `research/charts/` but the directory does not
exist. Create it with a `.gitkeep` file and add a comment in `research/README.md`
explaining that charts are written there by the study scripts.

---

### GFI-22: Verify `express.get_player_stats()` matches `career_batting()` output

**File:** `tests/`
**Task:** Call both functions on the same player with the same session and
assert that the `runs` and `average` fields match within a tolerance of 0.01.
**Note:** May reveal a bug (see `PRODUCTION_READINESS_GAPS.md`).

---

### GFI-23: Add Python version badge to README

**File:** `README.md`
**Task:** The Python badge currently reads `pyversions/midwicket`. Verify it
resolves correctly and add a minimum Python version note to `getting_started.md`.

---

### GFI-24: Add `wpl` and `hundred` to the `examples/README.md`

**File:** `examples/README.md`
**Task:** The examples README does not mention the two newest datasets (WPL
and The Hundred). Add a section showing how to load each.

---

### GFI-25: Fix `load_dataset` hardcoded `"raw/ipl"` path for non-IPL datasets

**File:** `midwicket/datasets.py` (the `load_dataset` function)
**Task:** The `raw_dir` is hardcoded to `"raw/ipl"` regardless of the dataset
being loaded. Change it to `f"raw/{canonical_name}"` so that non-IPL datasets
store their raw files in the correct subdirectory.
**Test to write:** Assert that loading `"bbl"` creates `raw/bbl/`, not `raw/ipl/`.

---

## Intermediate Issues (25 issues)
*Requires reading 2–4 source files. 2–8 hours.*

---

### INT-01: Fix hardcoded `zip_path` in `load_dataset`

**File:** `midwicket/datasets.py`
**Task:** `zip_path` is hardcoded to `"ipl_json.zip"` regardless of which
dataset is being loaded. Fix it to use the actual dataset filename derived
from the URL. Write a parametrised test that verifies the path for at least
three different datasets.

---

### INT-02: Make `load_dataset` idempotent for concurrent callers

**File:** `midwicket/datasets.py`
**Task:** If two threads call `load_dataset("ipl")` simultaneously, both will
attempt to download. Add a file-level lock (using `threading.Lock` or `filelock`)
to ensure only one download proceeds. Write a threading test.

---

### INT-03: Add `hash` field to `DATASETS` for integrity verification

**File:** `midwicket/datasets.py`
**Task:** Add an optional `sha256` field to each `DATASETS` entry. When a
zip file is downloaded, verify its SHA-256 hash if the field is present.
Raise `ValueError` if verification fails.

---

### INT-04: Implement `list_datasets()` as a DataFrame option

**File:** `midwicket/datasets.py`
**Task:** Add a parameter `as_dataframe: bool = False` to `list_datasets()`.
When `True`, return a `pandas.DataFrame` instead of a list of dicts. Type-check
properly so mypy accepts the overloaded return type.

---

### INT-05: Add `last_updated` timestamp to dataset metadata

**File:** `midwicket/datasets.py`
**Task:** When a dataset is successfully downloaded, write a `metadata.json`
file in the dataset directory with `{"downloaded_at": "<ISO timestamp>",
"dataset_name": "<name>"}`. Expose `last_updated` in the session object.

---

### INT-06: Write integration test for `load_dataset("mlc")` end-to-end

**File:** `tests/test_data_loader.py`
**Task:** MLC is the smallest dataset (~0.2 MB). Write a test that downloads
it (skip if no network), loads it, and asserts that `ball_events` has at
least 1,000 rows. Mark with `pytest.mark.slow` and `pytest.mark.network`.

---

### INT-07: Refactor `features.py` pressure index query into a named function

**File:** `midwicket/features.py`
**Task:** The pressure index SQL is inline in a decorated function. Extract
the SQL into a named constant or builder function so it can be unit-tested
without executing against a live session.

---

### INT-08: Add `bowling_by_phase` test covering all three phases

**File:** `tests/test_player_analytics.py`
**Task:** `bowling_by_phase` is tested but coverage is incomplete. Write tests
that assert the function returns rows for `powerplay`, `middle_overs`, and
`death` when sufficient data exists, and returns an empty frame gracefully
when insufficient data exists.

---

### INT-09: Implement `list_datasets()` pretty-print helper

**File:** `midwicket/datasets.py`
**Task:** Add a module-level `print_datasets()` function that prints a
formatted table of all datasets to stdout. The table must include all fields
returned by `list_datasets()`. No external dependencies (use `str.format`).

---

### INT-10: Add `venue_adjusted_form` feature documentation

**File:** `docs/api.md` or new `docs/features.md`
**Task:** `venue_adjusted_form` is one of the most-used features but has no
documentation. Write a complete description: formula, field definitions,
example output, and known limitations.

---

### INT-11: Fix `career_fielding` to handle players with zero fielding events

**File:** `midwicket/api/player_analytics.py`
**Task:** `career_fielding("V Kohli", session)` raises an exception when
the player has no fielding events in the dataset. Fix it to return an empty
`DataFrame` with the correct schema instead. Write a test.

---

### INT-12: Add `season` parameter to `batting_leaderboard`

**File:** `midwicket/api/player_analytics.py`
**Task:** `batting_leaderboard` has no season filter. Add an optional
`season: Optional[int] = None` parameter. When provided, restrict to matches
from that year. Write a test.

---

### INT-13: Add `min_wickets` parameter to `bowling_leaderboard`

**File:** `midwicket/api/player_analytics.py`
**Task:** `bowling_leaderboard` has a `min_innings` parameter but no
`min_wickets` filter. Add it. Default to 0 for backward compatibility.

---

### INT-14: Add validation error for negative `overs_done` in `predict_win`

**File:** `midwicket/express.py` or `midwicket/compute/winprob.py`
**Task:** `predict_win(overs_done=-1.0, ...)` does not raise an error — it
silently returns a nonsensical probability. Add input validation that raises
`ValueError` for overs < 0, overs > 20, wickets < 0, or wickets > 10.

---

### INT-15: Write benchmark baseline script for win probability

**File:** `research/benchmark_01_win_probability_baseline.py`
**Task:** Implement the naive baseline described in `docs/benchmarks.md`
(logistic regression on IPL 2008–2021, evaluate on 2023–2026). Report AUC
and Brier score. This becomes the reference implementation.

---

### INT-16: Add `session` parameter to `scouting_report`

**File:** `midwicket/report/scout.py`
**Task:** `scouting_report` uses the global singleton session. Add an explicit
`session: Optional[MidwicketSession] = None` parameter. If `None`, fall back
to the singleton. Update tests.

---

### INT-17: Add `format` column to ball_events schema documentation

**File:** `docs/` (find or create schema reference)
**Task:** The `ball_events` table schema is not documented anywhere in `docs/`.
Create `docs/schema.md` documenting every column with type and description.

---

### INT-18: Publish `PRODUCTION_READINESS_GAPS.md` findings as GitHub issues

**Task:** Read `PRODUCTION_READINESS_GAPS.md`. Open a GitHub issue for each
documented gap, labelled `bug` and `production`. Include the exact symptom,
a minimal reproducer, and the file/line implicated.
**Note:** This is a coordination task, not a code task.

---

### INT-19: Add `bowling_kind` population check to data loader

**File:** `midwicket/sources/cricsheet_loader.py`
**Task:** Several research studies degrade when `bowling_kind` is not
populated. Add a post-ingestion check: if fewer than 30% of ball_events
have `bowling_kind` populated, emit a `logger.warning`.

---

### INT-20: Add `conftest.py` fixture for in-memory session

**File:** `tests/conftest.py`
**Task:** Multiple test files create their own sessions independently.
Create a shared `@pytest.fixture(scope="session")` that provides a
`:memory:` `MidwicketSession` with synthetic data for testing. Update
at least 3 test files to use it.

---

### INT-21: Add `CHANGELOG.md` entry format enforcement

**File:** `CHANGELOG.md`, `.pre-commit-config.yaml`
**Task:** Add a pre-commit hook or CI step that enforces
`[Unreleased]` section presence in `CHANGELOG.md`. Without it, releases
frequently omit changelog updates.

---

### INT-22: Add `examples/` README.md that links to `docs/examples/index.md`

**File:** `examples/README.md`
**Task:** `examples/README.md` exists but is sparse. Add a clear header
and link to `docs/examples/index.md` as the canonical guide.

---

### INT-23: Add research study 26: "Best IPL Franchises for Player Development"

**File:** `research/26_franchise_player_development.py`
**Task:** Define "player development" as: a player debuting at a franchise with
fewer than 5 IPL appearances, then producing 500+ runs or 30+ wickets for that
franchise within 3 seasons. Count by franchise. Follow `_template.py` structure.

---

### INT-24: Add test for `batting_by_season` ordering

**File:** `tests/test_player_analytics.py`
**Task:** Assert that `batting_by_season("V Kohli")` returns results in
ascending `season` order. This is implicitly expected but not tested.

---

### INT-25: Fix `__version__` not being re-exported from `midwicket.__init__`

**File:** `midwicket/__init__.py`
**Task:** `__version__` is defined in `__init__.py` but not re-exported
consistently. Verify that `import midwicket; midwicket.__version__` returns
`"1.1.0"` and that `from midwicket import __version__` also works. Add a test.

---

## Advanced Issues (10 issues)
*Requires full codebase context. 1–3 days. Open an issue first.*

---

### ADV-01: Fix `load_dataset` to use correct `raw_dir` naming for all datasets

**Files:** `midwicket/datasets.py`, `midwicket/api/session.py`, `midwicket/sources/`
**Task:** The data loader hardcodes `raw/ipl` as the raw directory regardless of
dataset. This means all non-IPL datasets are stored under `raw/ipl/`, conflicting
if multiple datasets are loaded. The fix requires tracing the directory path
through `MidwicketSession` initialisation and ensuring each dataset has its own
named raw directory. Write an integration test that loads two datasets
simultaneously and verifies they do not overwrite each other's files.

---

### ADV-02: Implement reproducible train/test split helper for benchmarks

**Files:** new `midwicket/benchmark.py`
**Task:** Implement a `get_benchmark_split(benchmark_id: str, session)` function
that returns `(train_df, val_df, test_df)` exactly as specified in
`docs/benchmarks.md`. The splits must be deterministic (no randomness) and
must not leak future data into the training set. Document the function in
`docs/benchmarks.md`.

---

### ADV-03: Implement schema migration system

**Files:** `midwicket/schema/`, `midwicket/storage/`
**Task:** When the `ball_events` schema changes, existing local DuckDB files
become incompatible. Implement a migration system: detect the schema version
of an existing database, apply migration scripts in order. Write migration
scripts as numbered `.sql` files.

---

### ADV-04: Add `DuckDB` query explain plan to debug output

**Files:** `midwicket/api/session.py`, `midwicket/runtime/modes.py`
**Task:** When `debug_mode=True`, log the DuckDB `EXPLAIN` plan for every
`execute_sql` call. Gate behind the debug flag to avoid performance impact
in production use.

---

### ADV-05: Implement `load_dataset` progress reporting via callback

**Files:** `midwicket/datasets.py`
**Task:** Add a `progress_callback: Optional[Callable[[str, int, int], None]]`
parameter to `load_dataset`. The callback receives `(stage, current, total)`
where stage is one of `"downloading"`, `"extracting"`, `"ingesting"`. The
existing `tqdm` usage should be refactored to call this callback. This enables
GUI and Jupyter integrations.

---

### ADV-06: Audit and fix all SQL injection vectors in raw SQL passthrough

**Files:** `midwicket/api/session.py`, wherever `execute_sql` is exposed
**Task:** `execute_sql` accepts arbitrary SQL strings. In read-only mode,
write operations (`INSERT`, `UPDATE`, `DROP`) should be blocked. Implement
a SQL guard layer (similar to what `tests/test_sql_guard.py` presumably tests)
that rejects DDL and DML statements. Document the restriction in the API reference.

---

### ADV-07: Add `MANIFEST.in` coverage for new docs directories

**Files:** `MANIFEST.in`, `pyproject.toml`
**Task:** `docs/examples/`, `research/`, and the new docs pages are not
included in `MANIFEST.in`. This means `pip install midwicket` does not include
them. Add the necessary `include` directives and verify with
`python -m build --sdist && tar tzf dist/*.tar.gz | grep docs`.

---

### ADV-08: Implement `cancel_download` signal handling in `load_dataset`

**Files:** `midwicket/datasets.py`
**Task:** Long downloads (e.g., `all` at 85 MB) cannot be interrupted cleanly.
Implement `SIGINT` handling in `_download_file` that deletes the partial
download and raises `KeyboardInterrupt` cleanly. Write a test using `signal`.

---

### ADV-09: Add multi-dataset session (session merge)

**Files:** `midwicket/api/session.py`, `midwicket/datasets.py`
**Task:** Currently each `load_dataset` call returns an independent session.
There is no way to query across two datasets simultaneously (e.g., IPL + BBL
combined). Design and implement `merge_sessions(s1, s2, ...)` that returns a
single session backed by a unified DuckDB view. Requires careful handling of
duplicate match IDs across datasets.

---

### ADV-10: Implement `benchmark_results.md` with CI enforcement

**Files:** `docs/benchmark_results.md` (create), `.github/workflows/`
**Task:** Create `docs/benchmark_results.md` with the table format specified
in `docs/benchmarks.md`. Add a CI check that validates any new row against
the benchmark schema: required columns, metric range, and a link to
a reproduction script. This transforms benchmarks from aspirational to enforced.
