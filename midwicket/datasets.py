"""
Midwicket Dataset Management: Automatic Downloader and Ingestion Registry

Provides single-line loading for major cricket competitions from Cricsheet,
handling download, extraction, validation, and session orchestration.
"""

import logging
import os
import tempfile
import zipfile
import shutil
import requests
from pathlib import Path
from typing import Dict, Any, List, Optional
from tqdm import tqdm
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Internal Imports
from midwicket.api.session import MidwicketSession
from midwicket.config import DEFAULT_DATA_DIR

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# DATASET REGISTRY
#
# Each entry documents:
#   url          – Cricsheet download endpoint
#   description  – Human-readable name
#   format       – Match format(s) covered
#   est_matches  – Approximate match count (updated periodically)
#   est_deliveries – Approximate ball-event row count
#   est_players  – Approximate unique player registry size
#   date_range   – Earliest–latest season covered
#   version      – Registry schema version (bumped when metadata changes)
#   gender       – "men", "women", or "both"
#   est_size_mb  – Compressed download size
# ──────────────────────────────────────────────────────────────────────────────
DATASETS: Dict[str, Dict[str, Any]] = {
    # ── Franchise T20 leagues ─────────────────────────────────────────────────
    "ipl": {
        "url": "https://cricsheet.org/downloads/ipl_json.zip",
        "description": "Indian Premier League — the highest-profile T20 franchise competition",
        "format": "T20",
        "gender": "men",
        "est_matches": 1241,
        "est_deliveries": 295_258,
        "est_players": 811,
        "est_venues": 60,
        "date_range": "2008–2026",
        "coverage": "complete",
        "competitions": ["Indian Premier League"],
        "version": "1.1",
        "est_size_mb": 3.93,
    },
    "bbl": {
        "url": "https://cricsheet.org/downloads/bbl_json.zip",
        "description": "Big Bash League — Australia's premier domestic T20 competition",
        "format": "T20",
        "gender": "men",
        "est_matches": 662,
        "est_deliveries": 153_250,
        "est_players": 532,
        "est_venues": 32,
        "date_range": "2011/12–2025/26",
        "coverage": "complete",
        "competitions": ["Big Bash League"],
        "version": "1.1",
        "est_size_mb": 2.05,
    },
    "wbbl": {
        # Cricsheet publishes WBBL as wbb_json.zip — wbbl_json.zip returns HTTP 404.
        "url": "https://cricsheet.org/downloads/wbb_json.zip",
        "description": "Women's Big Bash League — Australia's elite women's T20 franchise",
        "format": "T20",
        "gender": "women",
        "est_matches": 519,
        "est_deliveries": 118_656,
        "est_players": 282,
        "est_venues": 62,
        "date_range": "2015/16–2025/26",
        "coverage": "complete",
        "competitions": ["Women's Big Bash League"],
        "version": "1.1",
        "est_size_mb": 1.55,
    },
    "psl": {
        "url": "https://cricsheet.org/downloads/psl_json.zip",
        "description": "Pakistan Super League — Pakistan's national franchise T20 league",
        "format": "T20",
        "gender": "men",
        "est_matches": 357,
        "est_deliveries": 83_799,
        "est_players": 460,
        "est_venues": 10,
        "date_range": "2015/16–2026",
        "coverage": "complete",
        "competitions": ["Pakistan Super League"],
        "version": "1.1",
        "est_size_mb": 1.13,
    },
    "cpl": {
        "url": "https://cricsheet.org/downloads/cpl_json.zip",
        "description": "Caribbean Premier League — West Indies franchise T20 competition",
        "format": "T20",
        "gender": "men",
        "est_matches": 407,
        "est_deliveries": 95_024,
        "est_players": 419,
        "est_venues": 17,
        "date_range": "2013–2025",
        "coverage": "complete",
        "competitions": ["Caribbean Premier League"],
        "version": "1.1",
        "est_size_mb": 1.26,
    },
    "sa20": {
        # Cricsheet publishes SA20 as sat_json.zip — sa20_json.zip returns HTTP 404.
        "url": "https://cricsheet.org/downloads/sat_json.zip",
        "description": "SA20 — South Africa's franchise T20 competition (launched 2023)",
        "format": "T20",
        "gender": "men",
        "est_matches": 130,
        "est_deliveries": 29_020,
        "est_players": 213,
        "est_venues": 6,
        "date_range": "2022/23–2025/26",
        "coverage": "complete",
        "competitions": ["SA20"],
        "version": "1.1",
        "est_size_mb": 0.40,
    },
    "mlc": {
        "url": "https://cricsheet.org/downloads/mlc_json.zip",
        "description": "Major League Cricket — USA's inaugural professional T20 league",
        "format": "T20",
        "gender": "men",
        "est_matches": 75,
        "est_deliveries": 17_413,
        "est_players": 167,
        "est_venues": 5,
        "date_range": "2023–2025",
        "coverage": "complete",
        "competitions": ["Major League Cricket"],
        "version": "1.1",
        "est_size_mb": 0.23,
    },
    "wpl": {
        "url": "https://cricsheet.org/downloads/wpl_json.zip",
        "description": "Women's Premier League — India's top-tier women's franchise T20 league",
        "format": "T20",
        "gender": "women",
        "est_matches": 88,
        "est_deliveries": 20_656,
        "est_players": 136,
        "est_venues": 7,
        "date_range": "2022/23–2025/26",
        "coverage": "complete",
        "competitions": ["Women's Premier League"],
        "version": "1.1",
        "est_size_mb": 0.27,
    },
    "hundred": {
        # Cricsheet publishes The Hundred as hnd_json.zip — hundred_json.zip returns HTTP 404.
        "url": "https://cricsheet.org/downloads/hnd_json.zip",
        "description": "The Hundred — England & Wales 100-ball competition (men's and women's)",
        "format": "The Hundred",
        "gender": "both",
        "est_matches": 322,
        "est_deliveries": 62_397,
        "est_players": 439,
        "est_venues": 8,
        "date_range": "2021–2025",
        "coverage": "complete",
        "competitions": ["The Hundred (Men)", "The Hundred (Women)"],
        "version": "1.1",
        "est_size_mb": 0.93,
    },
    # ── International formats ─────────────────────────────────────────────────
    "t20is": {
        "url": "https://cricsheet.org/downloads/t20s_json.zip",
        "description": "T20 Internationals — all men's and women's T20I matches",
        "format": "T20I",
        "gender": "both",
        "est_matches": 5318,
        "est_deliveries": 1_240_157,
        "est_players": 7098,
        "est_venues": 447,
        "date_range": "2004/05–2026",
        # partial: Cricsheet withholds 366 Afghanistan-related matches globally.
        "coverage": "partial",
        "competitions": ["Men's T20 Internationals", "Women's T20 Internationals"],
        "version": "1.1",
        "est_size_mb": 16.03,
    },
    "odis": {
        "url": "https://cricsheet.org/downloads/odis_json.zip",
        "description": "One Day Internationals — all men's and women's ODI matches",
        "format": "ODI",
        "gender": "both",
        "est_matches": 3134,
        "est_deliveries": 1_688_755,
        "est_players": 2658,
        "est_venues": 379,
        "date_range": "2002–2026",
        # partial: Cricsheet withholds 366 Afghanistan-related matches globally.
        "coverage": "partial",
        "competitions": ["Men's One Day Internationals", "Women's One Day Internationals"],
        "version": "1.1",
        "est_size_mb": 14.58,
    },
    "tests": {
        "url": "https://cricsheet.org/downloads/tests_json.zip",
        "description": "Test matches — the pinnacle of international cricket (men's and women's)",
        "format": "Test",
        "gender": "both",
        "est_matches": 903,
        "est_deliveries": 1_755_413,
        "est_players": 1205,
        "est_venues": 184,
        "date_range": "2001/02–2026",
        # partial: Cricsheet withholds 366 Afghanistan-related matches globally.
        "coverage": "partial",
        "competitions": ["Men's Test Matches", "Women's Test Matches"],
        "version": "1.1",
        "est_size_mb": 10.22,
    },
    # ── Aggregates ────────────────────────────────────────────────────────────
    # NOTE: "all_t20" has been deliberately omitted. Cricsheet publishes no
    # combined T20 bundle under any URL variant. all_t20_json.zip and all
    # tested alternatives return HTTP 404. Do not re-add without a verified URL.
    "all": {
        "url": "https://cricsheet.org/downloads/all_json.zip",
        "description": "Complete Cricsheet archive — every format, every gender, every competition",
        "format": "All",
        "gender": "both",
        "est_matches": 21877,
        # est_deliveries/players/venues below are carry-forward estimates:
        # the full archive was not re-parsed during the 2026-05-31 audit (96 MB).
        "est_deliveries": 9_148_000,
        "est_players": 12000,
        "est_venues": 450,
        "date_range": "2001–2026",
        # partial: 366 Afghanistan-related matches are explicitly withheld by Cricsheet.
        "coverage": "partial",
        "competitions": ["All formats and competitions (complete Cricsheet archive)"],
        "version": "1.1",
        "est_size_mb": 96.36,
    },
}

# ── Alias map: accept common synonyms ─────────────────────────────────────────
_ALIASES: Dict[str, str] = {
    "t20s": "t20is",          # legacy key
    "t20i": "t20is",
    "t20_international": "t20is",
    "odi": "odis",
    "test": "tests",
    "all_test": "tests",
    "all_odi": "odis",
    "women_t20": "wbbl",      # backward-compat for load_dataset("women_t20")
}


def list_datasets() -> List[Dict[str, Any]]:
    """Return a list of all registered datasets with full metadata.

    Each record contains:
        name           – key to pass to ``load_dataset()`` or ``describe_dataset()``
        description    – human-readable competition name
        format         – match format (T20, ODI, Test, T20I, The Hundred, All)
        gender         – player gender scope
        matches        – approximate match count
        deliveries     – approximate ball-event row count
        players        – approximate unique player registry size
        venues         – approximate unique venue count
        coverage       – Cricsheet completeness ("complete" or "partial")
        date_range     – earliest–latest season covered
        version        – registry schema version

    Example::

        from midwicket.datasets import list_datasets

        for ds in list_datasets():
            print(ds["name"], ds["matches"], ds["date_range"])
    """
    records: List[Dict[str, Any]] = []
    for name, meta in DATASETS.items():
        records.append(
            {
                "name": name,
                "description": meta["description"],
                "format": meta["format"],
                "gender": meta.get("gender", "unknown"),
                "matches": meta["est_matches"],
                "deliveries": meta["est_deliveries"],
                "players": meta["est_players"],
                "venues": meta.get("est_venues"),
                "coverage": meta.get("coverage", "complete"),
                "date_range": meta["date_range"],
                "version": meta["version"],
            }
        )
    return records


def describe_dataset(name: str) -> Dict[str, Any]:
    """Return complete metadata for a single registered dataset.

    Args:
        name: Dataset key (e.g. ``'ipl'``, ``'bbl'``) or any accepted alias.

    Returns:
        Dict containing:
            name          – canonical dataset key
            description   – human-readable competition name
            source        – Cricsheet download URL
            schema_version – registry schema version string
            format        – match format(s) covered
            gender        – player gender scope
            competitions  – list of specific competitions included
            date_range    – earliest–latest season covered
            coverage      – Cricsheet completeness ("complete" or "partial")
            statistics    – dict with matches, deliveries, players, venues, size_mb
            aliases       – list of accepted alternative keys for this dataset
            example_usage – ready-to-run Python code snippet

    Raises:
        ValueError: If ``name`` is not a registered dataset key or alias.

    Example::

        from midwicket.datasets import describe_dataset

        info = describe_dataset("ipl")
        print(info["description"])
        print(info["statistics"])
        print(info["example_usage"])
    """
    canonical_name = name.lower().strip()
    if canonical_name in _ALIASES:
        canonical_name = _ALIASES[canonical_name]

    if canonical_name not in DATASETS:
        raise ValueError(
            f"Dataset '{name}' is not registered. "
            f"Available keys: {sorted(DATASETS.keys())}. "
            f"Also accepts aliases: {sorted(_ALIASES.keys())}."
        )

    meta = DATASETS[canonical_name]

    # Collect all aliases that resolve to this canonical name
    aliases = [alias for alias, target in _ALIASES.items() if target == canonical_name]

    return {
        "name": canonical_name,
        "description": meta["description"],
        "source": meta["url"],
        "schema_version": meta["version"],
        "format": meta["format"],
        "gender": meta.get("gender", "unknown"),
        "competitions": meta.get("competitions", [canonical_name.upper()]),
        "date_range": meta["date_range"],
        "coverage": meta.get("coverage", "complete"),
        "statistics": {
            "matches": meta["est_matches"],
            "deliveries": meta["est_deliveries"],
            "players": meta["est_players"],
            "venues": meta.get("est_venues"),
            "size_mb": meta.get("est_size_mb"),
        },
        "aliases": sorted(aliases),
        "example_usage": (
            f"from midwicket.datasets import load_dataset\n\n"
            f'session = load_dataset("{canonical_name}")'
        ),
    }


@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=8.0),
    retry=retry_if_exception_type(requests.RequestException),
)
def _download_file(url: str, dest_path: Path) -> None:
    """Download a file with retry and streaming progress visualization."""
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    
    logger.info("Connecting to Cricsheet: %s", url)
    response = requests.get(url, stream=True, timeout=60)
    response.raise_for_status()
    
    total_size = int(response.headers.get('content-length', 0))
    tmp_fd, tmp_name = tempfile.mkstemp(dir=dest_path.parent, suffix=".tmp")
    tmp_path = Path(tmp_name)
    
    try:
        with os.fdopen(tmp_fd, 'wb') as f, tqdm(
            desc=f"Downloading {dest_path.name}",
            total=total_size,
            unit='iB',
            unit_scale=True,
            unit_divisor=1024,
        ) as bar:
            for chunk in response.iter_content(chunk_size=8192):
                size = f.write(chunk)
                bar.update(size)
        tmp_path.replace(dest_path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise

def load_dataset(name: str, version: str = "1.0", cache_dir: Optional[str] = None, force: bool = False) -> MidwicketSession:
    """
    Downloads, extracts, and boots a localized MidwicketSession for the requested competition.

    Args:
        name: Name of competition dataset (e.g. 'ipl', 'bbl', 'all_t20').
              See ``list_datasets()`` for all available keys. Aliases such as
              ``'t20s'``, ``'odi'``, and ``'women_t20'`` are also accepted.
        version: Dataset version string (default '1.0')
        cache_dir: Optional custom storage path. Defaults to ~/.midwicket_data/datasets
        force: If True, forces redownload and clean database rebuild.

    Returns:
        MidwicketSession: Initialized session loaded with dataset match tables.
    """
    canonical_name = name.lower().strip()
    if canonical_name in _ALIASES:
        canonical_name = _ALIASES[canonical_name]
        
    if canonical_name not in DATASETS:
        raise ValueError(
            f"Dataset '{name}' is not registered. "
            f"Available keys: {sorted(DATASETS.keys())}. "
            f"Also accepts aliases: {sorted(_ALIASES.keys())}."
        )
        
    info = DATASETS[canonical_name]
    
    # Establish versioned directory structure
    root_dir = Path(cache_dir) if cache_dir else DEFAULT_DATA_DIR / "datasets"
    dataset_dir = root_dir / f"{canonical_name}_v{version}"
    raw_dir = dataset_dir / "raw" / "ipl" # Maintain standard folder naming for internalDataLoader
    zip_path = dataset_dir / "ipl_json.zip"
    
    if force and dataset_dir.exists():
        logger.info("Force rebuild requested: removing directory %s", dataset_dir)
        shutil.rmtree(dataset_dir, ignore_errors=True)
        
    dataset_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)
    
    # Download and extract if not present
    if not zip_path.exists() and not any(raw_dir.glob("*.json")):
        try:
            _download_file(info["url"], zip_path)
            
            logger.info("Extracting matches for '%s'...", canonical_name)
            with zipfile.ZipFile(zip_path, "r") as z:
                # Guard against zip slip
                resolved_raw = raw_dir.resolve()
                for member in z.namelist():
                    target = (resolved_raw / member).resolve()
                    try:
                        target.relative_to(resolved_raw)
                    except ValueError:
                        logger.warning("Skipping unsafe zip entry: %s", member)
                        continue
                    z.extract(member, raw_dir)
            
            # Clean up intermediate zip archive
            zip_path.unlink(missing_ok=True)
            logger.info("Extraction complete. Extracted to: %s", raw_dir)
        except Exception as e:
            shutil.rmtree(dataset_dir, ignore_errors=True)
            raise ConnectionError(f"Failed to fetch dataset '{canonical_name}': {e}")
            
    # Initialize versioned Midwicket Session
    logger.info("Initializing Midwicket Session for dataset '%s'...", canonical_name)
    session = MidwicketSession(data_dir=str(dataset_dir))
    
    # Pre-load all extracted match JSONs into the DuckDB analytical tables
    json_files = list(raw_dir.glob("*.json"))
    if json_files:
        # Determine if database already has loaded rows
        row_count = 0
        try:
            res = session.engine.execute_sql("SELECT COUNT(*) FROM ball_events")
            row_count = res.to_pydict()["count()"][0]
        except Exception:
            pass
            
        if row_count == 0:
            logger.info("Ingesting %d matches into database...", len(json_files))
            for f in tqdm(json_files, desc="Ingesting match data"):
                match_id = f.stem
                session.load_match(match_id)
                
    # Update global singleton instance
    with MidwicketSession._instance_lock:
        MidwicketSession._instance = session
        
    return session
