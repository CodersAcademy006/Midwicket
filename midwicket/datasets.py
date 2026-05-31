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

# Standard Cricsheet dataset repositories
DATASETS = {
    "ipl": {
        "url": "https://cricsheet.org/downloads/ipl_json.zip",
        "description": "Indian Premier League (IPL) matches (2008 - Present)",
        "format": "T20",
        "est_matches": 1100,
        "est_size_mb": 4.5
    },
    "t20s": {
        "url": "https://cricsheet.org/downloads/t20s_json.zip",
        "description": "T20 International matches (Men's & Women's)",
        "format": "T20I",
        "est_matches": 3200,
        "est_size_mb": 14.5
    },
    "bbl": {
        "url": "https://cricsheet.org/downloads/bbl_json.zip",
        "description": "Big Bash League (BBL) matches (Australia)",
        "format": "T20",
        "est_matches": 650,
        "est_size_mb": 2.5
    },
    "psl": {
        "url": "https://cricsheet.org/downloads/psl_json.zip",
        "description": "Pakistan Super League (PSL) matches",
        "format": "T20",
        "est_matches": 350,
        "est_size_mb": 1.5
    },
    "cpl": {
        "url": "https://cricsheet.org/downloads/cpl_json.zip",
        "description": "Caribbean Premier League (CPL) matches",
        "format": "T20",
        "est_matches": 380,
        "est_size_mb": 1.6
    },
    "wbbl": {
        "url": "https://cricsheet.org/downloads/wbbl_json.zip",
        "description": "Women's Big Bash League (WBBL) matches",
        "format": "T20",
        "est_matches": 550,
        "est_size_mb": 2.2
    },
    "sa20": {
        "url": "https://cricsheet.org/downloads/sa20_json.zip",
        "description": "SA20 tournament matches (South Africa)",
        "format": "T20",
        "est_matches": 100,
        "est_size_mb": 0.5
    },
    "mlc": {
        "url": "https://cricsheet.org/downloads/mlc_json.zip",
        "description": "Major League Cricket (MLC) matches (USA)",
        "format": "T20",
        "est_matches": 50,
        "est_size_mb": 0.2
    },
    "odis": {
        "url": "https://cricsheet.org/downloads/odis_json.zip",
        "description": "One Day International (ODI) matches (Men's & Women's)",
        "format": "ODI",
        "est_matches": 2400,
        "est_size_mb": 18.0
    },
    "tests": {
        "url": "https://cricsheet.org/downloads/tests_json.zip",
        "description": "Test matches (Men's & Women's)",
        "format": "Test",
        "est_matches": 700,
        "est_size_mb": 25.0
    },
    "all_t20": {
        "url": "https://cricsheet.org/downloads/all_t20_json.zip",
        "description": "Combined All T20 matches globally (leagues + internationals)",
        "format": "T20",
        "est_matches": 8500,
        "est_size_mb": 35.0
    },
    "all": {
        "url": "https://cricsheet.org/downloads/all_json.zip",
        "description": "Complete Cricsheet data lake (All formats, Men's & Women's)",
        "format": "All",
        "est_matches": 16000,
        "est_size_mb": 85.0
    }
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

def list_datasets() -> Dict[str, Dict[str, Any]]:
    """Return dictionary of available Cricsheet datasets."""
    return DATASETS

def load_dataset(name: str, version: str = "1.0", cache_dir: Optional[str] = None, force: bool = False) -> MidwicketSession:
    """
    Downloads, extracts, and boots a localized MidwicketSession for the requested competition.

    Args:
        name: Name of competition dataset (e.g. 'ipl', 'bbl', 'all_t20')
        version: Dataset version string (default '1.0')
        cache_dir: Optional custom storage path. Defaults to ~/.midwicket_data/datasets
        force: If True, forces redownload and clean database rebuild.

    Returns:
        MidwicketSession: Initialized session loaded with dataset match tables.
    """
    # Map requested v2 aliases to canonical Cricsheet keys
    aliases = {
        "all_test": "tests",
        "all_odi": "odis",
        "women_t20": "wbbl"
    }
    canonical_name = name.lower().strip()
    if canonical_name in aliases:
        canonical_name = aliases[canonical_name]
        
    if canonical_name not in DATASETS:
        raise ValueError(
            f"Dataset '{name}' is not registered. "
            f"Choose from: {list(DATASETS.keys())}"
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
