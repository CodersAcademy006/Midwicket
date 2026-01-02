import requests
import zipfile
import json
from pathlib import Path
from typing import Iterator, Dict, Any, Optional
from tqdm import tqdm

# Constants
CRICSHEET_URL = "https://cricsheet.org/downloads/ipl_json.zip"
DEFAULT_DATA_DIR = Path.home() / ".pypitch_data"

class DataLoader:
    def __init__(self, data_dir: Optional[str] = None):
        """
        Manages raw data storage.
        Defaults to ~/.pypitch_data/ to keep the user's project clean.
        """
        self.data_dir = Path(data_dir) if data_dir else DEFAULT_DATA_DIR
        self.raw_dir = self.data_dir / "raw" / "ipl"
        self.zip_path = self.data_dir / "ipl_json.zip"
        
        # Ensure directories exist
        self.raw_dir.mkdir(parents=True, exist_ok=True)

    def download(self, force: bool = False) -> None:
        """
        Downloads the latest dataset from Cricsheet.
        Skips if already exists, unless force=True.
        """
        if self.zip_path.exists() and not force:
            print(f"[OK] Data already exists at {self.zip_path}")
            return

        print(f"[INFO] Downloading IPL Data from {CRICSHEET_URL}...")
        
        try:
            response = requests.get(CRICSHEET_URL, stream=True)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            
            with open(self.zip_path, 'wb') as f, tqdm(
                desc="Downloading",
                total=total_size,
                unit='iB',
                unit_scale=True,
                unit_divisor=1024,
            ) as bar:
                for chunk in response.iter_content(chunk_size=8192):
                    size = f.write(chunk)
                    bar.update(size)
            
            print("[INFO] Extracting files...")
            self._extract()
            print("[SUCCESS] Download Complete.")
            
        except Exception as e:
            # Clean up partial downloads
            if self.zip_path.exists():
                self.zip_path.unlink()
            raise ConnectionError(f"Failed to download data: {e}")

    def _extract(self) -> None:
        """Unzips the downloaded file into the raw directory."""
        with zipfile.ZipFile(self.zip_path, 'r') as z:
            z.extractall(self.raw_dir)

    def get_match(self, match_id: str) -> Dict[str, Any]:
        """
        Fetches a specific match by ID.
        """
        # Try direct JSON
        file_path = self.raw_dir / f"{match_id}.json"
        if not file_path.exists():
            # Try finding it if the ID format is different or just to be safe
            # But for now, assume ID matches filename
            raise FileNotFoundError(f"Match {match_id} not found in {self.raw_dir}")
            
        with open(file_path, 'r') as f:
            return json.load(f)

    def iter_matches(self) -> Iterator[Dict[str, Any]]:
        """
        Yields match data one by one.
        Generator pattern prevents RAM overflow when processing 10k+ matches.
        """
        json_files = list(self.raw_dir.glob("*.json"))
        
        if not json_files:
            raise FileNotFoundError("No JSON files found. Run loader.download() first.")
            
        print(f"[INFO] Found {len(json_files)} matches in {self.raw_dir}...")
        
        for file_path in json_files:
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    # Basic validation: ensure it looks like a match file
                    if 'info' in data and 'innings' in data:
                        yield data
            except json.JSONDecodeError:
                continue # Skip corrupt files
