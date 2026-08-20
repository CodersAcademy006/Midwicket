import requests
import zipfile
import io
import json
import sys

DATASET_URLS = {
    "ipl": "https://cricsheet.org/downloads/ipl_json.zip",
    "bbl": "https://cricsheet.org/downloads/bbl_json.zip",
    "wbbl": "https://cricsheet.org/downloads/wbb_json.zip",
    "psl": "https://cricsheet.org/downloads/psl_json.zip",
    "cpl": "https://cricsheet.org/downloads/cpl_json.zip",
    "sa20": "https://cricsheet.org/downloads/sat_json.zip",
    "mlc": "https://cricsheet.org/downloads/mlc_json.zip",
    "wpl": "https://cricsheet.org/downloads/wpl_json.zip",
    "hundred": "https://cricsheet.org/downloads/hnd_json.zip",
    "t20is": "https://cricsheet.org/downloads/t20s_json.zip",
    "odis": "https://cricsheet.org/downloads/odis_json.zip",
    "tests": "https://cricsheet.org/downloads/tests_json.zip",
    "all": "https://cricsheet.org/downloads/all_json.zip",
}

def analyze_dataset(name, url):
    print(f"Analyzing {name} from {url}...", file=sys.stderr)
    r = requests.get(url)
    if r.status_code != 200:
        print(f"Error downloading {name}: {r.status_code}", file=sys.stderr)
        return None
    
    cl = len(r.content)
    size_mb = round(cl / (1024 * 1024), 2)
    
    matches_count = 0
    deliveries_count = 0
    players = set()
    venues = set()
    seasons = set()
    events = set()
    
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        for filename in z.namelist():
            if filename.endswith(".json") and filename != "README.txt":
                matches_count += 1
                try:
                    data = json.loads(z.read(filename).decode("utf-8"))
                    info = data.get("info", {})
                    
                    # Deliveries
                    for inning in data.get("innings", []):
                        for over in inning.get("overs", []):
                            deliveries_count += len(over.get("deliveries", []))
                    
                    # Players
                    for team_players in info.get("players", {}).values():
                        players.update(team_players)
                    
                    # Venues
                    venue = info.get("venue")
                    if venue:
                        venues.add(venue)
                        
                    # Seasons
                    season = info.get("season")
                    if season:
                        seasons.add(str(season))
                        
                    # Events
                    event_info = info.get("event", {})
                    event_name = event_info.get("name") if isinstance(event_info, dict) else None
                    if event_name:
                        events.add(event_name)
                except Exception as e:
                    print(f"Error parsing {filename} in {name}: {e}", file=sys.stderr)
                    
    min_season = sorted(list(seasons))[0] if seasons else None
    max_season = sorted(list(seasons))[-1] if seasons else None
    date_range = f"{min_season}–{max_season}" if min_season and max_season else ""
    
    # Let's map events to clean competition names or list them
    competitions = sorted(list(events))
    
    # Coverage logic
    # Affected datasets where complete is incorrect: t20is, odis, tests, all
    coverage = "partial" if name in ["t20is", "odis", "tests", "all"] else "complete"
    
    return {
        "name": name,
        "url": url,
        "matches": matches_count,
        "deliveries": deliveries_count,
        "players": len(players),
        "venues": len(venues),
        "date_range": date_range,
        "coverage": coverage,
        "competitions": competitions,
        "size_mb": size_mb
    }

if __name__ == "__main__":
    results = {}
    for name, url in DATASET_URLS.items():
        res = analyze_dataset(name, url)
        if res:
            results[name] = res
            print(json.dumps(res, indent=2))
