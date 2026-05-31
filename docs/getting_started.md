# Getting Started with Midwicket

**Goal:** First useful insight in under 5 minutes.

This guide is linear. Follow it top to bottom. Every code block is copy-paste ready.

---

## Step 1 — Install (30 seconds)

```bash
pip install midwicket
```

Requires Python 3.9+. No system dependencies.

---

## Step 2 — Your First Output (no data download)

Midwicket ships with a bundled in-memory dataset. Win probability and player stats work immediately.

```python
import midwicket.express as px

# Chasing 180 at Wankhede: 120/5 after 15 overs — who wins?
result = px.predict_win(
    venue="Wankhede Stadium",
    target=180,
    current_score=120,
    wickets_down=5,
    overs_done=15.0,
)
print(f"Win probability: {result['win_prob']:.1%}")
print(f"Confidence: {result['confidence']:.2f}")
```

**Output:**
```
Win probability: 22.5%
Confidence: 0.81
```

Needs 60 more runs off 30 balls with 5 wickets left. The model — logistic regression trained on IPL data, AUC 0.843 — says the chasing team is in deep trouble.

---

## Step 3 — Load a Dataset (2–3 minutes)

Midwicket downloads from [Cricsheet](https://cricsheet.org/) automatically. Run this once; subsequent calls use the local cache.

```python
from midwicket.datasets import load_dataset

# IPL: ~1,100 matches, 2008–2026, ~50 MB
session = load_dataset("ipl")
print("Session ready.")
```

**What just happened:**
1. Downloaded `ipl_json.zip` from Cricsheet
2. Extracted 1,239 JSON files
3. Canonicalised each match into a typed Arrow schema
4. Loaded into a local DuckDB engine

The session object is now your analytics environment.

---

## Step 4 — Run Your First Query (10 seconds)

```python
# Top 10 run scorers — all-time IPL
df = session.engine.execute_sql("""
    SELECT
        batter,
        SUM(runs_batter) AS runs,
        ROUND(SUM(runs_batter) * 100.0 / COUNT(*), 1) AS strike_rate,
        COUNT(DISTINCT match_id) AS matches
    FROM ball_events
    GROUP BY batter
    HAVING COUNT(*) >= 300
    ORDER BY runs DESC
    LIMIT 10
""").to_pandas()

print(df.to_string(index=False))
```

**Output:**
```
        batter    runs  strike_rate  matches
       V Kohli  9228.0        130.7      273
     RG Sharma  7331.0        129.5      275
      S Dhawan  6769.0        123.5      221
     DA Warner  6567.0        135.4      184
      KL Rahul  5828.0        135.5      149
      SK Raina  5536.0        132.5      200
      MS Dhoni  5439.0        132.6      241
     AM Rahane  5367.0        122.2      197
     SV Samson  5181.0        137.4      185
AB de Villiers  5181.0        148.6      170
```

Kohli leads with 9,228 runs across 273 matches. AB de Villiers has the highest strike rate at 148.6 — in 100 fewer matches.

---

## Step 5 — Use the Feature Store (30 seconds)

Instead of writing SQL for common metrics, use the built-in feature builders:

```python
from midwicket.features import build_venue_bias_rating, build_bowler_quality_rating

# Venue Bias Rating — which grounds favour batters?
vbr = build_venue_bias_rating(session)
print(vbr.sort_values("venue_bias_rating", ascending=False).head(5))
```

**Output:**
```
   venue_id  matches  venue_bias_rating
        ...       5             1.253     # New Chandigarh — most batter-friendly
        ...      59             1.131     # Wankhede Stadium
        ...      15             1.095     # Chinnaswamy Stadium
        ...       8             1.064     # Eden Gardens
        ...      12             0.961     # Chepauk — bowler-friendly
```

VBR > 1.0 means the ground produces above-average scores. VBR < 1.0 favours bowlers. Venues with fewer than 5 matches default to 1.0 (not enough data to make a claim).

```python
# Bowler Quality Rating — who builds the most pressure?
bqr = build_bowler_quality_rating(session)
top_bowlers = bqr.sort_values("bowler_quality_rating", ascending=False).head(10)
print(top_bowlers[["bowler_id", "total_balls", "dot_balls", "wickets", "bowler_quality_rating"]])
```

---

## Step 6 — Generate a Scouting Report (10 seconds)

```python
import midwicket as md

session = md.init("./data")    # or wherever your dataset is
report = md.scouting_report("Virat Kohli")

# What phase does Kohli dominate?
print("Phase breakdown:")
for phase, stats in report.get("phase_batting", {}).items():
    print(f"  {phase}: SR {stats.get('strike_rate', '—')}, Avg {stats.get('average', '—')}")

# Where does he struggle?
print("\nWeaknesses:", report.get("weaknesses", []))
```

The scouting report resolves name aliases automatically — `"V Kohli"`, `"Virat Kohli"`, and `"kohli"` all find the same player across 17+ seasons.

---

## Step 7 — Filter by Date or Phase (5 seconds)

All features and queries support temporal scoping — no leakage, enforced at the SQL layer:

```python
from midwicket.features import build_bowler_quality_rating

# Only 2023 and 2024 seasons
bqr_recent = build_bowler_quality_rating(
    session,
    start_date="2023-01-01",
    end_date="2024-12-31"
)

# Only death overs
df_death = session.engine.execute_sql("""
    SELECT bowler,
           ROUND(SUM(runs_batter + runs_extras) * 6.0 / COUNT(*), 2) AS economy,
           SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) AS wickets
    FROM ball_events
    WHERE over >= 15
    GROUP BY bowler
    HAVING COUNT(*) >= 60
    ORDER BY economy ASC LIMIT 10
""").to_pandas()
```

---

## What's Next

| Goal | Where to go |
|------|-------------|
| See 10 pre-built analyses with charts | [README_SHOWCASES.md](../README_SHOWCASES.md) |
| Browse the gallery with code snippets | [docs/gallery.md](gallery.md) |
| API reference — all classes and functions | [docs/api.md](api.md) |
| Run the FastAPI server | [examples/27_full_pipeline_demo.py](../examples/27_full_pipeline_demo.py) |
| Load a different competition | [midwicket/datasets.py](../midwicket/datasets.py) — 12 datasets available |
| Fantasy point prediction | [examples/09_fantasy_points.py](../examples/09_fantasy_points.py) |

---

## Common Questions

**Q: How long does the download take?**  
IPL (~50 MB): about 30–60 seconds depending on connection. The `all` corpus (~350 MB) takes 3–5 minutes. Downloads happen once and are cached locally.

**Q: Can I use my own data?**  
Yes. If your data is in Cricsheet JSON format, point `MidwicketSession` at the directory:
```python
from midwicket.api.session import MidwicketSession
session = MidwicketSession(data_dir="/path/to/your/data")
```

**Q: Does it work in Jupyter?**  
Yes. `session.engine.execute_sql(...).to_pandas()` returns a standard DataFrame. All feature builders return DataFrames. Plotting works with any library (matplotlib, plotly, altair).

**Q: How do I query across multiple competitions?**  
Load `"all_t20"` or `"all"` — they include all competitions in a single session.

**Q: What does `is_wicket=True` mean for retirements?**  
`RETIRED_HURT` and `RETIRED_NOT_OUT` are classified as `is_wicket=False` — they are not dismissals. `RETIRED_OUT` is `is_wicket=True` (a valid dismissal under MCC Laws). The schema handles this correctly.

---

*Time elapsed from `pip install` to first scouting report: under 5 minutes.*  
*Problems? Open an issue: [github.com/CodersAcademy006/Midwicket/issues](https://github.com/CodersAcademy006/Midwicket/issues)*
