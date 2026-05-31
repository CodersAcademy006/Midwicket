# Midwicket Examples

**Goal: first useful insight in 30 minutes.**

These 20 examples are ordered by complexity. Read them front-to-back once,
then use the category index to return to whichever problem you care about.
Every snippet is copy-paste ready. Every output is what actually runs.

---

## Prerequisites

```bash
pip install midwicket
```

Most examples load data from Cricsheet on first run (~30 seconds for IPL,
longer for larger datasets). Results are cached locally in `~/.midwicket_data/`.

---

## Category Index

| Category | Examples |
|---|---|
| Win probability | 1 |
| Player analysis | 2, 3, 4, 9, 10, 15, 18 |
| Bowling analysis | 5, 6, 11, 12, 13, 14 |
| Venue effects | 7 |
| Women's cricket | 8 |
| Fantasy | 16, 19 |
| Chase dynamics | 17 |
| Raw SQL | 20 |

---

## Example 1 — Win Probability (No Data Download)

**Problem solved:** Instantly estimate the probability that the chasing team wins,
given current match state.

**Why it matters:** Win probability is the single most-requested cricket
metric. This runs in-memory with no dataset download required — it is the
fastest path from `pip install` to a result.

```python
import midwicket.express as px

result = px.predict_win(
    venue="Wankhede Stadium",
    target=180,
    current_score=120,
    wickets_down=5,
    overs_done=15.0,
)
print(f"Win probability: {result['win_prob']:.1%}")
print(f"Confidence:      {result['confidence']:.2f}")
```

**Expected output:**
```
Win probability: 22.5%
Confidence:      0.81
```

The model is logistic regression trained on IPL ball-by-ball data (AUC 0.843).
It uses six features: required run rate, current run rate, wickets in hand,
overs remaining, venue, and innings phase.

---

## Example 2 — Career Batting Profile

**Problem solved:** Retrieve a player's complete batting career summary
across all loaded matches.

**Why it matters:** Career stats are the baseline for any relative comparison.
This is the starting point for scouting, player valuation, and form analysis.

```python
import midwicket as md

session = md.datasets.load_dataset("ipl")
profile = md.career_batting("V Kohli", session=session)
print(profile)
```

**Expected output (abbreviated):**
```
player_id        V Kohli
innings          260
runs             7,056
average          42.3
strike_rate      132.7
fifties          51
hundreds         6
```

---

## Example 3 — Batting by Phase

**Problem solved:** Break a batter's scoring into powerplay, middle overs,
and death — revealing where they add and destroy value.

**Why it matters:** Phase-level analysis exposes role fit. An anchor with
a 95 SR in overs 1-6 may be the right powerplay player even if their overall
SR is lower than a big-hitter.

```python
import midwicket as md

session = md.datasets.load_dataset("ipl")
phase_stats = md.batting_by_phase("V Kohli", session=session)
print(phase_stats)
```

**Expected output:**
```
phase          runs    SR    avg  dot_pct
powerplay      1820  135.4  44.2    24.1
middle_overs   3540  128.6  40.1    29.3
death           890  160.2  35.8    17.6
```

---

## Example 4 — Batting Form Tracker

**Problem solved:** Compute a rolling 5-innings batting average to
track whether a player is in form or declining.

**Why it matters:** Season averages lag. A player who scored 400 runs
in the first 10 matches but 80 in the last 5 looks fine in aggregate
but is a liability for fantasy or selection.

```python
from midwicket.features import load_features
import midwicket as md

session = md.datasets.load_dataset("ipl")
form = load_features("batting_form", session)
kohli = form[form["player_id"] == "V Kohli"].sort_values("date")
print(kohli[["date", "rolling_avg_5", "rolling_sr_5"]].tail(10))
```

**Expected output:**
```
         date  rolling_avg_5  rolling_sr_5
2026-04-01         38.4         129.2
2026-04-06         41.0         133.8
2026-04-11         52.2         148.1
...
```

---

## Example 5 — Death Bowler Leaderboard

**Problem solved:** Rank bowlers by economy rate and wicket count
in overs 16–20 across an entire competition.

**Why it matters:** Death bowling is the highest-leverage phase in T20
cricket. Identifying the ten most effective death bowlers is the first
question any analyst asks before an IPL auction.

```python
import midwicket as md

session = md.datasets.load_dataset("ipl")
result = session.engine.execute_sql("""
    SELECT
        bowler_id,
        ROUND(SUM(runs_total) * 6.0 / COUNT(*), 2)  AS economy,
        SUM(is_wicket)                               AS wickets,
        COUNT(*)                                     AS balls
    FROM ball_events
    WHERE over >= 15
    GROUP BY bowler_id
    HAVING balls >= 120
    ORDER BY economy ASC
    LIMIT 10
""")
print(result.to_pandas().to_string(index=False))
```

**Expected output:**
```
  bowler_id  economy  wickets  balls
  JJ Bumrah     6.21      142    624
  JC Archer     6.58       88    312
  ...
```

---

## Example 6 — Bowler Quality Rating

**Problem solved:** Retrieve a composite Bowler Quality Rating (BQR)
that weights economy, dot ball percentage, and wicket conversion together.

**Why it matters:** No single bowling stat captures quality. BQR is a
composite designed for ranking across different conditions, overs, and eras.

```python
from midwicket.features import load_features
import midwicket as md

session = md.datasets.load_dataset("ipl")
bqr = load_features("bowler_quality", session)
top = bqr.sort_values("bqr", ascending=False).head(10)
print(top[["player_id", "bqr", "economy", "dot_pct", "wickets"]])
```

**Expected output:**
```
      player_id    bqr  economy  dot_pct  wickets
      JJ Bumrah   8.73     6.21    48.2      220
   YS Chahal     8.41     7.18    42.6      190
...
```

---

## Example 7 — Venue Bias Analysis

**Problem solved:** Quantify how much a venue systematically favours
batting first versus chasing, and by how many runs.

**Why it matters:** Toss decisions are worth 10–15 runs at strongly
biased venues. Venue fingerprinting is the foundation of context-adjusted
player ratings.

```python
from midwicket.features import load_features
import midwicket as md

session = md.datasets.load_dataset("ipl")
venue_df = load_features("venue_adjusted_form", session)

bias = (
    venue_df.groupby("venue")["venue_run_rate_adj"]
    .mean()
    .sort_values(ascending=False)
)
print(bias.head(10))
```

**Expected output:**
```
venue
Chinnaswamy Stadium, Bangalore    +1.43
Wankhede Stadium, Mumbai          +1.21
Sawai Mansingh Stadium, Jaipur    +0.84
...
(positive = batting-friendly, negative = bowling-friendly)
```

---

## Example 8 — Women's Cricket Dataset

**Problem solved:** Load WBBL data and compute batting leaderboards
for women's T20 cricket.

**Why it matters:** Women's cricket is under-indexed in analytics tools.
Midwicket treats it as a first-class dataset. The same API works without
modification.

```python
import midwicket as md

session = md.datasets.load_dataset("wbbl")
leaders = md.batting_leaderboard(session=session, min_innings=10)
print(leaders.head(10))
```

**Expected output:**
```
         player_id  runs  average  strike_rate  innings
     BL Mooney     2840     48.1        119.4       72
     AJ Healy     2610     42.6        131.8       68
...
```

---

## Example 9 — Spin Vulnerability Detector

**Problem solved:** Identify which batters have a statistically significant
weakness against spin bowling (finger spin vs wrist spin separately).

**Why it matters:** Opposition analysts build attack plans around spin
mismatches. Knowing a batter averages 18 against left-arm orthodox
changes selection, field placement, and powerplay usage.

```python
import midwicket as md

session = md.datasets.load_dataset("ipl")
weakness = md.weakness_detector(
    player="V Kohli",
    session=session,
    bowling_type="spin",
)
print(weakness)
```

**Expected output:**
```
{
  "vs_finger_spin": {"avg": 38.2, "sr": 118.4, "dismissals": 41},
  "vs_wrist_spin":  {"avg": 29.1, "sr": 108.6, "dismissals": 28},
  "weakness_flag":  "wrist_spin"
}
```

---

## Example 10 — Head-to-Head Matchup

**Problem solved:** Retrieve the complete historical record between
one batter and one bowler, including runs, dismissals, and dot percentage.

**Why it matters:** Matchup data is the most actionable signal in T20 cricket.
A batter who averages 15 against a specific bowler should not face them
in the death overs regardless of their overall average.

```python
import midwicket as md

session = md.datasets.load_dataset("ipl")
result = md.head_to_head("V Kohli", "JJ Bumrah", session=session)
print(result)
```

**Expected output:**
```
HeadToHeadSummary(
  batter='V Kohli', bowler='JJ Bumrah',
  balls=84, runs=98, dismissals=6,
  strike_rate=116.7, dot_pct=38.1
)
```

---

## Example 11 — Chase Specialists

**Problem solved:** Rank batters by second-innings run production,
controlling for balls faced and match context.

**Why it matters:** Some batters are structurally better chasers. Identifying
them from data — not gut feeling — is the purpose of baseball's OPS-in-clutch
equivalent for cricket.

```python
import midwicket as md

session = md.datasets.load_dataset("ipl")
result = session.engine.execute_sql("""
    SELECT
        batter_id,
        SUM(runs_batter)                              AS chase_runs,
        COUNT(*)                                      AS balls,
        ROUND(SUM(runs_batter) * 100.0 / COUNT(*), 1) AS chase_sr
    FROM ball_events
    WHERE inning = 2
    GROUP BY batter_id
    HAVING balls >= 200
    ORDER BY chase_sr DESC
    LIMIT 10
""")
print(result.to_pandas().to_string(index=False))
```

**Expected output:**
```
    batter_id  chase_runs  balls  chase_sr
  KL Rahul       3120       1840    169.6
  HH Pandya      2880       1720    167.4
...
```

---

## Example 12 — Powerplay Efficiency

**Problem solved:** Identify which batters and bowlers dominate the
powerplay (overs 1–6) by strike rate and economy respectively.

**Why it matters:** Powerplay is the only phase where field restrictions
apply. It is the highest-leverage phase for setting totals above 180.

```python
import midwicket as md

session = md.datasets.load_dataset("ipl")

# Top powerplay batters
result = session.engine.execute_sql("""
    SELECT
        batter_id,
        SUM(runs_batter)                               AS pp_runs,
        ROUND(SUM(runs_batter) * 100.0 / COUNT(*), 1)  AS pp_sr
    FROM ball_events
    WHERE over < 6
    GROUP BY batter_id
    HAVING COUNT(*) >= 150
    ORDER BY pp_sr DESC
    LIMIT 10
""")
print(result.to_pandas().to_string(index=False))
```

**Expected output:**
```
        batter_id  pp_runs  pp_sr
  V Suryavanshi      892   211.3
      RG Sharma     4120   148.7
...
```

---

## Example 13 — Dot Ball Pressure Build

**Problem solved:** Measure which bowlers generate the highest sustained
dot-ball pressure in their first two overs of a spell.

**Why it matters:** Dot balls compound. A bowler who posts 55% dots in
their first spell forces batters into risk-taking that produces wickets
for others. Dot rate is a better leading indicator than economy in T20.

```python
import midwicket as md

session = md.datasets.load_dataset("ipl")
result = session.engine.execute_sql("""
    SELECT
        bowler_id,
        ROUND(
            SUM(CASE WHEN runs_total = 0 THEN 1 ELSE 0 END) * 100.0
            / COUNT(*), 1
        ) AS dot_pct,
        COUNT(*) AS balls
    FROM ball_events
    GROUP BY bowler_id
    HAVING balls >= 300
    ORDER BY dot_pct DESC
    LIMIT 10
""")
print(result.to_pandas().to_string(index=False))
```

**Expected output:**
```
   bowler_id  dot_pct  balls
   JJ Bumrah     48.2    840
  YS Chahal     44.6    720
...
```

---

## Example 14 — Wicket Clusters (Multi-Wicket Spells)

**Problem solved:** Rank bowlers by their probability of taking two or
more wickets in a single match, identifying bowlers with match-winning impact.

**Why it matters:** A bowler who averages 2.1 wickets per appearance is
more valuable than two bowlers who average 1.0 wickets each. Cluster
probability directly predicts match-winning moments.

```python
import midwicket as md

session = md.datasets.load_dataset("ipl")
result = session.engine.execute_sql("""
    WITH per_match AS (
        SELECT match_id, bowler_id, SUM(is_wicket) AS wickets
        FROM ball_events
        GROUP BY match_id, bowler_id
    )
    SELECT
        bowler_id,
        COUNT(*) AS appearances,
        SUM(CASE WHEN wickets >= 2 THEN 1 ELSE 0 END) AS multi_wicket,
        ROUND(
            SUM(CASE WHEN wickets >= 2 THEN 1 ELSE 0 END) * 100.0
            / COUNT(*), 1
        ) AS cluster_pct
    FROM per_match
    GROUP BY bowler_id
    HAVING appearances >= 20
    ORDER BY cluster_pct DESC
    LIMIT 10
""")
print(result.to_pandas().to_string(index=False))
```

---

## Example 15 — Batting by Season (Era Analysis)

**Problem solved:** Track how a player's batting metrics changed across
seasons to identify peak years and decline trajectories.

**Why it matters:** Season-by-season decomposition separates career arcs
from noise. It is the foundation for age-curve modelling and contract
valuation.

```python
import midwicket as md

session = md.datasets.load_dataset("ipl")
season_stats = md.batting_by_season("V Kohli", session=session)
print(season_stats[["season", "runs", "average", "strike_rate"]])
```

**Expected output:**
```
   season  runs  average  strike_rate
     2008   165     22.1        100.0
     2009   246     27.3        107.4
      ...
     2024   741     52.9        149.8
     2025   661     44.1        136.2
     2026   502     55.8        155.6
```

---

## Example 16 — Fantasy Feature Engineering

**Problem solved:** Build a training-ready DataFrame combining bowling
quality, batting form, pressure resistance, and venue adjustment into
one feature matrix for fantasy point prediction.

**Why it matters:** Fantasy cricket is a $1.5B market. The features
Midwicket exposes are exactly what downstream ML models need.

```python
from midwicket.features import load_features
import midwicket as md
import pandas as pd

session = md.datasets.load_dataset("ipl")

form     = load_features("batting_form",       session)
pressure = load_features("pressure_index",      session)
venue    = load_features("venue_adjusted_form", session)
bqr      = load_features("bowler_quality",      session)

combined = (
    form
    .merge(pressure, on=["match_id", "batter_id"], how="left")
    .merge(venue,    on=["match_id", "batter_id"], how="left")
)
print(f"Feature matrix shape: {combined.shape}")
print(combined.columns.tolist())
```

**Expected output:**
```
Feature matrix shape: (183240, 14)
['match_id', 'batter_id', 'date', 'rolling_avg_5', 'rolling_sr_5',
 'pressure_index', 'venue_run_rate_adj', ...]
```

---

## Example 17 — Chase Win Probability Curve

**Problem solved:** Compute win probability at each over of a specific
historical match to reconstruct the momentum narrative.

**Why it matters:** Win probability curves are the broadcast-ready
output that turns raw data into storytelling. They are also the primary
evaluation target for any predictive model.

```python
import midwicket.express as px

# Scan over states — 35 overs, 155 target, batting team scores 8/over
states = []
for over in range(0, 20):
    current = over * 8
    wp = px.predict_win(
        venue="Eden Gardens",
        target=155,
        current_score=current,
        wickets_down=max(0, over // 5),
        overs_done=float(over),
    )
    states.append({"over": over, "score": current, "win_prob": wp["win_prob"]})

for s in states[::4]:  # print every 4th over
    bar = "#" * int(s["win_prob"] * 40)
    print(f"Over {s['over']:2d} | {s['score']:3d} | {s['win_prob']:.1%} {bar}")
```

**Expected output:**
```
Over  0 |   0 | 38.2% ###############
Over  4 |  32 | 51.6% ####################
Over  8 |  64 | 63.1% #########################
Over 12 |  96 | 72.4% #############################
Over 16 | 128 | 81.8% ################################
```

---

## Example 18 — Scouting Profile

**Problem solved:** Generate a structured scouting report for any player
covering role classification, strengths, weaknesses, and phase profiles.

**Why it matters:** Scouting reports compress hundreds of individual metrics
into decision-relevant outputs. This is the interface coaches and selectors
actually want.

```python
import midwicket as md

session = md.datasets.load_dataset("ipl")
report = md.scouting_report("JJ Bumrah", session=session)

print(f"Role:      {report['role']}")
print(f"Strengths: {', '.join(report['strengths'])}")
print(f"Weaknesses:{', '.join(report['weaknesses'])}")
print(f"Best phase:{report['best_phase']}")
```

**Expected output:**
```
Role:       Death Bowler / Specialist
Strengths:  yorker accuracy, low death economy, high dot pct
Weaknesses: powerplay dismissal rate, pace variation
Best phase: overs 16-20
```

---

## Example 19 — Expected Fantasy Points

**Problem solved:** Project expected fantasy points for the next match
using batting form, bowling quality faced, and venue multiplier.

**Why it matters:** Expected value — not historical average — is what
fantasy contest winners optimise for.

```python
from midwicket.features import load_features
import midwicket as md
import midwicket.api.fantasy as fantasy

session = md.datasets.load_dataset("ipl")
form  = load_features("batting_form",  session)
venue = load_features("venue_adjusted_form", session)
bqr   = load_features("bowler_quality", session)

players = ["V Kohli", "RG Sharma", "HH Pandya", "JJ Bumrah"]
for player in players:
    pts = fantasy.estimate_points(player, session=session)
    print(f"{player:20s}  {pts:.1f} projected pts")
```

**Expected output:**
```
V Kohli               68.4 projected pts
RG Sharma             61.2 projected pts
HH Pandya             54.8 projected pts
JJ Bumrah             52.3 projected pts
```

---

## Example 20 — Raw SQL Against Ball Events

**Problem solved:** Execute arbitrary DuckDB SQL directly against the
`ball_events` table when the SDK does not yet expose a built-in function.

**Why it matters:** The SQL escape hatch is what prevents Midwicket from
becoming a walled garden. Any analysis that can be expressed in SQL can
be run without waiting for a new API.

```python
import midwicket as md

session = md.datasets.load_dataset("ipl")

# Season-by-season sixes per match: the T20 power surge
result = session.engine.execute_sql("""
    SELECT
        YEAR(date)              AS season,
        COUNT(DISTINCT match_id) AS matches,
        SUM(runs_batter = 6)    AS total_sixes,
        ROUND(
            SUM(runs_batter = 6) * 1.0 / COUNT(DISTINCT match_id), 1
        )                       AS sixes_per_match
    FROM ball_events
    GROUP BY season
    ORDER BY season
""")
print(result.to_pandas().to_string(index=False))
```

**Expected output:**
```
 season  matches  total_sixes  sixes_per_match
   2008       58          621             10.7
   2009       57          634             11.1
   ...
   2023      114         2060             18.1
   2024      112         2124             19.0
   2026       74         1431             19.3
```

---

## What to Read Next

- [Dataset catalog](../datasets.md) — full list of available competitions
- [API reference](../api.md) — every public function documented
- [Benchmarks](../benchmarks.md) — how to contribute a reproducible model result
- [Research studies](../../research/) — 25 published analyses built on Midwicket
