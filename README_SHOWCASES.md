# Midwicket — What It Does in 60 Seconds

Midwicket is a cricket analytics library that turns 294,757 ball-by-ball IPL deliveries into production-grade insights with a single SQL query or four lines of Python.

The analyses below ran against **1,239 IPL matches (2008–2026)** using the local data store. No API calls. No cloud warehouse. Just `pip install midwicket` and your data.

---

## 10 Analyses That Show What's Possible

| # | Analysis | Surprising Finding | Charts |
|---|----------|--------------------|--------|
| 01 | [All-Time Run Leaders](#01-all-time-ipl-run-leaders) | Kohli's 9,228 runs beats #2 by nearly 2,000 — yet Warner has a higher SR | 2 charts |
| 02 | [Kohli's 19-Season Profile](#02-virat-kohlis-scoring-profile) | 2026 is his fastest-ever IPL season at age 37 (155.6 SR) | 2 charts |
| 03 | [Bumrah Death Over Dominance](#03-jasprit-bumrah-death-over-dominance) | Ranks #11 of 141 death bowlers — extraordinary at peak batter aggression | 2 charts |
| 04 | [Venue Scoring Atlas](#04-ipl-venue-scoring-atlas) | New Chandigarh scores 25% above average; New Wanderers 15% below | 2 charts |
| 05 | [Phase-wise Economy](#05-phase-wise-economy-leaders) | Bumrah is the only bowler elite in all three phases simultaneously | 2 charts |
| 06 | [Powerplay Kings](#06-powerplay-kings) | Vaibhav Suryavanshi 211 SR in PP — the highest ever recorded | 2 charts |
| 07 | [Chase Specialists](#07-chase-specialists) | 85%+ of batters perform better when chasing than setting | 2 charts |
| 08 | [Wicket Cluster Probability](#08-wicket-cluster-probability) | Ngidi takes 3+ wickets in 26% of appearances — highest of any bowler | 2 charts |
| 09 | [Dot Ball Kings](#09-dot-ball-kings) | Steyn's 44.65% dot rate would be structurally impossible in 2024 | 2 charts |
| 10 | [Season Scoring Trends](#10-ipl-scoring-trends-2008-2026) | Sixes/match nearly doubled (10.7 → 19.3) while dot ball % fell 6 points | 2 charts |

---

## 01 All-Time IPL Run Leaders

> *"Kohli leads with 9,228 runs — greener bars hit faster. Warner and KL Rahul score fewer runs but at elite strike rates."*

![Run Leaders](docs/showcases/01_run_leaders/01_run_leaders.png)

![Runs vs Strike Rate](docs/showcases/01_run_leaders/01_runs_vs_sr.png)

**The four-line query that produced this:**

```python
import midwicket as md

session = md.init("./data")
df = session.engine.execute_sql("""
    SELECT batter,
           SUM(runs_batter) AS total_runs,
           ROUND(SUM(runs_batter) * 100.0 / COUNT(*), 1) AS strike_rate,
           COUNT(DISTINCT match_id) AS matches
    FROM ball_events
    GROUP BY batter HAVING COUNT(*) >= 300
    ORDER BY total_runs DESC LIMIT 20
""").to_pandas()
```

[Full walkthrough](docs/showcases/01_run_leaders/WALKTHROUGH.md)

---

## 02 Virat Kohli's Scoring Profile

> *"2026 is Kohli's fastest-ever IPL season (155.6 SR). His phase heatmap shows AB de Villiers is the only batter who out-struck him in Death overs."*

![Kohli Phase and Season](docs/showcases/02_kohli_profile/02_kohli_phase_season.png)

![Kohli by Over](docs/showcases/02_kohli_profile/02_kohli_by_over.png)

[Full walkthrough](docs/showcases/02_kohli_profile/WALKTHROUGH.md)

---

## 03 Jasprit Bumrah — Death Over Dominance

> *"Among 141 bowlers with 120+ death-over balls, Bumrah ranks #11 by economy (8.07). The chart shows the full landscape he operates in."*

![Death Bowlers Scatter](docs/showcases/03_bumrah_death/03_death_bowlers.png)

![Bumrah Evolution](docs/showcases/03_bumrah_death/03_bumrah_evolution.png)

[Full walkthrough](docs/showcases/03_bumrah_death/WALKTHROUGH.md)

---

## 04 IPL Venue Scoring Atlas

> *"New Chandigarh (VBR 1.253) vs New Wanderers (VBR 0.848) — a 40% swing in expected scoring that changes every selection decision."*

![Venue Atlas](docs/showcases/04_venue_atlas/04_venue_atlas.png)

![VBR vs Wickets](docs/showcases/04_venue_atlas/04_vbr_vs_wickets.png)

**The metric:**
```
VBR = Venue 1st Innings Avg / Global 1st Innings Avg
VBR > 1.0 → bat-first advantage
VBR < 1.0 → bowl-first advantage
Venues with < 5 matches → VBR = 1.0 (stabilized)
```

[Full walkthrough](docs/showcases/04_venue_atlas/WALKTHROUGH.md)

---

## 05 Phase-wise Economy Leaders

> *"The heatmap shows Chahal is cheap in the Middle but expensive in Death. Bumrah alone sustains elite economy across all three phases."*

![Phase Economy](docs/showcases/05_phase_economy/05_phase_economy.png)

![Economy Heatmap](docs/showcases/05_phase_economy/05_economy_heatmap.png)

[Full walkthrough](docs/showcases/05_phase_economy/WALKTHROUGH.md)

---

## 06 Powerplay Kings

> *"Vaibhav Suryavanshi: 211 SR in 272 PP balls with 51 sixes — double the strike rate of a defensive batter, in the 6 overs when fielding restrictions are tightest."*

![Powerplay Kings](docs/showcases/06_powerplay_kings/06_pp_kings.png)

![Powerplay Detail](docs/showcases/06_powerplay_kings/06_pp_detail.png)

[Full walkthrough](docs/showcases/06_powerplay_kings/WALKTHROUGH.md)

---

## 07 Chase Specialists

> *"Pat Cummins hits 46 SR points faster when chasing than setting. 85% of IPL batters perform better in 2nd innings — chasing energises rather than pressures them."*

![Chase Scatter](docs/showcases/07_chase_specialists/07_chase_scatter.png)

![Chase Bars](docs/showcases/07_chase_specialists/07_chase_bars.png)

[Full walkthrough](docs/showcases/07_chase_specialists/WALKTHROUGH.md)

---

## 08 Wicket Cluster Probability

> *"Ngidi takes 3+ wickets in 26% of appearances. This 'ceiling metric' is the key fantasy cricket signal — not career wickets, but how often a bowler changes a match."*

![Wicket Clusters](docs/showcases/08_wicket_clusters/08_wicket_clusters.png)

![Cluster Scatter](docs/showcases/08_wicket_clusters/08_cluster_scatter.png)

[Full walkthrough](docs/showcases/08_wicket_clusters/WALKTHROUGH.md)

---

## 09 Dot Ball Kings

> *"DW Steyn: 44.65% dot rate, 6.79 economy. These numbers are structurally impossible in 2024 IPL — era-adjusted analysis reveals the true comparison."*

![Dot Ball Scatter](docs/showcases/09_dot_ball_kings/09_dot_ball_scatter.png)

![Dot Ball Bar](docs/showcases/09_dot_ball_kings/09_dot_pct_bar.png)

[Full walkthrough](docs/showcases/09_dot_ball_kings/WALKTHROUGH.md)

---

## 10 IPL Scoring Trends 2008–2026

> *"Avg first innings: 161 (2008) → 192 (2026). Sixes/match: 10.7 → 19.3. Dot balls: 36.5% → 31.1%. The game has fundamentally changed — this is the evidence."*

![Season Trends](docs/showcases/10_season_trends/10_season_trends.png)

![Boundary Trends](docs/showcases/10_season_trends/10_boundary_trends.png)

[Full walkthrough](docs/showcases/10_season_trends/WALKTHROUGH.md)

---

## Run It Yourself

```bash
pip install midwicket
```

```python
import midwicket as md

# Load 1,239 IPL matches locally — no downloads needed if you have the data
session = md.init("./data")

# Any analysis in the showcases is one SQL call away
df = session.engine.execute_sql("""
    SELECT batter, SUM(runs_batter) AS runs,
           ROUND(SUM(runs_batter) * 100.0 / COUNT(*), 1) AS sr
    FROM ball_events
    WHERE over >= 15                    -- death overs only
    GROUP BY batter HAVING COUNT(*) >= 60
    ORDER BY sr DESC LIMIT 10
""").to_pandas()

print(df)
```

```python
# Or use the high-level feature store
from midwicket.features import build_venue_bias_rating, build_match_context_score

vbr = build_venue_bias_rating(session)
mcs = build_match_context_score(session)
```

---

## What Midwicket Provides

| Layer | What it does |
|-------|-------------|
| **Data Ingestion** | Canonicalizes Cricsheet JSON to strict typed Arrow/DuckDB schema |
| **Identity Registry** | Resolves player/venue aliases across 17+ seasons (V Kohli = Virat Kohli) |
| **Feature Store** | 6 built-in metrics: Pressure Index, BQR, MCS, VBR, xRuns, Intent Score |
| **Query Engine** | Thread-safe DuckDB with snapshot management and temporal filtering |
| **Express API** | `md.init()` → `session.engine.execute_sql()` → done |

---

## Data

All analyses use **1,239 IPL matches (2008–2026)** from [Cricsheet](https://cricsheet.org/). The schema is versioned and typed — `over` is `int16`, runs are `int32`, retirements are correctly classified, and temporal filters are leak-proof.

See the [verification report](reports/verification_report.md) for the independent audit that confirmed 100% ingest success and 0 schema failures.
