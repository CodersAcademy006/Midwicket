# Showcase 01 — All-Time IPL Run Leaders

**One sentence:** Rank every batter in 17 seasons of IPL with runs, average, and strike rate — in four lines of SQL.

---

## The Question

Who are the greatest IPL batters of all time, and do the best scorers also score the fastest?

## The Data

- **1,239 IPL matches** (2008–2026), **294,757 deliveries**
- Filter: minimum 300 balls faced (ensures meaningful sample size)
- Result: **top 20 batters by career runs**

## The Insight

![Run Leaders Bar Chart](01_run_leaders.png)

Virat Kohli leads with **9,228 runs** — 1,897 more than Rohit Sharma in second. The colour coding by strike rate reveals the tension that defines T20 batting: Kohli's volume coexists with a 130.7 SR, while Warner (135.4) and KL Rahul (135.5) score fewer runs but at higher efficiency.

![Runs vs Strike Rate Scatter](01_runs_vs_sr.png)

The scatter plot maps **total volume vs scoring rate** with bubble size showing matches played. The top-right quadrant (many runs AND high SR) is where the rare "complete" T20 batter lives. AB de Villiers anchors this space with 5,181 runs at an extraordinary strike rate despite playing far fewer IPL seasons.

## Key Numbers

| Batter | Runs | Strike Rate | Avg | Sixes |
|--------|------|-------------|-----|-------|
| V Kohli | 9,228 | 130.7 | 41.8 | — |
| RG Sharma | 7,331 | 129.5 | 31.3 | — |
| DA Warner | 6,567 | 135.4 | 42.6 | — |
| KL Rahul | 5,828 | 135.5 | 47.0 | — |
| AB de Villiers | 5,181 | — | — | — |

## The Query

```sql
SELECT
    batter,
    SUM(runs_batter) AS total_runs,
    COUNT(*) AS balls_faced,
    ROUND(SUM(runs_batter) * 100.0 / COUNT(*), 1) AS strike_rate,
    ROUND(SUM(runs_batter) * 1.0 /
          NULLIF(SUM(CASE WHEN is_wicket AND wicket_type NOT IN
                    ('RUN_OUT','OBSTRUCTING_THE_FIELD','RETIRED_OUT',
                     'RETIRED_HURT','RETIRED_NOT_OUT') THEN 1 ELSE 0 END), 0), 1) AS batting_avg
FROM ball_events
GROUP BY batter
HAVING COUNT(*) >= 300
ORDER BY total_runs DESC
LIMIT 20;
```

## Files

| File | Description |
|------|-------------|
| `01_run_leaders.png` | Horizontal bar chart — runs coloured by strike rate |
| `01_runs_vs_sr.png` | Scatter — runs vs SR, bubble=matches, colour=sixes |
| `run_leaders.csv` | Full data table |
| `output.txt` | Raw SQL results |
