# Showcase 10 — IPL Scoring Trends: 18 Years of Evolution

**One sentence:** Average first innings scores rose from 161 in 2008 to 192 in 2026 — 19 more runs per innings — while sixes per match nearly doubled from 10.7 to 19.3.

---

## The Question

How has T20 batting evolved over 18 years of IPL? Are today's batters genuinely better, or are they just batting in an era of flat pitches?

## The Insight

![Season Trends Four-Panel](10_season_trends.png)

The four-panel dashboard tells the story of **T20's revolution**:

**Panel 1 — Avg Team Score:** A steady upward trend. 2023 (182.6) and 2026 (191.7) are the two highest seasons ever. The trend line shows approximately **+1.5 runs per year** on average — a staggering structural shift.

**Panel 2 — Sixes per Match:** From 10.7 in 2008 to 19.3 in 2026. The introduction of impact players, the evolution of switch-hits, ramps, and scoops drove the 2023–2026 explosion. The darkening color gradient visualizes the acceleration.

**Panel 3 — Dot Ball % Declining:** From 37.6% (2009) to 30.9% (2025). Batters are attacking more deliveries. Every ball that used to be "respected" is now targeted.

**Panel 4 — Wickets Increasing:** Despite all the runs, **wickets per match also increased** from ~10.4 to ~11.5 — batters are taking more risks, creating more dismissals even as they score more.

![Boundary Trends](10_boundary_trends.png)

The boundaries chart shows the **sixes:fours ratio tipping**. In 2008 there were ~2.5 fours per six. By 2026 the ratio is close to 1:1 — clear evidence of the "aerial game" taking over from traditional bat-on-ball hitting.

## Key Numbers

| Season | Avg 1st Inn | Sixes/Match | Dot % | 4s/6s Ratio |
|--------|------------|-------------|-------|-------------|
| 2008 | 161.0 | 10.7 | 36.5% | ~2.5:1 |
| 2016 | 162.6 | 10.7 | 33.0% | — |
| 2020 | 169.3 | 12.3 | 33.5% | — |
| 2023 | 182.6 | 15.2 | 32.6% | ~1.5:1 |
| 2024 | 189.5 | 17.8 | 31.4% | ~1.2:1 |
| 2026 | **191.7** | **19.3** | 31.1% | ~1.0:1 |

## The Query

```sql
SELECT EXTRACT(YEAR FROM date) AS season,
       COUNT(DISTINCT match_id) AS matches,
       ROUND(AVG(CASE WHEN inning=1 THEN 1 ELSE NULL END *
             (runs_batter + runs_extras)) * 120.0, 1) AS avg_1st_inn,
       ROUND(SUM(CASE WHEN runs_batter=6 THEN 1 ELSE 0 END) * 1.0 /
             COUNT(DISTINCT match_id), 1) AS sixes_per_match
FROM ball_events
GROUP BY season ORDER BY season;
```

## Files

| File | Description |
|------|-------------|
| `10_season_trends.png` | 4-panel: scores, sixes, dot%, wickets by season |
| `10_boundary_trends.png` | Fours vs sixes per match — 18 seasons |
| `output.txt` | Complete season-by-season data table |
