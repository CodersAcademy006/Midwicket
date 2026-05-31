# Showcase 02 — Virat Kohli: A 19-Season Scoring Profile

**One sentence:** Track Kohli's strike rate across every over number and every season since 2008 — one query, instant insight.

---

## The Question

How does Kohli score differently in the Powerplay vs Death overs? Which seasons were his best? Has he gotten faster with age?

## The Insight

### Phase Heatmap — Kohli vs Peers

![Phase and Season Chart](02_kohli_phase_season.png)

The phase SR heatmap reveals a striking pattern: **AB de Villiers is the only batter who genuinely out-strikes Kohli at Death**. Kohli's Powerplay SR (above 120) and Middle-overs SR (above 125) are consistently elite across all seasons.

The season bar chart tells an even more surprising story: **2026 is Kohli's best-ever IPL season by strike rate (155.6 SR)** at age 37. His 2016 season (973 runs) remains the highest single-season haul in IPL history.

### Over-by-Over Decomposition

![Kohli by Over Number](02_kohli_by_over.png)

Kohli accelerates **exactly when expected**: he plays conservatively in overs 0-2 (anchor role), peaks in middle overs 6-12, and lifts again in death overs 17-19. The drop in overs 3-5 is a known tactical pattern — he often takes stock after the powerplay.

## Key Numbers

| Season | Runs | SR | Matches |
|--------|------|----|---------|
| 2016 | 973 | 148.5 | 16 |
| 2024 | 741 | 149.1 | 15 |
| 2026 | 557 | **155.6** | 14 |
| 2008 | 165 | 98.2 | 12 |

**19 consecutive IPL seasons.** Career runs: **9,228**. Career matches: **219+**.

## The Query

```sql
-- Phase breakdown comparing top batters
SELECT batter, phase,
       SUM(runs_batter) AS runs,
       COUNT(*) AS balls,
       ROUND(SUM(runs_batter) * 100.0 / COUNT(*), 1) AS sr
FROM ball_events
WHERE batter IN ('V Kohli','RG Sharma','DA Warner','KL Rahul','MS Dhoni','AB de Villiers')
GROUP BY batter, phase
HAVING COUNT(*) >= 30;

-- Season-by-season
SELECT EXTRACT(YEAR FROM date) AS season,
       SUM(runs_batter) AS runs,
       ROUND(SUM(runs_batter) * 100.0 / COUNT(*), 1) AS sr
FROM ball_events
WHERE batter = 'V Kohli'
GROUP BY season ORDER BY season;
```

## Files

| File | Description |
|------|-------------|
| `02_kohli_phase_season.png` | Phase heatmap + season bar chart |
| `02_kohli_by_over.png` | SR by over number (0–19) |
| `output.txt` | Raw data |
