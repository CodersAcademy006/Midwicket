# Showcase 09 — Dot Ball Kings: Building Pressure Ball by Ball

**One sentence:** Dale Steyn's 44.65% dot ball rate is the highest among all bowlers with 500+ IPL balls — nearly half his deliveries produced zero runs, yet he conceded only 6.79 runs per over.

---

## The Question

Who builds the most pressure through dot balls, and does dot ball % actually correlate with low economy?

## The Insight

![Dot Ball Scatter](09_dot_ball_scatter.png)

The scatter maps **dot ball %** (x-axis) vs **economy** (y-axis). The ideal bowler occupies the **bottom-right**: many dots, few runs. The colour shows bowling strike rate (how often they take wickets).

The top-right corner reveals an important truth: **Steyn, Bollinger, and Morkel** achieve elite dot rates AND elite economy — but they played during an era (2008–2012) when T20 batting was less evolved. The contemporary comparison is more brutal.

The bottom-right quadrant (dot-heavy AND cheap) is **nearly empty** in modern IPL — a structural finding. Today's batters punish length balls that previous generations respected.

**What the data reveals about pressure bowling:**
- **Powerplay**: 44.5% dots — fielding restrictions mean batters are more selective
- **Middle**: 30.8% dots — batters accelerate, hard to build pressure
- **Death**: only 27.0% dots — batters swing at almost everything

![Dot Ball Bar](09_dot_pct_bar.png)

## Surprising Finding

**DW Steyn (economy 6.79, dot% 44.65)** — these numbers would be literally impossible in 2024 IPL. His combined economy + dot% is a statistical artefact of a different era, making it impossible to fairly compare across generations without era-adjustment. Midwicket's temporal filtering lets you do exactly this.

## Key Numbers

| Bowler | Dot Ball % | Economy | Era |
|--------|-----------|---------|-----|
| DW Steyn | 44.65% | 6.79 | 2008–2013 |
| DE Bollinger | 42.33% | 7.16 | 2008–2011 |
| M Morkel | 41.67% | 7.54 | 2008–2018 |
| JJ Bumrah | ~39% | 8.07 | 2013–2026 |
| SP Narine | ~38% | — | 2012–2026 |

## The Query

```sql
SELECT bowler,
       COUNT(*) AS total_balls,
       ROUND(SUM(CASE WHEN runs_batter = 0 AND extras_type IS NULL THEN 1 ELSE 0 END)
             * 100.0 / COUNT(*), 2) AS dot_pct,
       ROUND(SUM(runs_batter + runs_extras) * 6.0 / COUNT(*), 2) AS economy
FROM ball_events
GROUP BY bowler
HAVING COUNT(*) >= 500
ORDER BY dot_pct DESC;
```

## Files

| File | Description |
|------|-------------|
| `09_dot_ball_scatter.png` | Dot% vs economy scatter — career wickets as bubble size |
| `09_dot_pct_bar.png` | Top 20 dot ball % bar chart coloured by economy tier |
| `output.txt` | Full table + phase breakdown |
