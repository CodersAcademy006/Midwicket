# Showcase 03 — Jasprit Bumrah: Death Over Dominance

**One sentence:** Among 141 IPL death-over bowlers with 120+ balls, Bumrah ranks #11 by economy — and his 8.07 economy is elite given he bowls the hardest overs.

---

## The Question

Is Bumrah genuinely the best death bowler in IPL history, or does data tell a more nuanced story?

## The Insight

![Death Bowler Scatter](03_death_bowlers.png)

The scatter maps **economy vs wicket rate** for 141 bowlers with 120+ balls in overs 16–20. The ideal bowler lives in the **bottom-right**: cheap AND wicket-taking. Bumrah sits firmly in the green zone (economy < 8.5), ranked #11 overall — extraordinary given that he bowls to the hottest batters at the peak of their aggression.

What's surprising: **DW Steyn (economy 6.79) and DE Bollinger (7.16) rank above him purely on economy**, but their career death-over sample is smaller. Bumrah has bowled 1,190+ death balls — far more than most elite foreign pace bowlers who played fewer seasons.

![Bumrah Career Evolution](03_bumrah_evolution.png)

The career trend chart reveals his economy **never exceeded 8.50 in any season** — consistency over 12+ seasons that no other death specialist matches. His 2022 season (economy 6.91) stands out as his statistical peak.

## Key Numbers

| Metric | Bumrah | All Bowlers (min 120 death balls) |
|--------|--------|----------------------------------|
| Death Economy | 8.07 | Median 9.1 |
| Death Rank | #11 of 141 | — |
| Wicket % | ~2.8% | Median ~2.3% |
| Career Death Balls | 1,190+ | — |

## The Query

```sql
SELECT
    bowler,
    COUNT(*) AS death_balls,
    ROUND(SUM(runs_batter + runs_extras) * 6.0 / COUNT(*), 2) AS economy,
    SUM(CASE WHEN is_wicket AND wicket_type NOT IN ('RUN_OUT','RETIRED_OUT') THEN 1 ELSE 0 END) AS wickets,
    ROUND(SUM(CASE WHEN runs_batter = 0 AND extras_type IS NULL THEN 1 ELSE 0 END)
          * 100.0 / COUNT(*), 1) AS dot_pct
FROM ball_events
WHERE over >= 15
GROUP BY bowler
HAVING COUNT(*) >= 120
ORDER BY economy ASC;
```

## Files

| File | Description |
|------|-------------|
| `03_death_bowlers.png` | Economy vs wicket rate scatter (141 bowlers) |
| `03_bumrah_evolution.png` | Bumrah economy + wickets by season |
| `output.txt` | Full rankings |
