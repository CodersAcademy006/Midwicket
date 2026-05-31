# Showcase 05 — Phase-wise Economy Leaders

**One sentence:** Different bowlers dominate different phases — the economy heatmap shows YS Chahal is cheap in the Middle but expensive in Death, while Bumrah is the only bowler elite in all three.

---

## The Question

Who are the best bowlers in each phase, and which bowlers maintain elite economy across all three phases?

## The Insight

![Phase Economy Three-Panel](05_phase_economy.png)

The three-panel chart reveals **phase specialization**: most bowlers excel in one or two phases but not all three. The Powerplay chart is dominated by fast bowlers (swing and movement); Middle by spinners (containment); Death is the most contested.

![Economy Heatmap — Top 15 Wicket Takers](05_economy_heatmap.png)

The heatmap is the most revealing view: plotted against the **top 15 wicket takers of all time** (who bowled enough to appear in all phases), it shows:

- **Bumrah**: green across all three phases — the only bowler elite everywhere
- **Chahal / Narine / Ashwin**: excellent in Middle, more expensive in Death (expected for spinners)
- **DJ Bravo**: middling economy but consistently the most dangerous Death bowler by wickets
- **Malinga**: extraordinary Powerplay economy despite being a full-time death bowler

## Surprising Finding

Yuzvendra Chahal leads all bowlers by career wickets (233) but his Death economy is among the highest for elite bowlers — teams bet on his ability to take wickets over limiting runs.

## The Query

```sql
SELECT bowler, phase,
       COUNT(*) AS balls,
       ROUND(SUM(runs_batter + runs_extras) * 6.0 / COUNT(*), 2) AS economy,
       SUM(CASE WHEN is_wicket AND wicket_type NOT IN ('RUN_OUT','RETIRED_OUT') THEN 1 ELSE 0 END) AS wickets
FROM ball_events
GROUP BY bowler, phase
HAVING COUNT(*) >= 60
ORDER BY phase, economy ASC;
```

## Files

| File | Description |
|------|-------------|
| `05_phase_economy.png` | Top-7 economy leaders per phase (3-panel) |
| `05_economy_heatmap.png` | Economy heatmap for top-15 wicket takers |
| `output.txt` | Full rankings per phase |
