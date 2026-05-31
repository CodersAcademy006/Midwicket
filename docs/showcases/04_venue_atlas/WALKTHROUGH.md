# Showcase 04 — IPL Venue Scoring Atlas

**One sentence:** The VBR metric ranks all 76 IPL venues from most batter-friendly to most bowler-friendly — the New Chandigarh stadium is 25% above average; New Wanderers is 15% below.

---

## The Question

Which IPL venues inflate scores and which suppress them? How much does venue choice matter for team selection?

## The Metric: Venue Bias Rating (VBR)

```
VBR = (Venue Avg 1st Innings Run Rate) / (Global Avg 1st Innings Run Rate)
```

- VBR > 1.0: Batter-friendly (above-average scoring)
- VBR = 1.0: Exactly average (or <5 matches — defaulted for stability)
- VBR < 1.0: Bowler-friendly (below-average scoring)

## The Insight

![Venue Atlas Bar Chart](04_venue_atlas.png)

The spread is dramatic. The top venue (Maharaja Yadavindra Singh Stadium, New Chandigarh) has a VBR of **1.253** — a new ground with small boundaries. The New Wanderers Stadium (used for early IPL seasons in South Africa) sits at **0.848** — 15% below global average.

Wankhede Stadium, arguably the most famous IPL ground, sits comfortably above average at 1.09. The iconic Eden Gardens is notably balanced at near-1.0.

![VBR vs Wickets Scatter](04_vbr_vs_wickets.png)

The VBR vs wickets-per-match scatter shows that **batter-friendly venues don't necessarily produce more wickets** — the relationship is more complex. Some high-VBR venues see bowlers over-compensate with aggression, maintaining wicket rates.

## Key Numbers

| Venue | VBR | Matches | Avg 1st Inn |
|-------|-----|---------|-------------|
| New Chandigarh | 1.253 | 5 | 205 |
| Wankhede | ~1.09 | 59 | ~178 |
| Eden Gardens | ~0.99 | — | ~162 |
| New Wanderers | 0.848 | — | ~139 |

## The Query

```sql
SELECT venue,
       COUNT(DISTINCT match_id) AS matches,
       ROUND(AVG(runs_batter + runs_extras) * 120.0 /
             (SELECT AVG(runs_batter + runs_extras) * 120.0 FROM ball_events WHERE inning = 1),
             3) AS vbr
FROM ball_events
WHERE inning = 1
GROUP BY venue
HAVING COUNT(DISTINCT match_id) >= 5
ORDER BY vbr DESC;
```

## Files

| File | Description |
|------|-------------|
| `04_venue_atlas.png` | Horizontal bar — all 64 qualifying venues ranked by VBR |
| `04_vbr_vs_wickets.png` | VBR vs wickets-per-match scatter |
| `output.txt` | Full venue table |
