# Showcase 08 — Wicket Cluster Probability

**One sentence:** Lungi Ngidi takes 3+ wickets in 25.9% of his IPL appearances — the highest of any bowler with 20+ matches, built on short explosive spells.

---

## The Question

Which bowlers are most likely to deliver a multi-wicket haul that collapses an innings? Fantasy analysts call this the "ceiling" metric.

## The Metric

```
P(3+ wickets) = appearances with 3+ wickets / total appearances × 100
```

This rewards **ceiling performance** — not just career wickets, but how often a bowler completely changes a match.

## The Insight

![Wicket Cluster Probability](08_wicket_clusters.png)

The grouped bar chart shows three thresholds simultaneously: P(2+), P(3+), P(4+). The hierarchy is clear:

1. **L Ngidi (25.9%)**: small sample but freakish cluster rate
2. **MA Starc (21.4%)**: arguably the most feared new-ball bowler in T20 history
3. **JO Holder (20.4%)**: underrated all-rounder with genuine haul potential
4. **Imran Tahir (20.3%)**: the spin outlier — most top-cluster bowlers are pace

The surprise: **Rashid Khan** (177 career IPL wickets) has a lower cluster rate than Starc or Holder — consistent but rarely explosive. **K Rabada** (146 wickets) has the biggest sample of elite foreign pace and still maintains 17.3%.

![Cluster Scatter](08_cluster_scatter.png)

The scatter plots **career wickets (bubble) against avg wickets per appearance**, coloured by P(3+). It reveals a key tension: high-volume wicket takers are not always the best cluster bowlers.

## Key Numbers

| Bowler | Appearances | Career Wickets | P(3+ wickets) |
|--------|------------|----------------|---------------|
| L Ngidi | 27 | 42 | 25.9% |
| MA Starc | 56 | 76 | 21.4% |
| JO Holder | 54 | 66 | 20.4% |
| Imran Tahir | 59 | 82 | 20.3% |
| K Rabada | 98 | 146 | 17.3% |
| JJ Bumrah | — | 190 | — |

## The Query

```sql
WITH per_match AS (
    SELECT bowler, match_id,
           SUM(CASE WHEN is_wicket AND wicket_type NOT IN ('RUN_OUT','RETIRED_OUT')
                    THEN 1 ELSE 0 END) AS match_wickets
    FROM ball_events GROUP BY bowler, match_id
)
SELECT bowler,
       COUNT(*) AS appearances,
       SUM(match_wickets) AS total_wickets,
       ROUND(SUM(CASE WHEN match_wickets >= 3 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_3plus
FROM per_match
GROUP BY bowler
HAVING appearances >= 20 AND total_wickets >= 30
ORDER BY pct_3plus DESC;
```

## Files

| File | Description |
|------|-------------|
| `08_wicket_clusters.png` | Multi-wicket probability grouped bar chart |
| `08_cluster_scatter.png` | Career wickets vs cluster rate scatter |
| `output.txt` | Full table + best single-match hauls |
