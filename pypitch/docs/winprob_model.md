# PyPitch Baseline Win Probability Model

PyPitch ships with a robust, pre-trained win probability model for T20 cricket.

- **Location:** `pypitch.compute.winprob`
- **API:**

```python
from pypitch.compute.winprob import win_probability
prob = win_probability(target=180, current_runs=120, wickets_down=5, overs_done=15.0)
print(prob)  # {'win_prob': 0.42}
```

- **No configuration required.**
- Advanced users can swap in their own models, but a default is always available.

## Model Details
- Logistic regression with domain-informed features (runs remaining, balls left, wickets, run rates).
- Trained on historical T20 data.
