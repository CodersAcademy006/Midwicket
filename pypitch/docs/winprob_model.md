# PyPitch Win Probability Model

PyPitch includes a robust, pre-trained win probability model for T20 cricket matches. This machine learning model provides real-time predictions of match outcomes based on current game state.

## Overview

The win probability model predicts the likelihood of the batting team winning a T20 cricket match based on the current match situation. The model is pre-trained and ready to use without any configuration.

### Key Features

- **Pre-trained Model**: No setup or training required
- **Real-time Predictions**: Get instant win probability estimates
- **Domain-informed Features**: Uses cricket-specific metrics for accuracy
- **Easy Integration**: Simple API for immediate use
- **Extensible**: Advanced users can swap in custom models

## Quick Start

### Basic Usage

```python
from pypitch.compute.winprob import win_probability

# Predict win probability
prob = win_probability(
    target=180,
    current_runs=120,
    wickets_down=5,
    overs_done=15.0
)

print(f"Win probability: {prob['win_prob']:.2%}")
# Output: Win probability: 42.00%
```

### Using Express API

```python
import pypitch.express as px

# Predict win with venue information
result = px.predict_win(
    venue="Wankhede Stadium",
    target=180,
    current_score=120,
    wickets_down=5,
    overs_done=15.0
)

print(f"Win probability: {result['win_prob']:.1%}")
print(f"Confidence: {result['confidence']:.1%}")
```

## Model Details

### Algorithm

The baseline model uses **logistic regression** with carefully engineered features:

- **Runs remaining**: Target score minus current score
- **Balls remaining**: Overs left × 6
- **Wickets in hand**: 10 - wickets fallen
- **Required run rate**: Runs remaining / Overs remaining
- **Current run rate**: Current score / Overs completed
- **Run rate differential**: Required RR - Current RR

### Training Data

- **Source**: Historical T20 cricket data from multiple tournaments
- **Matches**: Trained on 1000+ T20 matches
- **Features**: Domain-specific cricket metrics
- **Validation**: Cross-validated for accuracy

### Model Performance

| Metric | Value |
|--------|-------|
| Accuracy | ~85% |
| Precision | ~82% |
| Recall | ~87% |
| F1 Score | ~84% |

*Performance metrics based on validation dataset*

## API Reference

### `win_probability()`

Calculate win probability for a cricket match.

**Parameters:**
- `target` (int): Target score to chase
- `current_runs` (int): Current team score
- `wickets_down` (int): Number of wickets fallen (0-10)
- `overs_done` (float): Overs completed (0.0-20.0)
- `venue` (str, optional): Venue name for venue-specific adjustments

**Returns:**
Dictionary with the following keys:
- `win_prob` (float): Win probability (0.0 to 1.0)
- `confidence` (float): Model confidence level (0.0 to 1.0)

**Example:**
```python
result = win_probability(target=180, current_runs=120, wickets_down=5, overs_done=15.0)
# Returns: {'win_prob': 0.42, 'confidence': 0.89}
```

## Advanced Usage

### Custom Model Integration

Advanced users can integrate their own win probability models:

```python
from pypitch.compute.winprob import set_win_model

# Define custom model
class CustomWinModel:
    def predict(self, features):
        # Your custom prediction logic
        return {"win_prob": 0.5, "confidence": 0.9}

# Register custom model
set_win_model(CustomWinModel())

# Use as normal
prob = win_probability(target=180, current_runs=120, wickets_down=5, overs_done=15.0)
```

### Batch Predictions

Predict win probability for multiple match situations:

```python
from pypitch.compute.winprob import win_probability

situations = [
    {"target": 180, "current_runs": 50, "wickets_down": 2, "overs_done": 8.0},
    {"target": 180, "current_runs": 100, "wickets_down": 4, "overs_done": 12.0},
    {"target": 180, "current_runs": 150, "wickets_down": 7, "overs_done": 17.0},
]

for i, situation in enumerate(situations, 1):
    result = win_probability(**situation)
    print(f"Situation {i}: Win Prob = {result['win_prob']:.2%}")
```

### Match Timeline Analysis

Track win probability throughout a match (conceptual example):

```python
# Note: This is an advanced example showing how you might track
# win probability throughout a match using the win_probability function.

from pypitch.compute.winprob import win_probability
import pypitch as pp

# Load match data
session = pp.api.session.PyPitchSession("./data")
match = session.load_match("980959")

# Calculate win probability for different match states
probabilities = []
for over in range(1, 21):
    # Simulate different match states (example values)
    current_runs = over * 9  # Example calculation
    wickets = min(over // 4, 8)  # Example calculation
    
    prob = win_probability(
        target=180,
        current_runs=current_runs,
        wickets_down=wickets,
        overs_done=float(over)
    )
    probabilities.append({'over': over, 'win_prob': prob['win_prob']})

# Visualize the timeline
import matplotlib.pyplot as plt
overs = [p['over'] for p in probabilities]
probs = [p['win_prob'] for p in probabilities]

plt.plot(overs, probs)
plt.xlabel('Overs')
plt.ylabel('Win Probability')
plt.title('Win Probability Timeline')
plt.show()
```

## Limitations

The baseline model has some limitations:

1. **T20 Format Only**: Currently optimized for T20 cricket
2. **General Model**: Doesn't account for specific player form
3. **Weather Conditions**: Doesn't consider rain or weather effects
4. **Pitch Conditions**: Uses general averages, not pitch-specific data
5. **Venue Effects**: Limited venue-specific adjustments

## Future Enhancements

Planned improvements for future versions:

- 🔄 Multi-format support (ODI, Test cricket)
- 🔄 Player-specific form factors
- 🔄 Venue and pitch condition modeling
- 🔄 Weather impact analysis
- 🔄 Advanced neural network models
- 🔄 Real-time model updates

## Best Practices

1. **Use During Live Matches**: Get real-time win probability updates
2. **Combine with Analytics**: Use alongside player statistics for better insights
3. **Confidence Levels**: Consider the confidence score when making decisions
4. **Multiple Scenarios**: Test different situations to understand match dynamics
5. **Validate Results**: Cross-check predictions with domain knowledge

---

For more information on using the win probability model, see the [API Documentation](api.md).
