import pandas as pd
import numpy as np
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import accuracy_score, roc_auc_score

import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.retrain_model import ensure_data, parse_all, add_target, learn_venue_adjustments, build_features

files = ensure_data()
df = parse_all(files)
df = add_target(df)
venue_adj = learn_venue_adjustments(df)
X, y, match_ids = build_features(df, venue_adj)

unique_ids = list(dict.fromkeys(match_ids))
rng = np.random.RandomState(42)
rng.shuffle(unique_ids)
n_test = max(1, int(len(unique_ids) * 0.2))
test_ids = set(unique_ids[:n_test])
mask = pd.Series([m not in test_ids for m in match_ids])

X_tr, X_te = X[mask.values], X[~mask.values]
y_tr, y_te = y[mask.values], y[~mask.values]

model = HistGradientBoostingClassifier(random_state=42, max_iter=200, learning_rate=0.1)
model.fit(X_tr, y_tr)
te_pred = model.predict_proba(X_te)[:, 1]

print("HGB AUC:", roc_auc_score(y_te, te_pred))
print("HGB Acc:", accuracy_score(y_te, te_pred > 0.5))
