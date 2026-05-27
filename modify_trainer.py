import re

with open('midwicket/models/train.py', 'r') as f:
    content = f.read()

# 1. Update imports
imports = """import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression, SGDClassifier
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import accuracy_score, log_loss, roc_auc_score
from sklearn.preprocessing import StandardScaler
from typing import Any, Dict, Optional, Tuple, List
import logging
from datetime import datetime
import joblib
import copy
import os"""

content = re.sub(
    r'import pandas as pd.*?from datetime import datetime',
    imports,
    content,
    flags=re.DOTALL
)

# 2. Update train_model
old_train = """    def train_model(self, features: pd.DataFrame, target: pd.Series,
                   test_size: float = 0.2, random_state: int = 42,
                   match_ids: Optional[List[str]] = None) -> Tuple[LogisticRegression, Dict[str, Any]]:"""

new_train = """    def train_model(self, features: pd.DataFrame, target: pd.Series,
                   test_size: float = 0.2, random_state: int = 42,
                   match_ids: Optional[List[str]] = None) -> Tuple[Any, Dict[str, Any]]:"""

content = content.replace(old_train, new_train)

# Now we need to replace the part that trains the model
old_training_logic = """        # Train model
        model = LogisticRegression(random_state=random_state, max_iter=1000)
        model.fit(X_train_scaled, y_train)

        # Evaluate
        train_pred = model.predict_proba(X_train_scaled)[:, 1]
        test_pred = model.predict_proba(X_test_scaled)[:, 1]"""

new_training_logic = """        # Grid search / hyperparameter tuning and Epochs training
        best_loss = float('inf')
        best_model = None
        best_metrics = None
        best_epochs = 50
        
        # Test a couple of params (alphas)
        alphas = [0.0001, 0.001, 0.01]
        
        for alpha in alphas:
            model = SGDClassifier(loss='log_loss', penalty='l2', alpha=alpha,
                                  random_state=random_state, max_iter=1,
                                  warm_start=True, learning_rate='optimal')
            
            for epoch in range(1, best_epochs + 1):
                model.partial_fit(X_train_scaled, y_train, classes=np.array([0, 1]))
                
                # Evaluate
                try:
                    val_pred = model.predict_proba(X_test_scaled)[:, 1]
                    val_loss = log_loss(y_test, val_pred, labels=[0, 1])
                except Exception:
                    val_loss = float('inf')
                
                # Checkpoints
                os.makedirs('checkpoints', exist_ok=True)
                joblib.dump(model, f'checkpoints/last_checkpoint.pkl')
                
                if val_loss < best_loss:
                    best_loss = val_loss
                    best_model = copy.deepcopy(model)
                    joblib.dump(best_model, f'checkpoints/best_model.pkl')
                    
        # Use the best model
        model = best_model
        
        # Evaluate final
        train_pred = model.predict_proba(X_train_scaled)[:, 1]
        test_pred = model.predict_proba(X_test_scaled)[:, 1]"""

content = content.replace(old_training_logic, new_training_logic)

# Replace the type hint for trained_model in create_win_predictor
old_create = """    def create_win_predictor(self, trained_model: LogisticRegression,"""
new_create = """    def create_win_predictor(self, trained_model: Any,"""
content = content.replace(old_create, new_create)

with open('midwicket/models/train.py', 'w') as f:
    f.write(content)
