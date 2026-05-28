import re

with open('midwicket/models/train.py', 'r') as f:
    content = f.read()

old_code = """        # Prepare data
        features, target = self.prepare_training_data(match_data)"""

new_code = """        # Prepare data
        if "runs_total" not in match_data.columns:
            features, target, _ = self.prepare_training_dataset(match_data)
        else:
            features, target = self.prepare_training_data(match_data)"""

content = content.replace(old_code, new_code)

with open('midwicket/models/train.py', 'w') as f:
    f.write(content)
