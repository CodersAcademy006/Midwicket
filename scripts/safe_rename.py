import os
import re

def process_file(filepath):
    if 'safe_rename.py' in filepath:
        return
        
    with open(filepath, 'r') as f:
        content = f.read()
    
    new_content = content.replace('pypitch', 'midwicket')
    new_content = new_content.replace('PyPitch', 'Midwicket')
    new_content = new_content.replace('PYPITCH', 'MIDWICKET')
    
    new_content = new_content.replace('import midwicket as pp', 'import midwicket as md')
    new_content = re.sub(r'\bpp\.', 'md.', new_content)
    
    if content != new_content:
        with open(filepath, 'w') as f:
            f.write(new_content)
        print(f"Updated {filepath}")

for root, dirs, files in os.walk('.'):
    if '.venv' in dirs: dirs.remove('.venv')
    if '.git' in dirs: dirs.remove('.git')
    # Do NOT remove midwicket dir! We want to rename things inside midwicket!
    
    for file in files:
        if file.endswith('.py') or file.endswith('.md') or file.endswith('.txt') or file.endswith('.ini') or file.endswith('.toml'):
            process_file(os.path.join(root, file))
