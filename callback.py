import re

is_bytes = isinstance(message, bytes)
if is_bytes:
    msg_str = message.decode('utf-8', errors='ignore')
else:
    msg_str = message

lines = msg_str.splitlines()
new_lines = []
for line in lines:
    lower_line = line.lower()
    if lower_line.startswith('co-authored-by:') and ('claude' in lower_line or 'copilot' in lower_line):
        continue
    new_lines.append(line)

new_msg = '\n'.join(new_lines) + '\n'
if is_bytes:
    return new_msg.encode('utf-8')
return new_msg
