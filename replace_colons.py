import re
import sys

def process_file(filepath):
    with open(filepath, 'r') as f:
        content = f.read()

    keywords = [
        'filter', 'group_by', 'aggregate', 'accumulate', 'having', 'map',
        'rate', 'flatten', 'distinct', 'throttle', 'sample', 'deduplicate'
    ]
    
    for kw in keywords:
        pattern = r'\b' + kw + r'\s*:'
        content = re.sub(pattern, kw, content)

    window_pattern = r'\bwindow\s+([a-zA-Z0-9_]+)\s*:'
    content = re.sub(window_pattern, r'window \1', content)

    window_anon_pattern = r'\bwindow\s*:'
    content = re.sub(window_anon_pattern, 'window', content)

    with open(filepath, 'w') as f:
        f.write(content)

if __name__ == "__main__":
    for arg in sys.argv[1:]:
        process_file(arg)
