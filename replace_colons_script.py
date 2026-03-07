import re
import sys


def process_file(filepath):
    with open(filepath, "r") as f:
        content = f.read()

    # Keywords to replace
    keywords = [
        "filter",
        "group_by",
        "window",
        "aggregate",
        "accumulate",
        "having",
        "map",
        "rate",
        "flatten",
        "distinct",
        "throttle",
        "sample",
        "deduplicate",
    ]

    for kw in keywords:
        # Match `|> keyword:`
        content = re.sub(r"(\|\>\s*)" + kw + r":", r"\1" + kw, content)

        # Match `  keyword:` at the start of a line
        content = re.sub(r"^(\s*)" + kw + r":", r"\1" + kw, content, flags=re.MULTILINE)

    # Handle `window NAME:`
    # `|> window NAME:`
    content = re.sub(r"(\|\>\s*window\s+[a-zA-Z0-9_]+):", r"\1", content)
    # `  window NAME:`
    content = re.sub(
        r"^(\s*window\s+[a-zA-Z0-9_]+):", r"\1", content, flags=re.MULTILINE
    )

    with open(filepath, "w") as f:
        f.write(content)


process_file("tests/test_grammar.py")
process_file("tests/test_semantics.py")
