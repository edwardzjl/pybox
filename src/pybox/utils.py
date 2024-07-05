import re


def clean_ansi_codes(source: str) -> str:
    """Remove ANSI escape sequences from source."""
    ansi_escape = re.compile(r"(\x9B|\x1B\[|\u001b\[)[0-?]*[ -/]*[@-~]")
    return ansi_escape.sub("", source)
