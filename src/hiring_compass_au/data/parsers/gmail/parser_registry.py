from hiring_compass_au.data.parsers.gmail.seek_mail_parser import parse_seek_email
from collections.abc import Iterator


PARSER_CONFIGS = {
  "jobmail@s.seek.com.au": {
     "source": "seek",
     "parser_name": "seek_mail_parser",
     "parser_version": "v1",
     "fn": parse_seek_email,
     "hits_expected": (12, 20),
  },
}

def parse_email(from_email, html_raw) -> tuple[Iterator[dict] | None, dict | None]:
    """
    Dispatch to a parser based on email metadata.
    Option 1 / commit 2: registry is intentionally empty (no real parsers yet).
    Returns an empty iterator when no parser is registered.
    """
    parser_cfg = PARSER_CONFIGS.get(from_email)
    if not parser_cfg:
        return None, None
    
    it = parser_cfg["fn"](html_raw)
    return it, parser_cfg
