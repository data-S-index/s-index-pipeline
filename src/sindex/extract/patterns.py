from __future__ import annotations

import re

# URL regex: good general-purpose scanner
URL_RE = re.compile(r"\bhttps?://[^\s<>\"]+", re.IGNORECASE)

# Strip punctuation that commonly sticks to tokens in prose/PDF text
TRAILING_PUNCT = ")]}>,.;:!?\"'"
