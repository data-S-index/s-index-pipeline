from __future__ import annotations

import os

DEFAULT_USER_AGENT = "bvh (mailto:bvhpatel@gmail.com)"


def get_user_agent() -> str:
    return os.getenv("SINDEX_USER_AGENT", DEFAULT_USER_AGENT)
