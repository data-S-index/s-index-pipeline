from __future__ import annotations

from typing import Optional

import requests
from requests.auth import HTTPBasicAuth

from .constants import FUJI_EVALUATE_PATH, FUJI_TIMEOUT_SECS


def fuji_evaluate(
    object_identifier: str,
    *,
    session: requests.Session,
    base_url: str,
    username: str | None = None,
    password: str | None = None,
) -> dict:
    """
    Pure F-UJI client call. Requires a session.
    object_identifier should be a resolvable URL (including DOI URL).
    """
    evaluate_url = f"{base_url.rstrip('/')}{FUJI_EVALUATE_PATH}"

    auth: Optional[HTTPBasicAuth] = None
    if username is not None and password is not None:
        auth = HTTPBasicAuth(username, password)

    resp = session.post(
        evaluate_url,
        json={"object_identifier": object_identifier},
        auth=auth,
        timeout=FUJI_TIMEOUT_SECS,
    )
    resp.raise_for_status()
    return resp.json()
