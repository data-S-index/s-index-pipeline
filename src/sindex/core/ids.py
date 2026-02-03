import re
from typing import Any, Optional
from urllib.parse import unquote, urlparse

import requests

from sindex.core.http import _is_reachable

_DOI_PATTERN = re.compile(
    r"""
    (?P<doi>                                  # capture group "doi"
        10\.\d{4,9}                           # directory indicator: 10.<4-9 digits>
        /                                     # slash
        [^\s"'<>\]]+                          # suffix: anything except whitespace/quotes/brackets
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)

_EMDB_PATTERN = re.compile(r"EMD-\d{4,5}", re.IGNORECASE)


def _norm_doi(s: str) -> str:
    """
    Normalize a DOI-like string to its lowercase canonical identifier form.

    This function is robust to various prefixes and noisy inputs, including:
      - Plain identifiers: "10.1000/ABC.DEF"
      - DOI URLs: "https://doi.org/10.1000/ABC.DEF", "http://dx.doi.org/10.1000/xyz"
      - Prefixed values: "doi:10.1000/xyz", "DOI 10.1000/xyz"
      - Text blobs containing a DOI somewhere inside:
            "see https://dx.doi.org/10.1000/foo-bar for details"
      - URL-encoded DOIs (e.g. "10.1000/foo%2Fbar") using `unquote()`.

    Args:
        s:
            Raw DOI string, DOI URL, or an arbitrary string that may contain
            a DOI.

    Returns:
        A normalized DOI identifier in lowercase (e.g. "10.1000/abc.def"),
        or an empty string if no plausible DOI can be detected.

    Examples:
        _norm_doi("10.1000/ABC.DEF") -> "10.1000/abc.def"
        _norm_doi("HTTPS://doi.org/10.1000/XYZ") -> "10.1000/xyz"
        _norm_doi("doi:10.1000/Foo-Bar") -> "10.1000/foo-bar"
        _norm_doi("see https://dx.doi.org/10.1000/XYZ?param=1") -> "10.1000/xyz"
    """
    if not isinstance(s, str):
        return ""

    # Strip whitespace and decode any %-escapes first (handles %2F etc.)
    s_clean = unquote(s.strip())
    if not s_clean:
        return ""

    # Work in lowercase for stable matching
    s_lower = s_clean.lower()

    # Search for a DOI substring anywhere in the string
    m = _DOI_PATTERN.search(s_lower)
    if not m:
        return ""

    doi = m.group("doi").strip()
    # Sanity check: all DOI identifiers must start with "10."
    if not doi.startswith("10."):
        return ""

    return doi


def _norm_doi_url(s: str) -> str:
    """
    Normalize a DOI-like string to its canonical https://doi.org/<doi_id> URL form.

    This function uses `_norm_doi()` to extract and normalize the DOI identifier and
    then prefixes it with "https://doi.org/". It is useful when the full URL DOI is needed.

    Args:
        s: Raw DOI string, DOI URL, or text containing a DOI.

    Returns:
        Canonical DOI URL string (e.g. "https://doi.org/10.1000/abc.def"),
        or an empty string if the input cannot be normalized to a DOI.
    """
    doi_id = _norm_doi(s)
    if not doi_id:
        return ""
    return f"https://doi.org/{doi_id}"


def _norm_emdb_id(s: str) -> str:
    """
    Normalize an EMDB ID-like string to its uppercase canonical form.

    This function is robust to various noisy inputs, including:
      - Plain identifiers: "EMD-1234"
      - Lowercase or mixed case: "emd-1234", "Emd-1234"
      - URL paths: "https://www.ebi.ac.uk/emdb/entry/EMD-1234/"
      - Text blobs: "The structure was deposited as EMD-1234."
      - URL-encoded strings.

    Args:
        s: Raw string that may contain an EMDB ID.

    Returns:
        A normalized EMDB ID in uppercase (e.g., "EMD-1234"),
        or an empty string if no valid ID is detected.
    """
    if not isinstance(s, str):
        return ""

    # Strip whitespace and decode any %-escapes
    s_clean = unquote(s.strip())
    if not s_clean:
        return ""

    # Strict EMDB ID detection: "EMD-" followed by 1 to 6 digits.
    # We use \b for word boundaries to ensure we don't catch "NOT-EMD-1234"
    # or part of a longer serial number.
    m = re.search(r"\b(EMD-\d{1,6})\b", s_clean, flags=re.IGNORECASE)

    if m:
        # Standardize to uppercase (e.g., EMD-1234)
        return m.group(1).upper()

    return ""


def _norm_dataset_id(raw: Any) -> str:
    """
    Normalize a dataset identifier (DOI, EMDB ID, or URL) into a canonical form.

    This function converts different identifier representations into a unified
    canonical string suitable for comparison. It supports:

      • DOIs (bare or URL form) — normalized using `_norm_doi()`
      • EMDB IDs (e.g., "EMD-1234" or URLs containing them)
      • Any other identifier types (e.g., SRA, internal IDs) via a lowercase fallback

    Normalization rules:
      1. If the input is DOI-like, return the normalized DOI string.
      2. If the input contains a valid EMDB identifier, return "EMD-####".
      3. Otherwise, return back the lowercased trimmed string.

    Args:
        raw: The raw input identifier string or other object.

    Returns:
        A canonical normalized identifier string
    """
    if not isinstance(raw, str):
        return ""
    raw = raw.strip()
    if not raw:
        return ""

    # 1. DOI normalization
    doi = _norm_doi(raw)
    if doi:
        return doi

    # 2. Strict EMDB ID detection
    emdb_id = _norm_emdb_id(raw)
    if emdb_id:
        return emdb_id

    # 3. Fallback for other identifiers (SRA, internal IDs, etc.)
    return raw.lower()


def _normalize_orcid(orcid: str) -> str:
    """
    Normalize an ORCID into the canonical URL format.

    Accepts:
        - raw ORCID (####-####-####-####)
        - full URL
        - orcid.org/####-####-####-####
        - variants with unicode hyphens

    Returns:
        'https://orcid.org/<id>'  or None if it cannot be parsed.
    """
    if not orcid:
        return None

    # Remove protocol + domain if present
    orcid = orcid.strip()
    orcid = re.sub(r"^https?://orcid\.org/", "", orcid)
    orcid = re.sub(r"^orcid\.org/", "", orcid)

    # Normalize unicode hyphens
    orcid = orcid.replace("-", "-").replace("–", "-").replace("—", "-")

    # Extract canonical ORCID ID (16 digits with hyphens)
    m = re.match(r"^(\d{4}-\d{4}-\d{4}-\d{4})$", orcid)
    if not m:
        return None

    return f"https://orcid.org/{m.group(1)}"


def is_working_doi(
    s: str,
    session: requests.Session | None = None,
    *,
    timeout: float = 10.0,
    allow_blocked: bool = True,
) -> bool:
    """
    Return True if the DOI URL is reachable enough to be considered resolving.

    allow_blocked=True treats 401/403 as "working but blocked" (common with bot protections).
    """
    doi_url = _norm_doi_url(s)
    if not doi_url:
        return False
    return _is_reachable(doi_url, session, timeout=timeout, allow_blocked=allow_blocked)


def is_working_url(s: str, session: requests.Session | None = None) -> bool:
    """
    Checks if the string is a valid URL (not a DOI) and if it resolves.
    """
    # If it's a DOI, we ignore it here to keep functions distinct
    if _norm_doi(s):
        return False

    parsed = urlparse(s)
    if not all([parsed.scheme, parsed.netloc]):
        return False

    return _is_reachable(s, session)


def norm_dataset_id_db(x: Optional[str]) -> Optional[str]:
    """
    DB-safe wrapper around _norm_dataset_id.
    Never raises, returns None on failure.
    """
    if not x:
        return None
    y = _norm_dataset_id(x)
    return y if y else None


def norm_doi_url_or_raw(x: Optional[str]) -> Optional[str]:
    """
    Normalize DOI URL when possible; otherwise return raw string.
    """
    if not x:
        return None
    y = _norm_doi_url(x)
    return y or x
