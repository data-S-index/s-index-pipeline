from datetime import date, datetime, timezone
from typing import Optional

from dateutil import parser


def _parse_date_strict(iso_str: str) -> date:
    """
    Strictly parse a `YYYY-MM-DD` ISO calendar date string.

    This function is used when querying the DataCite API for records between two given dates.
    It makes sure the date is formatted correctly and is a valid date.

    Args:
        iso_str: Input date string in YYYY-MM-DD format.

    Returns:
        Python `date` object representing the parsed calendar date.

    Raises:
        ValueError: If the input string is not properly formatted or is not a valid date.
    """
    try:
        y, m, d = map(int, iso_str.split("-"))
        return date(y, m, d)
    except Exception as e:
        raise ValueError(
            f"Invalid date '{iso_str}' — must be YYYY-MM-DD and a real calendar date."
        ) from e


def _norm_date_iso(s: str) -> str:
    """
    Normalize a date string into a canonical ISO-8601 format.

    This function is needed to normalize dates.
    At minimum the string must contain a 4-digit year. If the input cannot be
    parsed or does not contain a valid year, a ValueError is raised.

    Examples:
        _norm_date_iso("2025") -> "2025-01-01T00:00:00"
        _norm_date_iso("2025-04") -> "2025-04-01T00:00:00"
        _norm_date_iso("2025/04/04") -> "2025-04-04T00:00:00"
        _norm_date_iso("2025-04-04T13:22:55Z") -> "2025-04-04T13:22:55+00:00"

    Args:
        s: A date string (must include at least a 4-digit year).

    Returns:
        Canonical ISO-8601 formatted date string.

    Raises:
        ValueError: If the input date cannot be parsed or no 4-digit year is found.
    """
    default_dt = datetime(1, 1, 1, 0, 0, 0)

    if not isinstance(s, str):
        raise ValueError("Date must be a string.")
    s = s.strip()
    if not s:
        raise ValueError("Date input is empty.")

    try:
        dt = parser.parse(s, default=default_dt)
    except Exception as e:
        raise ValueError(f"Invalid date format: {s}") from e

    if dt.year is None or dt.year < 1 or len(s) < 4:
        raise ValueError(f"Date must contain a valid 4-digit year: {s}")

    return dt.isoformat()


def _to_datetime_utc(s: Optional[str]) -> Optional[datetime]:
    """
    Normalize a date string via `_norm_date_iso` and return a timezone-aware UTC datetime.

    This is needed for instance to calculate the amount of time elapsed between two dates.

    Args:
        s: Any reasonable date string (must contain at least a 4-digit year) or None.

    Returns:
        A timezone-aware `datetime` in UTC if parsing succeeds, else None.
    """
    if not s:
        return None
    try:
        iso = _norm_date_iso(
            s
        )  # raises ValueError if completely invalid / missing year
    except ValueError:
        return None
    # Support both "...Z" and "...+00:00"
    iso = iso.replace("Z", "+00:00")
    dt = datetime.fromisoformat(iso)
    # If no tzinfo, set UTC; else convert to UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def _years_between(start_dt: datetime, end_dt: datetime) -> float:
    """
    Compute the fractional number of years between two UTC datetimes.

    The result is constrained to be non-negative. If `end_dt` is earlier than
    `start_dt`, the return value is `0.0`. This prevents negative contribution
    to weighting (e.g., citations that appear to pre-date the dataset publicatio for some reason).

    Year length approximation: 365.25 days/year (to include leap-year average)

    Args:
        start_dt: Dataset creation datetime (UTC).
        end_dt: Citation publication datetime (UTC).

    Returns:
        Fractional years difference (float >= 0.0).
    """
    seconds = (end_dt - start_dt).total_seconds()
    years = seconds / (365.25 * 24 * 3600.0)
    return years if years > 0 else 0.0


def _as_iso_from_dateparts(parts) -> str:
    """
    Convert a Crossref-style date-parts array to an ISO-8601 string.

    Crossref formats date parts as: [[YYYY, M, D]] or [[YYYY, M]] or [[YYYY]].

    Args:
        parts: Crossref 'date-parts' value (e.g., [[2024, 10, 31]]).

    Returns:
        ISO-8601 string like "2024-10-31T00:00:00" if possible, else "".
    """
    try:
        if not parts or not parts[0]:
            return ""
        nums = parts[0]
        if len(nums) >= 3:
            return _norm_date_iso(f"{nums[0]:04d}-{nums[1]:02d}-{nums[2]:02d}")
        if len(nums) == 2:
            return _norm_date_iso(f"{nums[0]:04d}-{nums[1]:02d}-01")
        if len(nums) == 1:
            return _norm_date_iso(f"{nums[0]:04d}-01-01")
    except Exception:
        pass
    return ""


def norm_date_iso_db(x: Optional[str]) -> Optional[str]:
    """
    Safe ISO date normalization for database ingestion.
    """
    if not x:
        return None
    try:
        y = _norm_date_iso(x)
        return y if y else None
    except Exception:
        return None


def _dt_utc_or_today(s: Optional[str], *, today_dt: datetime) -> datetime:
    """
    if missing/invalid -> today_dt (00:00 UTC).
    """
    dt = _to_datetime_utc(s)
    return dt if dt is not None else today_dt


def get_realistic_date(date_str: str | None, start_year: int = 1950) -> str | None:
    """
    Checks if an ISO date string is between Jan 1st of start_year and today.
    Ignores time components during comparison.
    Returns the original string if realistic, otherwise None.
    """
    if not date_str:
        return None

    try:
        # fromisoformat handles '2024-10-10T21:27:19+00:00'
        dt_obj = datetime.fromisoformat(
            date_str
        ).date()  # .date() strips the time and timezone for pure date comparison

        start_bound = date(start_year, 1, 1)
        end_bound = date.today()

        if start_bound <= dt_obj <= end_bound:
            return date_str

    except (ValueError, TypeError):
        # Returns None if the string is malformed or not a date
        pass

    return None


def get_best_dataset_date(
    publication_date: str | None,
    created_date: str | None,
) -> str | None:
    """
    Returns publication_date if realistic, otherwise created_date if realistic,
    otherwise None.
    """
    start_year = 1950
    # Try publication date
    realistic_pub = get_realistic_date(publication_date, start_year)
    if realistic_pub:
        return realistic_pub

    # Fallback to  created date
    realistic_created = get_realistic_date(created_date, start_year)
    if realistic_created:
        return realistic_created

    # No realistic dates found
    return None
