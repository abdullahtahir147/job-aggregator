"""
Shared utilities: CSV loading, YAML loading, timestamps, logging setup,
thread-safe HTTP sessions.
"""

from __future__ import annotations

import csv
import logging
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
import yaml


# ── Thread-local HTTP sessions ───────────────────────────────────────────

_thread_local = threading.local()

SESSION_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "job-aggregator/1.2",
}


def get_thread_session() -> requests.Session:
    """
    Return a requests.Session scoped to the current thread.

    Each thread gets its own Session (avoiding the documented thread-safety
    issues with requests.Session).  Sessions are reused within a thread for
    connection pooling.
    """
    if not hasattr(_thread_local, "session"):
        s = requests.Session()
        s.headers.update(SESSION_HEADERS)
        _thread_local.session = s
    return _thread_local.session


def setup_logging(verbose: bool = False) -> logging.Logger:
    """Configure and return the root application logger."""
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
    logging.basicConfig(level=level, format=fmt, stream=sys.stderr)
    return logging.getLogger("job_aggregator")


def now_iso() -> str:
    """Current UTC time as ISO-8601 string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_iso_days(iso_str: str | None) -> int | None:
    """Parse an ISO-8601 string and return days since that date, or None."""
    if not iso_str:
        return None
    try:
        clean = iso_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(clean)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        delta = datetime.now(timezone.utc) - dt
        return max(delta.days, 0)
    except (ValueError, TypeError):
        return None


def compute_job_age_days(posted_at: str | None, first_seen: str | None = None) -> int | None:
    """
    Return integer days since the job was posted.

    Prefers *first_seen* (our own DB timestamp) when available, because
    some ATS systems refresh posted_at on edits, making stale jobs look
    fresh.  Falls back to *posted_at* if first_seen is not set.

    Returns None when neither date is usable.
    """
    # Prefer first_seen — our own reliable timestamp
    fs_days = _parse_iso_days(first_seen)
    if fs_days is not None:
        return fs_days
    return _parse_iso_days(posted_at)


import re as _re

# ── Experience / seniority filter ─────────────────────────────────────────

_YEARS_RE = _re.compile(
    r"(?:minimum\s+of\s+|at\s+least\s+)?(\d{1,2})\s*[\+\-]?\s*(?:to\s+\d{1,2}\s+)?years?",
    _re.IGNORECASE,
)

_SENIOR_TITLE_EXCLUDE = {"director", "vp", "vice president", "head of", "principal"}


def extract_years_required(title: str, description: str | None) -> int | None:
    """
    Return the lower-bound years of experience required, or None.

    Scans title first, then the first 1500 chars of description.
    """
    for text in (title, (description or "")[:1500]):
        m = _YEARS_RE.search(text)
        if m:
            return int(m.group(1))
    return None


def is_too_senior_title(title: str) -> bool:
    """Return True if the title contains director/vp/head-of/principal."""
    t = title.lower()
    return any(term in t for term in _SENIOR_TITLE_EXCLUDE)


def load_companies(path: str | Path) -> list[dict[str, str]]:
    """
    Load companies from a CSV file.

    Returns a list of dicts with at least:
        company, sector, uk_base, ats_hint, careers_url, notes
    Plus optional override columns:
        ats_type_override, ats_slug_override
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Companies CSV not found: {path}")

    companies: list[dict[str, str]] = []
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            # Strip whitespace from keys and values
            cleaned = {k.strip(): v.strip() for k, v in row.items() if k}
            # Skip empty rows
            if not cleaned.get("company"):
                continue
            companies.append(cleaned)

    return companies


def load_all_companies(paths: list[str | Path]) -> list[dict[str, str]]:
    """
    Load and merge companies from multiple CSV files.

    Deduplicates by careers_url (first occurrence wins, preserving overrides).
    Silently skips CSVs that don't exist.
    """
    seen_urls: set[str] = set()
    merged: list[dict[str, str]] = []

    for p in paths:
        p = Path(p)
        if not p.exists():
            continue
        rows = load_companies(p)
        for row in rows:
            url = row.get("careers_url", "").strip().rstrip("/").lower()
            if url and url in seen_urls:
                continue
            if url:
                seen_urls.add(url)
            merged.append(row)

    if not merged:
        raise FileNotFoundError(f"No companies found in any CSV: {paths}")

    return merged


# ── UK location filter ────────────────────────────────────────────────────

_UK_INCLUDE = [
    "london", "united kingdom", "uk", "remote uk", "remote (uk)",
    "hybrid uk", "cardiff", "manchester", "edinburgh",
]

_UK_EXCLUDE = [
    "india", "united states", "usa", "new york", "san francisco",
    "canada", "berlin", "singapore", "sydney",
]


def is_uk_role(location: str | None) -> bool:
    """Return True if *location* looks like a UK-based role."""
    if not location:
        return False
    loc = location.lower()
    # Reject if any exclude term is present
    for term in _UK_EXCLUDE:
        if term in loc:
            return False
    # Accept if any include term is present
    for term in _UK_INCLUDE:
        if term in loc:
            return True
    return False


def load_filters(path: str | Path) -> dict[str, Any]:
    """
    Load filter config from a YAML file.

    Expected keys:
        include_titles, exclude_titles, include_keywords, exclude_keywords
    All lists of lowercase strings.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Filters YAML not found: {path}")

    with open(path, encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}

    # Normalise everything to lowercase lists
    for key in ("include_titles", "exclude_titles", "include_keywords", "exclude_keywords"):
        raw = data.get(key, [])
        data[key] = [str(item).lower() for item in raw] if raw else []

    return data
