"""
ATS detection heuristics with JSON cache.

Given a company row from the CSV, determine which ATS it uses and
extract the board slug needed by the collector.

Priority:
  1. Manual override columns  (ats_type_override / ats_slug_override)
  2. Cached result from data/ats_cache.json
  3. URL pattern matching     (boards.greenhouse.io/<slug>, jobs.lever.co/<slug>,
                               jobs.ashbyhq.com/<slug>, jobs.smartrecruiters.com/<id>)
  4. HTML scraping fallback   (fetch careers page, scan for ATS links)

Returns a 3-tuple: (ats_type, slug, detected_links)
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Optional

import requests

logger = logging.getLogger(__name__)

# ---- regex patterns --------------------------------------------------------

_GH_BOARD_RE = re.compile(
    r"boards(?:\.eu)?\.greenhouse\.io/([a-zA-Z0-9_-]+)", re.IGNORECASE
)
_GH_BOARD_ALT_RE = re.compile(
    r"board\.greenhouse\.io/([a-zA-Z0-9_-]+)", re.IGNORECASE
)
_LV_BOARD_RE = re.compile(
    r"jobs\.lever\.co/([a-zA-Z0-9_-]+)", re.IGNORECASE
)
_ASHBY_BOARD_RE = re.compile(
    r"jobs\.ashbyhq\.com/([a-zA-Z0-9_.-]+)", re.IGNORECASE
)
_SR_BOARD_RE = re.compile(
    r"jobs\.smartrecruiters\.com/([a-zA-Z0-9_.-]+)", re.IGNORECASE
)

_ATS_LINK_RE = re.compile(
    r"https?://[^\s\"'<>]*(?:greenhouse\.io|lever\.co|workable\.com"
    r"|ashbyhq\.com|smartrecruiters\.com)[^\s\"'<>]*",
    re.IGNORECASE,
)

ATS_RESULT = tuple[str, Optional[str], list[str]]

REQUEST_TIMEOUT = 10


# ── Cache ──────────────────────────────────────────────────────────────────

class ATSCache:
    """Simple JSON file cache for ATS detection results."""

    def __init__(self, path: str | Path = "data/ats_cache.json") -> None:
        self.path = Path(path)
        self._data: dict[str, dict] = {}
        self._load()

    def _load(self) -> None:
        if self.path.exists():
            try:
                self._data = json.loads(self.path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                self._data = {}

    def get(self, company: str) -> Optional[ATS_RESULT]:
        """Return cached (ats_type, slug, detected_links) or None."""
        entry = self._data.get(company)
        if entry is None:
            return None
        return (entry["ats_type"], entry.get("slug"), entry.get("detected_links", []))

    def put(self, company: str, result: ATS_RESULT) -> None:
        """Store a detection result."""
        self._data[company] = {
            "ats_type": result[0],
            "slug": result[1],
            "detected_links": result[2],
        }

    def save(self) -> None:
        """Persist cache to disk."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self._data, indent=2), encoding="utf-8")


# Module-level cache instance (lazy-initialised by main.py)
_cache: Optional[ATSCache] = None


def init_cache(path: str | Path = "data/ats_cache.json") -> ATSCache:
    """Initialise and return the module-level cache."""
    global _cache
    _cache = ATSCache(path)
    return _cache


# ── Main entry point ───────────────────────────────────────────────────────

def detect_ats(
    company_row: dict[str, str],
    session: Optional[requests.Session] = None,
) -> ATS_RESULT:
    """
    Detect the ATS type and slug for a company.

    Returns:
        (ats_type, slug, detected_links)
    """
    company = company_row.get("company", "?")

    # --- 1. Manual override ---------------------------------------------------
    override_type = company_row.get("ats_type_override", "").strip().lower()
    override_slug = company_row.get("ats_slug_override", "").strip()
    if override_type and override_slug:
        logger.debug("[%s] Using manual override: %s / %s", company, override_type, override_slug)
        return (override_type, override_slug, [])

    # --- 2. Cache hit ---------------------------------------------------------
    if _cache is not None:
        cached = _cache.get(company)
        if cached is not None:
            logger.debug("[%s] Cache hit: %s / %s", company, cached[0], cached[1])
            return cached

    # --- 3. URL pattern matching ---------------------------------------------
    careers_url = company_row.get("careers_url", "").strip()
    if not careers_url:
        logger.warning("[%s] No careers_url provided", company)
        result: ATS_RESULT = ("unknown", None, [])
        _cache_put(company, result)
        return result

    ats_type, slug = _match_url(careers_url)
    if ats_type != "unknown":
        logger.debug("[%s] URL pattern matched: %s / %s", company, ats_type, slug)
        result = (ats_type, slug, [])
        _cache_put(company, result)
        return result

    # --- 4. HTML fallback -----------------------------------------------------
    result = _scrape_for_ats_links(careers_url, company, session)
    if result[0] != "unknown":
        logger.debug("[%s] HTML scrape found: %s / %s", company, result[0], result[1])
    else:
        logger.debug("[%s] Could not detect ATS for %s", company, careers_url)

    _cache_put(company, result)
    return result


def _cache_put(company: str, result: ATS_RESULT) -> None:
    if _cache is not None:
        _cache.put(company, result)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _match_url(url: str) -> tuple[str, Optional[str]]:
    """Try to extract ATS type + slug from the URL string directly."""
    m = _GH_BOARD_RE.search(url)
    if m:
        return ("greenhouse", m.group(1))
    m = _GH_BOARD_ALT_RE.search(url)
    if m:
        return ("greenhouse", m.group(1))
    m = _LV_BOARD_RE.search(url)
    if m:
        return ("lever", m.group(1))
    m = _ASHBY_BOARD_RE.search(url)
    if m:
        # Strip trailing path segments (e.g. /job-id) — slug is the first part
        slug = m.group(1).split("/")[0]
        return ("ashby", slug)
    m = _SR_BOARD_RE.search(url)
    if m:
        ident = m.group(1).split("/")[0]
        return ("smartrecruiters", ident)
    return ("unknown", None)


def _scrape_for_ats_links(
    url: str,
    company: str,
    session: Optional[requests.Session] = None,
) -> ATS_RESULT:
    """Fetch the careers page HTML and search for ATS links."""
    detected_links: list[str] = []
    requester = session or requests

    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        }
        resp = requester.get(url, headers=headers, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        resp.raise_for_status()
        html = resp.text

        redir_type, redir_slug = _match_url(resp.url)
        if redir_type != "unknown":
            return (redir_type, redir_slug, [])

        detected_links = list(set(_ATS_LINK_RE.findall(html)))

        # Check each ATS pattern against the page HTML
        m = _GH_BOARD_RE.search(html)
        if m:
            return ("greenhouse", m.group(1), detected_links)
        m = _GH_BOARD_ALT_RE.search(html)
        if m:
            return ("greenhouse", m.group(1), detected_links)
        m = _LV_BOARD_RE.search(html)
        if m:
            return ("lever", m.group(1), detected_links)
        m = _ASHBY_BOARD_RE.search(html)
        if m:
            slug = m.group(1).split("/")[0]
            return ("ashby", slug, detected_links)
        m = _SR_BOARD_RE.search(html)
        if m:
            ident = m.group(1).split("/")[0]
            return ("smartrecruiters", ident, detected_links)

    except requests.RequestException as exc:
        logger.debug("[%s] Failed to fetch %s: %s", company, url, exc)

    return ("unknown", None, detected_links)
