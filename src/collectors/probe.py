"""
Lightweight ATS probe for unknown companies.

Generates slug candidates from company name + careers URL, then hits
the public JSON APIs of each supported ATS to see if any return jobs.

Does NOT modify companies.csv or ats_cache.json — only produces
suggestions written to output/suggested_overrides.csv.
"""

from __future__ import annotations

import csv
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import requests

from src.core.utils import get_thread_session

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = 6
MAX_RETRIES = 2
PROBE_WORKERS = 6

# Suffixes to strip when generating slug candidates
_STRIP_SUFFIXES = {"ltd", "limited", "plc", "inc", "group", "technologies",
                   "technology", "tech", "ai", "io", "hq", "labs", "co"}

# Prefixes to strip from hostnames
_HOST_NOISE = {"careers", "jobs", "apply", "join", "work", "www", "boards"}


# ── Slug candidate generation ────────────────────────────────────────

def _candidates_from_name(name: str) -> list[str]:
    """Generate slug candidates from company name."""
    # Normalise
    base = name.lower().strip()
    base = re.sub(r"[()&.,!'\"]", "", base)  # remove punctuation
    base = re.sub(r"\s+", " ", base).strip()

    slugs: list[str] = []
    words = base.split()

    # Full name variants
    slugs.append("".join(words))           # "deepmind"
    slugs.append("-".join(words))           # "deep-mind"

    # Strip suffixes
    core = [w for w in words if w not in _STRIP_SUFFIXES]
    if core and core != words:
        slugs.append("".join(core))
        slugs.append("-".join(core))

    # First word only (common pattern)
    if len(words) > 1:
        slugs.append(words[0])

    return slugs


def _candidates_from_url(url: str) -> list[str]:
    """Generate slug candidates from careers URL hostname."""
    try:
        host = urlparse(url).hostname or ""
    except Exception:
        return []

    parts = host.split(".")
    # Remove TLD and www
    candidates: list[str] = []
    for p in parts:
        if p in _HOST_NOISE or len(p) <= 2:
            continue
        # e.g. "asoscareers" -> also try "asos"
        clean = p
        for noise in _HOST_NOISE:
            if clean.endswith(noise) and len(clean) > len(noise):
                candidates.append(clean.replace(noise, ""))
        candidates.append(clean)

    return candidates


def generate_candidates(company_name: str, careers_url: str) -> list[str]:
    """Return deduplicated slug candidates (max 15)."""
    raw = _candidates_from_name(company_name) + _candidates_from_url(careers_url)
    seen: set[str] = set()
    unique: list[str] = []
    for s in raw:
        s = s.strip().lower()
        if s and s not in seen and len(s) >= 2:
            seen.add(s)
            unique.append(s)
    return unique[:15]


# ── ATS endpoint probes ──────────────────────────────────────────────

def _get_json(session: requests.Session, url: str) -> Optional[dict | list]:
    """GET JSON with retry on 429. Returns parsed JSON or None."""
    for attempt in range(MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 429:
                time.sleep(1)
                continue
            if resp.status_code != 200:
                return None
            return resp.json()
        except (requests.RequestException, ValueError):
            return None
    return None


def _probe_greenhouse(session: requests.Session, slug: str) -> Optional[dict]:
    url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
    data = _get_json(session, url)
    if isinstance(data, dict) and "jobs" in data:
        jobs = data["jobs"]
        if isinstance(jobs, list) and len(jobs) > 0:
            return {"ats": "greenhouse", "slug": slug, "count": len(jobs), "url": url}
    return None


def _probe_lever(session: requests.Session, slug: str) -> Optional[dict]:
    url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
    data = _get_json(session, url)
    if isinstance(data, list) and len(data) > 0:
        return {"ats": "lever", "slug": slug, "count": len(data), "url": url}
    return None


def _probe_ashby(session: requests.Session, slug: str) -> Optional[dict]:
    url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}"
    data = _get_json(session, url)
    if isinstance(data, dict) and "jobs" in data:
        jobs = data["jobs"]
        if isinstance(jobs, list) and len(jobs) > 0:
            return {"ats": "ashby", "slug": slug, "count": len(jobs), "url": url}
    return None


def _probe_smartrecruiters(session: requests.Session, slug: str) -> Optional[dict]:
    url = f"https://api.smartrecruiters.com/v1/companies/{slug}/postings?limit=1&offset=0"
    data = _get_json(session, url)
    if isinstance(data, dict):
        total = data.get("totalFound", 0)
        if total and total > 0:
            return {"ats": "smartrecruiters", "slug": slug, "count": total, "url": url}
    return None


_PROBES = [_probe_greenhouse, _probe_lever, _probe_ashby, _probe_smartrecruiters]


# ── Per-company probe ────────────────────────────────────────────────

def probe_unknown_company(
    company_name: str,
    careers_url: str,
    session: requests.Session,
) -> list[dict]:
    """
    Probe all supported ATS APIs for slug candidates derived from the
    company name and careers URL.

    Returns list of suggestion dicts. Stops early after 2 hits.
    """
    candidates = generate_candidates(company_name, careers_url)
    hits: list[dict] = []

    for slug in candidates:
        for probe_fn in _PROBES:
            result = probe_fn(session, slug)
            if result:
                hits.append({
                    "company": company_name,
                    "careers_url": careers_url,
                    "suggested_ats_type": result["ats"],
                    "suggested_slug": result["slug"],
                    "sample_count": result["count"],
                    "evidence": result["url"],
                })
                if len(hits) >= 2:
                    return hits
    return hits


# ── Batch probe (called from pipeline) ───────────────────────────────

def probe_unknown_batch(
    unknown_companies: list[dict],
    limit: int = 20,
) -> list[dict]:
    """
    Probe a batch of unknown companies concurrently.

    Returns flat list of suggestion dicts.
    """
    to_probe = unknown_companies[:limit]
    all_suggestions: list[dict] = []

    def _probe_one(row: dict) -> list[dict]:
        session = get_thread_session()
        name = row.get("company", "?")
        url = row.get("careers_url", "")
        logger.info("[probe] Probing %s ...", name)
        results = probe_unknown_company(name, url, session)
        if results:
            logger.info("[probe] %s — found %d suggestion(s)", name, len(results))
        else:
            logger.debug("[probe] %s — no matches", name)
        return results

    with ThreadPoolExecutor(max_workers=PROBE_WORKERS) as pool:
        futures = {pool.submit(_probe_one, row): row for row in to_probe}
        for future in as_completed(futures):
            try:
                all_suggestions.extend(future.result())
            except Exception as exc:
                logger.debug("[probe] error: %s", exc)

    return all_suggestions


def write_suggestions_csv(suggestions: list[dict], path: str | Path) -> None:
    """Write suggestions to a CSV file."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "company", "careers_url", "suggested_ats_type",
        "suggested_slug", "sample_count", "evidence",
    ]
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for s in suggestions:
            writer.writerow(s)
