"""
SmartRecruiters ATS collector.

Uses the public Posting API:
    LIST   https://api.smartrecruiters.com/v1/companies/{id}/postings?limit=100&offset=0
    DETAIL https://api.smartrecruiters.com/v1/companies/{id}/postings/{postingId}

No auth required for public postings.
"""

from __future__ import annotations

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import requests

from src.core.models import Job

logger = logging.getLogger(__name__)

API_BASE = "https://api.smartrecruiters.com/v1/companies"
REQUEST_TIMEOUT = 15
PAGE_SIZE = 100
MAX_DETAIL_JOBS = 100  # cap detail fetches per company
DETAIL_WORKERS = 6


class SmartRecruitersCollector:
    """Fetch and normalise jobs from a SmartRecruiters company board."""

    source = "smartrecruiters"

    def __init__(self, session: Optional[requests.Session] = None) -> None:
        self.session = session or requests.Session()

    # ── public API ────────────────────────────────────────────────────

    def fetch_jobs(
        self,
        company_name: str,
        company_id: str,
        *,
        fetch_descriptions: bool = False,
    ) -> list[Job]:
        """
        Fetch all postings for *company_id*.

        When *fetch_descriptions* is True, detail endpoints are hit
        concurrently (capped at DETAIL_WORKERS) for up to MAX_DETAIL_JOBS
        jobs.
        """
        raw_postings = self._list_all(company_id)
        if not raw_postings:
            return []

        jobs: list[Job] = []
        for rp in raw_postings:
            try:
                jobs.append(self._normalise(company_name, rp))
            except Exception:
                pass

        if fetch_descriptions:
            self._backfill_details(company_id, jobs)

        return jobs

    # ── paginated listing ─────────────────────────────────────────────

    def _list_all(self, company_id: str) -> list[dict]:
        """Paginate through the postings list endpoint."""
        all_postings: list[dict] = []
        offset = 0

        while True:
            url = f"{API_BASE}/{company_id}/postings"
            params = {"limit": PAGE_SIZE, "offset": offset}

            try:
                resp = self._get_with_retry(url, params=params)
                resp.raise_for_status()
                data = resp.json()
            except (requests.RequestException, ValueError) as exc:
                logger.error("[SR/%s] list page offset=%d: %s", company_id, offset, exc)
                break

            content = data.get("content", [])
            total = data.get("totalFound", 0)
            all_postings.extend(content)

            offset += PAGE_SIZE
            if offset >= total or not content:
                break

        return all_postings

    # ── detail backfill ───────────────────────────────────────────────

    def _backfill_details(self, company_id: str, jobs: list[Job]) -> None:
        """Fetch full descriptions for jobs (up to MAX_DETAIL_JOBS)."""
        to_fill = [j for j in jobs if not j.description][:MAX_DETAIL_JOBS]
        if not to_fill:
            return

        def _fetch_detail(job: Job) -> None:
            url = f"{API_BASE}/{company_id}/postings/{job.job_id}"
            try:
                resp = self._get_with_retry(url)
                resp.raise_for_status()
                detail = resp.json()
                job.description = self._extract_description(detail)
                posting_url = detail.get("postingUrl", "")
                if posting_url:
                    job.url = posting_url
            except Exception:
                pass

        with ThreadPoolExecutor(max_workers=DETAIL_WORKERS) as pool:
            futures = [pool.submit(_fetch_detail, j) for j in to_fill]
            for f in as_completed(futures):
                f.result()  # propagate nothing, errors swallowed in _fetch_detail

    # ── HTTP helper with 429 retry ────────────────────────────────────

    def _get_with_retry(
        self, url: str, params: dict | None = None, retries: int = 3
    ) -> requests.Response:
        """GET with simple back-off on 429."""
        for attempt in range(retries):
            resp = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if resp.status_code != 429:
                return resp
            wait = float(resp.headers.get("Retry-After", 1))
            logger.debug("[SR] 429 — sleeping %.1fs (attempt %d)", wait, attempt + 1)
            time.sleep(wait)
        return resp  # return last response even if still 429

    # ── normalisation ─────────────────────────────────────────────────

    @staticmethod
    def _normalise(company: str, raw: dict) -> Job:
        """Convert a SmartRecruiters list-endpoint posting into Job."""
        # Location
        loc = raw.get("location", {})
        location = loc.get("fullLocation", "")
        if not location:
            parts = [loc.get("city", ""), loc.get("region", ""), loc.get("country", "")]
            location = ", ".join(p for p in parts if p)
        if loc.get("remote"):
            location = f"{location} (Remote)" if location else "Remote"

        # Department / team
        dept = raw.get("department", {})
        team = dept.get("label") if isinstance(dept, dict) else None

        # Posted date
        posted_at = raw.get("releasedDate", "")

        # URL — construct from company + id
        company_id_raw = raw.get("company", {}).get("identifier", "")
        posting_id = str(raw.get("id", ""))
        url = f"https://jobs.smartrecruiters.com/{company_id_raw}/{posting_id}" if company_id_raw else ""

        return Job(
            company=company,
            source="smartrecruiters",
            job_id=posting_id,
            title=raw.get("name", ""),
            location=location,
            team=team,
            url=url,
            posted_at=posted_at,
            description="",  # filled by detail if requested
            raw=json.dumps(raw, default=str),
        )

    @staticmethod
    def _extract_description(detail: dict) -> str:
        """Concatenate jobAd sections into a single description string."""
        sections = detail.get("jobAd", {}).get("sections", {})
        parts: list[str] = []
        for key in ("jobDescription", "qualifications", "additionalInformation"):
            text = sections.get(key, {}).get("text", "")
            if text:
                parts.append(text)
        return "\n\n".join(parts)
