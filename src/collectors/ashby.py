"""
Ashby ATS collector.

Uses the public posting API:
    https://api.ashbyhq.com/posting-api/job-board/{slug}

Docs: https://developers.ashbyhq.com/docs/posting-api-overview
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Optional

import requests

from src.core.models import Job

logger = logging.getLogger(__name__)

API_BASE = "https://api.ashbyhq.com/posting-api/job-board"
REQUEST_TIMEOUT = 10


class AshbyCollector:
    """Fetch and normalise jobs from an Ashby job board."""

    source = "ashby"

    def __init__(self, session: Optional[requests.Session] = None) -> None:
        self.session = session or requests.Session()

    def fetch_jobs(
        self,
        company_name: str,
        ats_slug: str,
        *,
        fetch_descriptions: bool = False,
    ) -> list[Job]:
        """
        Fetch all jobs from the Ashby board *ats_slug*.

        Ashby always returns descriptions in the payload; when
        *fetch_descriptions* is False we strip them post-normalisation
        to keep scoring lightweight (descriptions can be back-filled later
        if needed).
        """
        url = f"{API_BASE}/{ats_slug}"

        try:
            resp = self.session.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as exc:
            logger.error("[ASHBY/%s] %s", ats_slug, exc)
            return []
        except ValueError as exc:
            logger.error("[ASHBY/%s] bad JSON: %s", ats_slug, exc)
            return []

        raw_jobs = data.get("jobs", [])

        jobs: list[Job] = []
        for rj in raw_jobs:
            try:
                job = self._normalise(company_name, rj)
                if not fetch_descriptions:
                    job.description = ""
                jobs.append(job)
            except Exception:
                pass
        return jobs

    # ------------------------------------------------------------------

    @staticmethod
    def _normalise(company: str, raw: dict) -> Job:
        """Convert a single Ashby API posting dict into our Job model."""
        # Location: use primary location field
        location = raw.get("location", "")

        # Team / department
        team = raw.get("team") or raw.get("department") or None

        # Posted date
        posted_at = None
        published_str = raw.get("publishedAt")
        if published_str:
            try:
                dt = datetime.fromisoformat(published_str)
                posted_at = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            except (ValueError, AttributeError):
                posted_at = published_str

        # Description: prefer plain text, fall back to HTML
        description = raw.get("descriptionPlain") or raw.get("descriptionHtml", "")

        # URL: use the public job URL provided by the API
        url = raw.get("jobUrl", "")

        return Job(
            company=company,
            source="ashby",
            job_id=str(raw.get("id", "")),
            title=raw.get("title", ""),
            location=location,
            team=team,
            url=url,
            posted_at=posted_at,
            description=description,
            raw=json.dumps(raw, default=str),
        )
