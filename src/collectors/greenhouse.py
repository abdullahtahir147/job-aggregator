"""
Greenhouse ATS collector.

Uses the public boards API:
    https://boards-api.greenhouse.io/v1/boards/{slug}/jobs

Docs: https://developers.greenhouse.io/job-board.html
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Optional

import requests

from src.core.models import Job

logger = logging.getLogger(__name__)

API_BASE = "https://boards-api.greenhouse.io/v1/boards"
REQUEST_TIMEOUT = 10


class GreenhouseCollector:
    """Fetch and normalise jobs from a Greenhouse job board."""

    source = "greenhouse"

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
        Fetch all jobs from the Greenhouse board *ats_slug*.

        When *fetch_descriptions* is False the API is called without
        ``content=true``, which is significantly faster for large boards.
        """
        url = f"{API_BASE}/{ats_slug}/jobs"
        params: dict[str, str] = {}
        if fetch_descriptions:
            params["content"] = "true"

        try:
            resp = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as exc:
            logger.error("[GH/%s] %s", ats_slug, exc)
            return []
        except ValueError as exc:
            logger.error("[GH/%s] bad JSON: %s", ats_slug, exc)
            return []

        raw_jobs = data.get("jobs", [])

        jobs: list[Job] = []
        for rj in raw_jobs:
            try:
                jobs.append(self._normalise(company_name, rj))
            except Exception:
                pass
        return jobs

    def fetch_description(self, job: Job) -> Job:
        """Fetch the full description for a single job (by re-hitting its board)."""
        if not job.job_id:
            return job
        # Greenhouse single-job endpoint: /v1/boards/{slug}/jobs/{id}
        # We can derive the slug from the URL or just use the content param
        # Simpler: use the job URL to find the board slug
        # But easier: just fetch the single job endpoint
        try:
            # Extract slug from URL pattern
            url = job.url
            if "greenhouse.io/" in url:
                parts = url.split("greenhouse.io/")[1].split("/")
                slug = parts[0]
                detail_url = f"{API_BASE}/{slug}/jobs/{job.job_id}"
                resp = self.session.get(
                    detail_url, params={"content": "true"}, timeout=REQUEST_TIMEOUT
                )
                resp.raise_for_status()
                data = resp.json()
                job.description = data.get("content", "")
        except Exception:
            pass
        return job

    # ------------------------------------------------------------------

    @staticmethod
    def _normalise(company: str, raw: dict) -> Job:
        """Convert a single Greenhouse API job dict into our Job model."""
        location = raw.get("location", {}).get("name", "") if isinstance(raw.get("location"), dict) else ""

        departments = raw.get("departments", [])
        team = departments[0]["name"] if departments else None

        posted_at = None
        updated_str = raw.get("updated_at") or raw.get("created_at")
        if updated_str:
            try:
                dt = datetime.fromisoformat(updated_str.replace("Z", "+00:00"))
                posted_at = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            except (ValueError, AttributeError):
                posted_at = updated_str

        description = raw.get("content", "")
        url = raw.get("absolute_url", "")

        return Job(
            company=company,
            source="greenhouse",
            job_id=str(raw.get("id", "")),
            title=raw.get("title", ""),
            location=location,
            team=team,
            url=url,
            posted_at=posted_at,
            description=description,
            raw=json.dumps(raw, default=str),
        )
