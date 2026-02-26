"""
Lever ATS collector.

Uses the public postings API:
    https://api.lever.co/v0/postings/{slug}?mode=json

Docs: https://github.com/lever/postings-api
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Optional

import requests

from src.core.models import Job

logger = logging.getLogger(__name__)

API_BASE = "https://api.lever.co/v0/postings"
REQUEST_TIMEOUT = 10


class LeverCollector:
    """Fetch and normalise jobs from a Lever job board."""

    source = "lever"

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
        Fetch all jobs from the Lever board *ats_slug*.

        Lever always returns descriptions in the payload; when
        *fetch_descriptions* is False we strip them post-normalisation
        to keep scoring lightweight (descriptions can be back-filled later).
        """
        url = f"{API_BASE}/{ats_slug}"
        params = {"mode": "json"}

        try:
            resp = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as exc:
            logger.error("[LV/%s] %s", ats_slug, exc)
            return []
        except ValueError as exc:
            logger.error("[LV/%s] bad JSON: %s", ats_slug, exc)
            return []

        if not isinstance(data, list):
            logger.warning("[LV/%s] unexpected response type: %s", ats_slug, type(data))
            return []

        jobs: list[Job] = []
        for rj in data:
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
        """Convert a single Lever API posting dict into our Job model."""
        categories = raw.get("categories", {})
        location = categories.get("location", "") if isinstance(categories, dict) else ""

        team = None
        if isinstance(categories, dict):
            team = categories.get("team") or categories.get("department") or None

        posted_at = None
        created_ms = raw.get("createdAt")
        if created_ms and isinstance(created_ms, (int, float)):
            try:
                dt = datetime.fromtimestamp(created_ms / 1000, tz=timezone.utc)
                posted_at = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            except (OSError, ValueError):
                pass

        description = raw.get("descriptionPlain") or raw.get("description", "")
        url = raw.get("hostedUrl", "")

        return Job(
            company=company,
            source="lever",
            job_id=str(raw.get("id", "")),
            title=raw.get("text", ""),
            location=location,
            team=team,
            url=url,
            posted_at=posted_at,
            description=description,
            raw=json.dumps(raw, default=str),
        )
