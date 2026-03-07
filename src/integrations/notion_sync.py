"""
Push shortlisted jobs to a Notion database.

Env vars:
    NOTION_TOKEN        – Notion integration token
    NOTION_DATABASE_ID  – Target database ID
"""

from __future__ import annotations

import logging
import os
from datetime import date
from typing import Any

import requests

from src.core.models import Job

logger = logging.getLogger(__name__)

NOTION_API = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"


def _headers() -> dict[str, str]:
    token = os.environ.get("NOTION_TOKEN", "")
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def _existing_urls(database_id: str) -> set[str]:
    """Query the Notion DB and return all Link URLs already present."""
    urls: set[str] = set()
    payload: dict[str, Any] = {"page_size": 100}
    has_more = True
    while has_more:
        resp = requests.post(
            f"{NOTION_API}/databases/{database_id}/query",
            headers=_headers(),
            json=payload,
        )
        if resp.status_code != 200:
            logger.warning("Notion query failed (%d): %s", resp.status_code, resp.text[:200])
            return urls
        data = resp.json()
        for page in data.get("results", []):
            link_prop = page.get("properties", {}).get("Link", {})
            if link_prop.get("url"):
                urls.add(link_prop["url"])
        has_more = data.get("has_more", False)
        if has_more:
            payload["start_cursor"] = data["next_cursor"]
    return urls


def _build_page(database_id: str, job: Job, run_date: str) -> dict[str, Any]:
    """Build a Notion page payload for one job."""
    return {
        "parent": {"database_id": database_id},
        "properties": {
            "Company": {"title": [{"text": {"content": job.company}}]},
            "Title": {"rich_text": [{"text": {"content": job.title}}]},
            "Link": {"url": job.url or ""},
            "Location": {"rich_text": [{"text": {"content": job.location or ""}}]},
            "Score": {"number": job.final_score},
            "Age Days": {"number": job.age_days if job.age_days is not None else -1},
            "Run Date": {"date": {"start": run_date}},
        },
    }


def sync_shortlist_to_notion(jobs: list[Job], limit: int = 15) -> int:
    """
    Push top *limit* jobs to the Notion database.

    Returns count of pages created.  Silently skips if env vars are missing.
    """
    token = os.environ.get("NOTION_TOKEN", "")
    db_id = os.environ.get("NOTION_DATABASE_ID", "")
    if not token or not db_id:
        logger.info("Notion sync skipped — NOTION_TOKEN or NOTION_DATABASE_ID not set")
        return 0

    run_date = date.today().isoformat()
    existing = _existing_urls(db_id)
    logger.info("Notion DB has %d existing links", len(existing))

    created = 0
    for job in jobs[:limit]:
        if job.url in existing:
            logger.debug("Skipping duplicate: %s", job.url)
            continue
        payload = _build_page(db_id, job, run_date)
        resp = requests.post(
            f"{NOTION_API}/pages",
            headers=_headers(),
            json=payload,
        )
        if resp.status_code == 200:
            created += 1
        else:
            logger.warning("Notion create failed (%d): %s", resp.status_code, resp.text[:200])

    logger.info("Notion sync: %d pages created, %d duplicates skipped",
                created, len(jobs[:limit]) - created)
    return created
