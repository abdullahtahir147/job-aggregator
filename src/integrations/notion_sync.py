"""
Push shortlisted jobs to a Notion database (rolling 4-day window).

Env vars:
    NOTION_TOKEN        – Notion integration token
    NOTION_DATABASE_ID  – Target database ID

Behaviour:
    1. Query all existing pages in the database.
    2. Archive pages whose Run Date is older than 4 days.
    3. Collect Link URLs from remaining (live) pages for dedup.
    4. Insert new jobs from today's shortlist if the link doesn't exist.
"""

from __future__ import annotations

import logging
import os
from datetime import date, timedelta
from typing import Any

import requests

from src.core.models import Job

logger = logging.getLogger(__name__)

NOTION_API = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"
ROLLING_WINDOW_DAYS = 4


def _headers() -> dict[str, str]:
    token = os.environ.get("NOTION_TOKEN", "")
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


# ── Page: {id, link_url, run_date_str} ──────────────────────────────────

def _fetch_all_pages(database_id: str) -> list[dict[str, Any]]:
    """Return all pages in the database with id, link URL, and run_date."""
    pages: list[dict[str, Any]] = []
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
            return pages
        data = resp.json()
        for page in data.get("results", []):
            props = page.get("properties", {})
            link_url = (props.get("Link") or {}).get("url", "")
            run_date_prop = (props.get("Run Date") or {}).get("date") or {}
            run_date_str = run_date_prop.get("start", "")
            pages.append({
                "id": page["id"],
                "link_url": link_url or "",
                "run_date": run_date_str,
            })
        has_more = data.get("has_more", False)
        if has_more:
            payload["start_cursor"] = data["next_cursor"]
    return pages


def _archive_page(page_id: str) -> bool:
    """Archive (soft-delete) a Notion page. Returns True on success."""
    resp = requests.patch(
        f"{NOTION_API}/pages/{page_id}",
        headers=_headers(),
        json={"archived": True},
    )
    return resp.status_code == 200


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


def sync_shortlist_to_notion(jobs: list[Job], limit: int = 15) -> dict[str, int]:
    """
    Push top *limit* jobs to the Notion database with a rolling window.

    Returns dict with keys: archived, kept, created.
    Silently skips if env vars are missing (returns all zeros).
    """
    result = {"archived": 0, "kept": 0, "created": 0}

    token = os.environ.get("NOTION_TOKEN", "")
    db_id = os.environ.get("NOTION_DATABASE_ID", "")
    if not token or not db_id:
        logger.info("Notion sync skipped — NOTION_TOKEN or NOTION_DATABASE_ID not set")
        return result

    run_date = date.today().isoformat()
    cutoff = (date.today() - timedelta(days=ROLLING_WINDOW_DAYS)).isoformat()

    # 1. Fetch all existing pages
    all_pages = _fetch_all_pages(db_id)

    # 2. Archive pages older than the rolling window
    live_urls: set[str] = set()
    for page in all_pages:
        if page["run_date"] and page["run_date"] < cutoff:
            if _archive_page(page["id"]):
                result["archived"] += 1
            else:
                logger.warning("Failed to archive page %s", page["id"])
        else:
            if page["link_url"]:
                live_urls.add(page["link_url"])
            result["kept"] += 1

    # 3. Insert new jobs (dedup against live pages)
    for job in jobs[:limit]:
        if job.url in live_urls:
            continue
        payload = _build_page(db_id, job, run_date)
        resp = requests.post(
            f"{NOTION_API}/pages",
            headers=_headers(),
            json=payload,
        )
        if resp.status_code == 200:
            result["created"] += 1
        else:
            logger.warning("Notion create failed (%d): %s", resp.status_code, resp.text[:200])

    # 4. Log summary
    print()
    print("  Notion sync summary:")
    print(f"    Archived pages  : {result['archived']}")
    print(f"    Existing kept   : {result['kept']}")
    print(f"    New jobs added  : {result['created']}")

    logger.info("Notion sync: archived=%d, kept=%d, created=%d",
                result["archived"], result["kept"], result["created"])
    return result
