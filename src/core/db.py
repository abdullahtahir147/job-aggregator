"""
SQLite storage layer.

Tables:
  - jobs   : normalised job postings with first_seen / last_seen tracking
  - runs   : metadata for each aggregation run

Provides upsert (insert-or-update) and stale-job marking.
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Optional

from src.core.models import Job, RunRecord
from src.core.utils import now_iso

logger = logging.getLogger(__name__)

SCHEMA_JOBS = """
CREATE TABLE IF NOT EXISTS jobs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    company     TEXT    NOT NULL,
    source      TEXT    NOT NULL,
    job_id      TEXT    NOT NULL,
    url         TEXT    NOT NULL UNIQUE,
    title       TEXT    NOT NULL,
    location    TEXT    DEFAULT '',
    team        TEXT,
    posted_at   TEXT,
    description TEXT,
    raw         TEXT,
    first_seen  TEXT    NOT NULL,
    last_seen   TEXT    NOT NULL,
    is_active   INTEGER DEFAULT 1
);
"""

SCHEMA_RUNS = """
CREATE TABLE IF NOT EXISTS runs (
    run_id              TEXT PRIMARY KEY,
    run_time            TEXT NOT NULL,
    companies_processed INTEGER DEFAULT 0,
    jobs_found          INTEGER DEFAULT 0,
    jobs_new            INTEGER DEFAULT 0
);
"""

INDEX_JOBS = """
CREATE INDEX IF NOT EXISTS idx_jobs_company ON jobs (company);
CREATE INDEX IF NOT EXISTS idx_jobs_source  ON jobs (source);
CREATE INDEX IF NOT EXISTS idx_jobs_active  ON jobs (is_active);
"""


class JobDB:
    """Thin wrapper around a SQLite database for job storage."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        cur = self.conn.cursor()
        cur.executescript(SCHEMA_JOBS + SCHEMA_RUNS + INDEX_JOBS)
        self.conn.commit()

    # ------------------------------------------------------------------
    # Upsert jobs
    # ------------------------------------------------------------------

    def upsert_job(self, job: Job) -> bool:
        """
        Insert a new job or update an existing one (matched on url).

        Returns True if the job was newly inserted, False if updated.
        """
        ts = now_iso()
        cur = self.conn.cursor()

        # Check if exists
        cur.execute("SELECT id FROM jobs WHERE url = ?", (job.url,))
        row = cur.fetchone()

        if row:
            # Update existing
            cur.execute(
                """
                UPDATE jobs
                   SET title       = ?,
                       location    = ?,
                       team        = ?,
                       posted_at   = ?,
                       description = ?,
                       raw         = ?,
                       last_seen   = ?,
                       is_active   = 1
                 WHERE url = ?
                """,
                (
                    job.title,
                    job.location,
                    job.team,
                    job.posted_at,
                    job.description,
                    job.raw,
                    ts,
                    job.url,
                ),
            )
            self.conn.commit()
            return False
        else:
            # Insert new
            cur.execute(
                """
                INSERT INTO jobs
                    (company, source, job_id, url, title, location, team,
                     posted_at, description, raw, first_seen, last_seen, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                """,
                (
                    job.company,
                    job.source,
                    job.job_id,
                    job.url,
                    job.title,
                    job.location,
                    job.team,
                    job.posted_at,
                    job.description,
                    job.raw,
                    ts,
                    ts,
                ),
            )
            self.conn.commit()
            return True

    def upsert_jobs(self, jobs: list[Job]) -> int:
        """Upsert a batch of jobs. Returns count of newly inserted jobs."""
        new_count = 0
        for job in jobs:
            if job.url:  # skip jobs without a URL
                if self.upsert_job(job):
                    new_count += 1
        return new_count

    # ------------------------------------------------------------------
    # Mark stale jobs
    # ------------------------------------------------------------------

    def mark_stale(self, current_urls: set[str]) -> int:
        """
        Mark jobs not in *current_urls* as inactive (is_active=0).

        Returns number of jobs marked stale.
        """
        if not current_urls:
            return 0

        cur = self.conn.cursor()
        # Get all currently active URLs
        cur.execute("SELECT url FROM jobs WHERE is_active = 1")
        active_urls = {row["url"] for row in cur.fetchall()}

        stale_urls = active_urls - current_urls
        if not stale_urls:
            return 0

        placeholders = ",".join("?" for _ in stale_urls)
        cur.execute(
            f"UPDATE jobs SET is_active = 0 WHERE url IN ({placeholders})",
            list(stale_urls),
        )
        self.conn.commit()
        logger.info("Marked %d jobs as stale", len(stale_urls))
        return len(stale_urls)

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def get_new_jobs_since(self, since_iso: str) -> list[dict]:
        """Return jobs first_seen after *since_iso*."""
        cur = self.conn.cursor()
        cur.execute(
            "SELECT * FROM jobs WHERE first_seen > ? ORDER BY first_seen DESC",
            (since_iso,),
        )
        return [dict(row) for row in cur.fetchall()]

    def get_all_active_jobs(self) -> list[dict]:
        """Return all active jobs."""
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM jobs WHERE is_active = 1 ORDER BY company, title")
        return [dict(row) for row in cur.fetchall()]

    def get_first_seen_map(self) -> dict[str, str]:
        """Return a {url: first_seen} map for all jobs."""
        cur = self.conn.cursor()
        cur.execute("SELECT url, first_seen FROM jobs")
        return {row["url"]: row["first_seen"] for row in cur.fetchall()}

    def get_last_run_time(self) -> Optional[str]:
        """Return the run_time of the most recent completed run, or None."""
        cur = self.conn.cursor()
        cur.execute("SELECT run_time FROM runs ORDER BY run_time DESC LIMIT 1")
        row = cur.fetchone()
        return row["run_time"] if row else None

    # ------------------------------------------------------------------
    # Run records
    # ------------------------------------------------------------------

    def save_run(self, run: RunRecord) -> None:
        """Persist a run record."""
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT OR REPLACE INTO runs (run_id, run_time, companies_processed, jobs_found, jobs_new)
            VALUES (?, ?, ?, ?, ?)
            """,
            (run.run_id, run.run_time, run.companies_processed, run.jobs_found, run.jobs_new),
        )
        self.conn.commit()

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def close(self) -> None:
        self.conn.close()
