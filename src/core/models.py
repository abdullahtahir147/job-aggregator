"""
Normalized job schema used across all collectors and storage.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from typing import Optional


@dataclass
class Job:
    """A single job posting, normalised across ATS sources."""

    company: str
    source: str  # "greenhouse" | "lever"
    job_id: str
    title: str
    location: str
    url: str
    team: Optional[str] = None
    posted_at: Optional[str] = None  # ISO-8601 string
    description: Optional[str] = None
    raw: str = ""  # JSON string of original payload

    # ---- populated after scoring ----
    score: int = 0
    score_reasons: list[str] = field(default_factory=list)

    # ---- populated by DB layer ----
    first_seen: Optional[str] = None
    last_seen: Optional[str] = None
    is_active: int = 1

    def to_dict(self) -> dict:
        """Convert to a plain dict (for DB insertion, etc.)."""
        return asdict(self)

    def __repr__(self) -> str:
        return f"Job({self.company!r}, {self.title!r}, {self.source})"


@dataclass
class RunRecord:
    """Metadata for a single aggregation run."""

    run_id: str
    run_time: str  # ISO-8601
    companies_processed: int = 0
    jobs_found: int = 0
    jobs_new: int = 0
