"""
Job scoring engine.

Reads filter config and computes a relevance score for each job based on
title and description keyword matching.

Scoring rules:
  +3  if title contains any include_titles phrase
  -5  if title contains any exclude_titles phrase
  +1  for each include_keyword found in (title + description)
  -2  for each exclude_keyword found in (title + description)
"""

from __future__ import annotations

import logging
from typing import Any

from src.core.models import Job

logger = logging.getLogger(__name__)


def score_job(job: Job, filters: dict[str, Any]) -> Job:
    """
    Compute a relevance score for *job* using *filters*.

    Mutates job.score and job.score_reasons in-place, then returns the job.
    """
    score = 0
    reasons: list[str] = []

    title_lower = (job.title or "").lower()
    # Combine title + description for keyword matching
    text_lower = title_lower + " " + (job.description or "").lower()

    # ---- Title includes (+3 each) ----
    for phrase in filters.get("include_titles", []):
        if phrase in title_lower:
            score += 3
            reasons.append(f"+3 title match: '{phrase}'")

    # ---- Title excludes (-5 each) ----
    for phrase in filters.get("exclude_titles", []):
        if phrase in title_lower:
            score -= 5
            reasons.append(f"-5 title exclude: '{phrase}'")

    # ---- Keyword includes (+1 each) ----
    for kw in filters.get("include_keywords", []):
        if kw in text_lower:
            score += 1
            reasons.append(f"+1 keyword: '{kw}'")

    # ---- Keyword excludes (-2 each) ----
    for kw in filters.get("exclude_keywords", []):
        if kw in text_lower:
            score -= 2
            reasons.append(f"-2 exclude kw: '{kw}'")

    job.score = score
    job.score_reasons = reasons
    return job


def score_jobs(jobs: list[Job], filters: dict[str, Any]) -> list[Job]:
    """Score a batch of jobs. Returns the same list, mutated."""
    for job in jobs:
        score_job(job, filters)
    return jobs
