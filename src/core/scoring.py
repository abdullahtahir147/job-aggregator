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


def compute_recency_delta(age_days: int | None, rules: dict[str, Any]) -> tuple[int, str]:
    """
    Return (delta_score, reason_string) based on how old a posting is.

    *rules* keys: fresh_days, recent_days, fresh_boost, recent_boost, stale_penalty.
    """
    if age_days is None:
        return (0, "")

    fresh_days: int = rules.get("fresh_days", 2)
    recent_days: int = rules.get("recent_days", 7)
    fresh_boost: int = rules.get("fresh_boost", 3)
    recent_boost: int = rules.get("recent_boost", 1)
    stale_penalty: int = rules.get("stale_penalty", 0)

    if age_days <= fresh_days:
        return (fresh_boost, f"very recent ({age_days}d)")
    elif age_days <= recent_days:
        return (recent_boost, f"recent ({age_days}d)")
    else:
        if stale_penalty:
            return (stale_penalty, f"older listing ({age_days}d)")
        return (0, "")


def score_jobs(jobs: list[Job], filters: dict[str, Any]) -> list[Job]:
    """Score a batch of jobs. Returns the same list, mutated."""
    for job in jobs:
        score_job(job, filters)
    return jobs
