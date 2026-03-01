"""
Intent-aware scoring layer.

Applies lightweight boosts/penalties on top of the base keyword score
to reflect the user's career direction preferences.

Rules are loaded from config/intent.yaml.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, Optional

import yaml

from src.core.models import Job

logger = logging.getLogger(__name__)


def load_intent_rules(path: str | Path) -> dict[str, Any]:
    """Load intent rules from a YAML file. Returns empty dict on failure."""
    path = Path(path)
    if not path.exists():
        logger.warning("Intent config not found: %s", path)
        return {}
    with open(path, encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    rules = data.get("intent", {})
    # Pre-compile regex patterns
    for section in ("boost", "penalty"):
        for entry in rules.get(section, []):
            if entry.get("regex"):
                entry["_compiled"] = re.compile(entry["phrase"], re.IGNORECASE)
    logger.info(
        "Loaded intent rules: %d boosts, %d penalties",
        len(rules.get("boost", [])),
        len(rules.get("penalty", [])),
    )
    return rules


def compute_intent_delta(
    title: str,
    description: Optional[str],
    rules: dict[str, Any],
) -> tuple[int, list[str]]:
    """
    Compute an intent delta from title + description.

    Returns (delta, reasons) where reasons are human-readable strings.
    Each phrase matches at most once (title checked first, then description).
    """
    delta = 0
    reasons: list[str] = []
    title_lower = (title or "").lower()
    desc_lower = (description or "").lower()

    for section, label in (("boost", "boost"), ("penalty", "penalty")):
        for entry in rules.get(section, []):
            phrase = entry["phrase"]
            d = entry["delta"]
            compiled = entry.get("_compiled")

            if compiled:
                matched = compiled.search(title_lower) or compiled.search(desc_lower)
            else:
                p = phrase.lower()
                matched = p in title_lower or p in desc_lower

            if matched:
                delta += d
                sign = f"+{d}" if d > 0 else str(d)
                reasons.append(f"{label}: {phrase} ({sign})")

    return delta, reasons


def apply_intent(jobs: list[Job], rules: dict[str, Any]) -> None:
    """
    Apply intent deltas to a list of jobs in-place.

    Sets job.intent_delta, job.intent_reasons, job.final_score.
    """
    if not rules:
        for job in jobs:
            job.intent_delta = 0
            job.intent_reasons = []
            job.final_score = job.score
        return

    for job in jobs:
        d, reasons = compute_intent_delta(job.title, job.description, rules)
        job.intent_delta = d
        job.intent_reasons = reasons
        job.final_score = job.score + d
