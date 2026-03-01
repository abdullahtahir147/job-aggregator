"""
Deterministic role taxonomy classifier.

Assigns a single tag to each job based on title + description matching
against configurable phrase lists. First matching tag in priority order wins.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Optional

import yaml

from src.core.models import Job

logger = logging.getLogger(__name__)

# Priority order — first match wins
TAG_PRIORITY = [
    "DEVREL_EVANGELISM",
    "APPLIED_AI_FDE",
    "MARKETING_SCIENCE",
    "PRODUCT_GROWTH_DATA",
    "RISK_FRAUD",
    "CORE_DS",
    "ANALYTICS_BI",
]

DEFAULT_TAG = "OTHER"


def load_taxonomy_rules(path: str | Path) -> dict[str, Any]:
    """Load taxonomy rules from YAML. Returns empty dict on failure."""
    path = Path(path)
    if not path.exists():
        logger.warning("Taxonomy config not found: %s", path)
        return {}
    with open(path, encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    rules = data.get("taxonomy", {})
    logger.info("Loaded taxonomy rules: %d tags", len(rules))
    return rules


def classify_tag(
    title: str,
    description: Optional[str],
    rules: dict[str, Any],
) -> str:
    """
    Return the single best tag for a job.

    Checks tags in TAG_PRIORITY order; first match wins.
    Matching is case-insensitive substring on (title + " " + description).
    """
    text = (title or "").lower()
    if description:
        text += " " + description.lower()

    for tag in TAG_PRIORITY:
        phrases = rules.get(tag, {}).get("any", [])
        for phrase in phrases:
            if phrase.lower() in text:
                return tag

    return DEFAULT_TAG


def apply_taxonomy(jobs: list[Job], rules: dict[str, Any]) -> None:
    """Set job.tag for every job in-place."""
    if not rules:
        return
    for job in jobs:
        job.tag = classify_tag(job.title, job.description, rules)
