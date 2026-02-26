"""
CLI entry point for the job aggregator.

Usage:
    python -m src.main run [OPTIONS]

Options:
    --companies  PATH   Path to companies CSV   (default: config/companies.csv)
    --filters    PATH   Path to filters YAML    (default: config/filters.yaml)
    --db         PATH   Path to SQLite database (default: data/jobs.db)
    --report     PATH   Path to output report   (default: output/report.md)
    --verbose           Enable debug logging
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import requests

from src.core.utils import setup_logging, now_iso, load_companies, load_filters, is_uk_role
from src.core.models import Job, RunRecord
from src.core.db import JobDB
from src.core.scoring import score_jobs, score_job
from src.core.reporting import generate_report, write_unknown_csv
from src.collectors.detect import detect_ats, init_cache
from src.collectors.greenhouse import GreenhouseCollector
from src.collectors.lever import LeverCollector

logger: logging.Logger = None  # type: ignore[assignment]

MAX_WORKERS = 8


# ──────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────

def _build_session() -> requests.Session:
    """Create a shared requests.Session with default headers."""
    s = requests.Session()
    s.headers.update({
        "Accept": "application/json",
        "User-Agent": "job-aggregator/1.1",
    })
    return s


def _title_might_match(title: str, filters: dict[str, Any]) -> bool:
    """Quick pre-check: does the title contain any include phrase?"""
    t = title.lower()
    for phrase in filters.get("include_titles", []):
        if phrase in t:
            return True
    return False


def _fetch_company(
    company_row: dict[str, str],
    gh: GreenhouseCollector,
    lv: LeverCollector,
    session: requests.Session,
) -> tuple[str, str, str | None, list[str], list[Job]]:
    """
    Detect ATS and fetch jobs for one company.  Thread-safe.

    Returns:
        (company_name, ats_type, ats_slug, detected_links, jobs)
    """
    company_name = company_row.get("company", "?")
    ats_type, ats_slug, detected_links = detect_ats(company_row, session=session)

    jobs: list[Job] = []
    if ats_type == "greenhouse" and ats_slug:
        jobs = gh.fetch_jobs(company_name, ats_slug, fetch_descriptions=False)
    elif ats_type == "lever" and ats_slug:
        jobs = lv.fetch_jobs(company_name, ats_slug, fetch_descriptions=False)

    return (company_name, ats_type, ats_slug, detected_links, jobs)


# ──────────────────────────────────────────────────────────────────────────
# Main pipeline
# ──────────────────────────────────────────────────────────────────────────

def run_pipeline(
    companies_path: str,
    filters_path: str,
    db_path: str,
    report_path: str,
) -> None:
    """Execute the full aggregation pipeline."""
    t0 = time.monotonic()
    run_id = uuid.uuid4().hex[:12]
    run_time = now_iso()
    logger.info("Starting run %s", run_id)

    # ── Load config ─────────────────────────────────────────────────────
    companies = load_companies(companies_path)
    filters = load_filters(filters_path)

    # ── Init shared session, collectors, cache, DB ──────────────────────
    session = _build_session()
    gh = GreenhouseCollector(session=session)
    lv = LeverCollector(session=session)
    cache = init_cache(Path(db_path).parent / "ats_cache.json")
    db = JobDB(db_path)

    last_run_time = db.get_last_run_time() or "1970-01-01T00:00:00Z"

    # ── Parallel fetch ──────────────────────────────────────────────────
    all_jobs: list[Job] = []
    unknown_companies: list[dict] = []
    companies_processed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(_fetch_company, row, gh, lv, session): row
            for row in companies
        }
        for future in as_completed(futures):
            row = futures[future]
            try:
                name, ats_type, ats_slug, detected_links, jobs = future.result()
            except Exception as exc:
                name = row.get("company", "?")
                logger.error("[%s] fetch failed: %s", name, exc)
                row["_detected_links"] = []
                unknown_companies.append(row)
                continue

            if ats_type in ("greenhouse", "lever") and ats_slug:
                all_jobs.extend(jobs)
                companies_processed += 1
                logger.info("[%s] %s/%s — %d jobs", name, ats_type, ats_slug, len(jobs))
            else:
                row["_detected_links"] = detected_links
                unknown_companies.append(row)
                logger.info("[%s] unknown ATS — skipped", name)

    # Save ATS cache after all detections complete
    cache.save()

    logger.info(
        "Fetched %d jobs from %d companies (%d unknown) in %.1fs",
        len(all_jobs), companies_processed, len(unknown_companies),
        time.monotonic() - t0,
    )

    # ── Store in DB ─────────────────────────────────────────────────────
    new_count = db.upsert_jobs(all_jobs)

    current_urls = {j.url for j in all_jobs if j.url}
    stale_count = db.mark_stale(current_urls)

    # ── Score (title-only pass — no descriptions yet) ───────────────────
    score_jobs(all_jobs, filters)

    # ── Lazy description back-fill for promising jobs ───────────────────
    promising = [j for j in all_jobs if _title_might_match(j.title, filters) or j.score >= 1]
    backfilled = 0
    if promising:
        backfilled = _backfill_descriptions(promising, gh, filters)

    # ── Identify new jobs ───────────────────────────────────────────────
    new_urls = {r["url"] for r in db.get_new_jobs_since(last_run_time)}
    new_jobs = [j for j in all_jobs if j.url in new_urls]

    # ── UK counts ───────────────────────────────────────────────────────
    new_uk_count = sum(1 for j in new_jobs if is_uk_role(j.location))
    top_uk_count = sum(1 for j in all_jobs if j.score >= 3 and is_uk_role(j.location))

    # ── Save run record ─────────────────────────────────────────────────
    run_record = RunRecord(
        run_id=run_id,
        run_time=run_time,
        companies_processed=companies_processed,
        jobs_found=len(all_jobs),
        jobs_new=new_count,
    )
    db.save_run(run_record)

    # ── Generate report ─────────────────────────────────────────────────
    report_out = Path(report_path)
    generate_report(
        run=run_record,
        new_jobs=new_jobs,
        all_jobs=all_jobs,
        unknown_companies=unknown_companies,
        output_path=report_out,
    )

    unknown_csv_path = report_out.parent / "unknown_companies.csv"
    write_unknown_csv(unknown_companies, unknown_csv_path)

    # ── Console summary ─────────────────────────────────────────────────
    elapsed = time.monotonic() - t0
    print()
    print("=" * 60)
    print(f"  Run {run_id} complete in {elapsed:.1f}s")
    print(f"  Companies processed : {companies_processed}")
    print(f"  Unknown ATS         : {len(unknown_companies)}")
    print(f"  Total jobs fetched  : {len(all_jobs)}")
    print(f"  New jobs this run   : {new_count}")
    print(f"  New UK jobs         : {new_uk_count}")
    print(f"  Stale jobs marked   : {stale_count}")
    print(f"  Top UK matches (≥3) : {top_uk_count}")
    print(f"  Descriptions loaded : {backfilled}")
    print(f"  Report              : {report_path}")
    print(f"  Unknown CSV         : {unknown_csv_path}")
    print("=" * 60)
    print()

    db.close()


def _backfill_descriptions(
    jobs: list[Job],
    gh: GreenhouseCollector,
    filters: dict[str, Any],
) -> int:
    """
    Fetch full descriptions only for jobs whose titles look promising.

    For Greenhouse jobs we hit the single-job endpoint.
    Lever descriptions were already in the payload (stripped earlier);
    we re-score after back-fill.

    Returns count of descriptions successfully loaded.
    """
    gh_jobs = [j for j in jobs if j.source == "greenhouse" and not j.description]
    if not gh_jobs:
        return 0

    logger.info("Back-filling descriptions for %d promising Greenhouse jobs", len(gh_jobs))
    loaded = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(gh.fetch_description, j): j for j in gh_jobs}
        for future in as_completed(futures):
            try:
                result = future.result()
                if result.description:
                    loaded += 1
            except Exception:
                pass

    # Re-score with descriptions now available
    for j in jobs:
        score_job(j, filters)

    return loaded


# ──────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="job-aggregator",
        description="ATS-first job aggregation tool (Greenhouse + Lever)",
    )
    sub = parser.add_subparsers(dest="command")

    run_parser = sub.add_parser("run", help="Run the full aggregation pipeline")
    run_parser.add_argument(
        "--companies", default="config/companies.csv",
        help="Path to companies CSV (default: config/companies.csv)",
    )
    run_parser.add_argument(
        "--filters", default="config/filters.yaml",
        help="Path to filters YAML (default: config/filters.yaml)",
    )
    run_parser.add_argument(
        "--db", default="data/jobs.db",
        help="Path to SQLite database (default: data/jobs.db)",
    )
    run_parser.add_argument(
        "--report", default="output/report.md",
        help="Path to output report (default: output/report.md)",
    )
    run_parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable debug logging",
    )

    return parser


def main() -> None:
    global logger
    parser = build_parser()
    args = parser.parse_args()

    if args.command != "run":
        parser.print_help()
        sys.exit(1)

    logger = setup_logging(verbose=args.verbose)

    run_pipeline(
        companies_path=args.companies,
        filters_path=args.filters,
        db_path=args.db,
        report_path=args.report,
    )


if __name__ == "__main__":
    main()
