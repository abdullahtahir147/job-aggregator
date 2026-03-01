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
    --probe-unknown     Probe unknown companies for ATS suggestions
    --probe-limit  N    Max companies to probe  (default: 20)
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from src.core.utils import (
    setup_logging, now_iso, load_companies, load_all_companies,
    load_filters, is_uk_role, get_thread_session, compute_job_age_days,
)
from src.core.models import Job, RunRecord
from src.core.db import JobDB
from src.core.scoring import score_jobs, score_job, compute_recency_delta
from src.core.intent import load_intent_rules, apply_intent
from src.core.taxonomy import load_taxonomy_rules, apply_taxonomy, TAG_PRIORITY
from src.core.reporting import (
    generate_report, write_diff_report, write_unknown_csv,
    write_shortlist_md, write_shortlist_csv, write_brief_md,
)
from src.collectors.detect import detect_ats, init_cache
from src.collectors.greenhouse import GreenhouseCollector
from src.collectors.lever import LeverCollector
from src.collectors.ashby import AshbyCollector
from src.collectors.smartrecruiters import SmartRecruitersCollector
from src.collectors.probe import probe_unknown_batch, write_suggestions_csv

logger: logging.Logger = None  # type: ignore[assignment]

MAX_WORKERS = 8

# Supported ATS types that have a collector
SUPPORTED_ATS = {"greenhouse", "lever", "ashby", "smartrecruiters"}


# ──────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────

def _write_llm_pack(
    jobs: list[Job],
    payload_path: Path,
    prompt_path: Path,
    intent_enabled: bool,
) -> None:
    """Write llm_payload.json and llm_prompt.md for the shortlisted jobs."""
    payload_path.parent.mkdir(parents=True, exist_ok=True)

    payload = []
    for i, job in enumerate(jobs, 1):
        entry: dict[str, Any] = {
            "rank": i,
            "tag": job.tag,
            "company": job.company,
            "title": job.title,
            "location": job.location,
            "url": job.url or "",
            "final_score": job.final_score,
            "base_score": job.score,
            "intent_delta": job.intent_delta,
            "match_reason": "; ".join(job.score_reasons) if job.score_reasons else "",
            "intent_reasons": "; ".join(job.intent_reasons) if job.intent_reasons else "",
        }
        if job.description:
            entry["description"] = job.description[:1500]
        if job.posted_at:
            entry["posted_at"] = job.posted_at
        payload.append(entry)

    payload_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    prompt = """# Job Shortlist — LLM Review Prompt

You are helping a data scientist (based in the UK, focused on product analytics,
experimentation, marketing science, and applied AI/FDE roles) prioritise job
applications from the attached shortlist.

## Instructions

1. **Read** the JSON payload (attached or pasted below).
2. **Categorise** every job into exactly one bucket:
   - **Apply now** (top 5) — strong fit, worth an immediate application
   - **Maybe** (next 5) — decent fit, apply if time permits
   - **Ignore** (rest) — poor fit or low priority
3. For each job give a **2-line rationale** (why apply / why skip).
4. For the top 3 "Apply now" jobs:
   - Draft a **2-line LinkedIn message** to the hiring manager (professional,
     not cringe, mention a specific reason you're interested).
   - Suggest **3 CV bullet points** to emphasise for that role.
5. Keep output concise. Use markdown tables where helpful.

## Payload

See `llm_payload.json` (or paste its contents here).
"""
    prompt_path.write_text(prompt, encoding="utf-8")
    logging.getLogger(__name__).info("LLM pack written: %s, %s", payload_path, prompt_path)


def _title_might_match(title: str, filters: dict[str, Any]) -> bool:
    """Quick pre-check: does the title contain any include phrase?"""
    t = title.lower()
    for phrase in filters.get("include_titles", []):
        if phrase in t:
            return True
    return False


def _fetch_company(
    company_row: dict[str, str],
) -> tuple[str, str, str | None, list[str], list[Job]]:
    """
    Detect ATS and fetch jobs for one company.

    Each worker thread gets its own requests.Session via get_thread_session(),
    avoiding the thread-safety issues with shared Session objects.

    Returns:
        (company_name, ats_type, ats_slug, detected_links, jobs)
    """
    session = get_thread_session()
    company_name = company_row.get("company", "?")
    ats_type, ats_slug, detected_links = detect_ats(company_row, session=session)

    jobs: list[Job] = []
    if ats_type == "greenhouse" and ats_slug:
        gh = GreenhouseCollector(session=session)
        jobs = gh.fetch_jobs(company_name, ats_slug, fetch_descriptions=False)
    elif ats_type == "lever" and ats_slug:
        lv = LeverCollector(session=session)
        jobs = lv.fetch_jobs(company_name, ats_slug, fetch_descriptions=False)
    elif ats_type == "ashby" and ats_slug:
        ab = AshbyCollector(session=session)
        jobs = ab.fetch_jobs(company_name, ats_slug, fetch_descriptions=False)
    elif ats_type == "smartrecruiters" and ats_slug:
        sr = SmartRecruitersCollector(session=session)
        jobs = sr.fetch_jobs(company_name, ats_slug, fetch_descriptions=False)

    return (company_name, ats_type, ats_slug, detected_links, jobs)


# ──────────────────────────────────────────────────────────────────────────
# Main pipeline
# ──────────────────────────────────────────────────────────────────────────

def run_pipeline(
    companies_path: str,
    filters_path: str,
    db_path: str,
    report_path: str,
    *,
    probe_unknown: bool = False,
    probe_limit: int = 20,
    diff: bool = False,
    diff_limit_top: int = 25,
    diff_limit_uk: int = 40,
    diff_limit_other: int = 25,
    diff_score: int = 3,
    shortlist: int = 0,
    shortlist_uk_only: bool = True,
    exclude_seniority: str = "intern,graduate,apprentice,junior",
    intent: bool = False,
    intent_config: str = "config/intent.yaml",
    intent_apply_to_report: bool = False,
    taxonomy_config: str = "config/taxonomy.yaml",
    only_tags: str = "",
    exclude_tags: str = "",
    brief: bool = True,
    llm_pack: bool = True,
    recency: bool = True,
    recency_config: str = "config/recency.yaml",
) -> None:
    """Execute the full aggregation pipeline."""
    t0 = time.monotonic()
    run_id = uuid.uuid4().hex[:12]
    run_time = now_iso()
    logger.info("Starting run %s", run_id)

    # ── Load config ─────────────────────────────────────────────────────
    extra_csv = Path(companies_path).parent / "companies_extra_100.csv"
    companies = load_all_companies([companies_path, extra_csv])
    companies_loaded = len(companies)
    logger.info("Loaded %d companies from CSV(s)", companies_loaded)
    filters = load_filters(filters_path)

    # ── Init cache + DB (no shared session — each thread creates its own)
    cache = init_cache(Path(db_path).parent / "ats_cache.json")
    db = JobDB(db_path)

    last_run_time = db.get_last_run_time() or "1970-01-01T00:00:00Z"

    # ── Parallel fetch ──────────────────────────────────────────────────
    all_jobs: list[Job] = []
    unknown_companies: list[dict] = []
    companies_processed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(_fetch_company, row): row
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

            if ats_type in SUPPORTED_ATS and ats_slug:
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
        backfilled = _backfill_descriptions(promising, filters)

    # ── Identify new jobs ───────────────────────────────────────────────
    # Use run_time (current run start) as cutoff — not last_run_time
    # (which is the *start* of the previous run, before its upserts).
    new_urls = {r["url"] for r in db.get_new_jobs_since(run_time)}
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

    # ── Optional: diff report ─────────────────────────────────────────
    diff_path = report_out.parent / "diff_report.md"
    if diff:
        write_diff_report(
            run=run_record,
            new_jobs=new_jobs,
            previous_run_time=last_run_time if last_run_time != "1970-01-01T00:00:00Z" else None,
            output_path=diff_path,
            limit_top=diff_limit_top,
            limit_uk=diff_limit_uk,
            limit_other=diff_limit_other,
            min_score=diff_score,
        )
        logger.info("Diff report written to %s", diff_path)

    # ── Optional: intent scoring ─────────────────────────────────────
    intent_rules: dict = {}
    if intent:
        intent_rules = load_intent_rules(intent_config)
        apply_intent(all_jobs, intent_rules)
    else:
        # Ensure final_score = base score when intent is off
        for j in all_jobs:
            j.final_score = j.score

    # ── Optional: recency scoring ─────────────────────────────────
    recency_rules: dict = {}
    if recency:
        recency_cfg_path = Path(recency_config)
        if recency_cfg_path.exists():
            import yaml
            with open(recency_cfg_path, encoding="utf-8") as fh:
                recency_rules = yaml.safe_load(fh) or {}
        for j in all_jobs:
            j.age_days = compute_job_age_days(j.posted_at)
            delta, reason = compute_recency_delta(j.age_days, recency_rules)
            j.recency_delta = delta
            j.recency_reason = reason
            j.final_score = j.score + j.intent_delta + j.recency_delta
    else:
        for j in all_jobs:
            j.age_days = compute_job_age_days(j.posted_at)

    # ── Optional: taxonomy tagging ───────────────────────────────────
    taxonomy_rules: dict = {}
    if shortlist > 0:
        taxonomy_rules = load_taxonomy_rules(taxonomy_config)
        if taxonomy_rules:
            apply_taxonomy(all_jobs, taxonomy_rules)

    # ── Optional: shortlist ──────────────────────────────────────────
    shortlist_md_path = report_out.parent / "shortlist.md"
    shortlist_csv_path = report_out.parent / "shortlist.csv"
    shortlist_count = 0
    shortlisted: list[Job] = []
    if shortlist > 0:
        seniority_terms = [t.strip().lower() for t in exclude_seniority.split(",") if t.strip()]
        pool = list(all_jobs)
        if shortlist_uk_only:
            pool = [j for j in pool if is_uk_role(j.location)]
        if seniority_terms:
            pool = [j for j in pool if not any(t in j.title.lower() for t in seniority_terms)]
        # Tag filters
        if only_tags:
            keep = {t.strip().upper() for t in only_tags.split(",") if t.strip()}
            pool = [j for j in pool if j.tag in keep]
        if exclude_tags:
            drop = {t.strip().upper() for t in exclude_tags.split(",") if t.strip()}
            pool = [j for j in pool if j.tag not in drop]
        pool.sort(key=lambda j: (-j.final_score, j.age_days if j.age_days is not None else 9999, j.company))
        shortlisted = pool[:shortlist]
        shortlist_count = len(shortlisted)
        write_shortlist_md(
            jobs=shortlisted, output_path=shortlist_md_path,
            run_id=run_id, generated_at=run_time, limit=shortlist,
            uk_only=shortlist_uk_only, exclude_seniority=seniority_terms,
            intent_enabled=intent,
        )
        write_shortlist_csv(jobs=shortlisted, output_path=shortlist_csv_path,
                            intent_enabled=intent)

    # ── Optional: brief ──────────────────────────────────────────────
    brief_path = report_out.parent / "brief.md"
    if shortlist > 0 and brief and shortlisted:
        write_brief_md(
            output_path=brief_path,
            run=run_record,
            shortlist_jobs=shortlisted,
            intent_enabled=intent,
            new_uk_count=new_uk_count,
            unknown_ats_count=len(unknown_companies),
        )

    # ── Optional: LLM pack ───────────────────────────────────────────
    llm_payload_path = report_out.parent / "llm_payload.json"
    llm_prompt_path = report_out.parent / "llm_prompt.md"
    if shortlist > 0 and llm_pack and shortlisted:
        _write_llm_pack(shortlisted, llm_payload_path, llm_prompt_path, intent)

    # ── Optional: probe unknowns for ATS suggestions ─────────────────
    suggestions_count = 0
    suggestions_path = report_out.parent / "suggested_overrides.csv"
    if probe_unknown and unknown_companies:
        logger.info("Probing %d unknown companies (limit %d)...",
                     min(len(unknown_companies), probe_limit), probe_limit)
        suggestions = probe_unknown_batch(unknown_companies, limit=probe_limit)
        suggestions_count = len(suggestions)
        write_suggestions_csv(suggestions, suggestions_path)
        logger.info("Wrote %d suggestions to %s", suggestions_count, suggestions_path)

    # ── Console summary ─────────────────────────────────────────────────
    elapsed = time.monotonic() - t0
    print()
    print("=" * 60)
    print(f"  Run {run_id} complete in {elapsed:.1f}s")
    print(f"  Companies loaded    : {companies_loaded}")
    print(f"  Companies processed : {companies_processed}")
    print(f"  Unknown ATS         : {len(unknown_companies)}")
    print(f"  Total jobs fetched  : {len(all_jobs)}")
    print(f"  New jobs this run   : {new_count}")
    print(f"  New UK jobs         : {new_uk_count}")
    print(f"  Stale jobs marked   : {stale_count}")
    print(f"  Top UK matches (≥3) : {top_uk_count}")
    print(f"  Descriptions loaded : {backfilled}")
    if probe_unknown:
        print(f"  Suggested overrides : {suggestions_count}")
        print(f"  Suggestions CSV     : {suggestions_path}")
    print(f"  Report              : {report_path}")
    if diff:
        print(f"  Diff report         : {diff_path}")
    if shortlist > 0:
        print(f"  Shortlist ({shortlist_count:>3} jobs) : {shortlist_md_path}")
        print(f"  Shortlist CSV       : {shortlist_csv_path}")
        if brief and shortlist_count > 0:
            print(f"  Brief               : {brief_path}")
        if llm_pack and shortlist_count > 0:
            print(f"  LLM payload         : {llm_payload_path}")
            print(f"  LLM prompt          : {llm_prompt_path}")
    print(f"  Unknown CSV         : {unknown_csv_path}")
    print("=" * 60)
    print()

    db.close()


def _backfill_descriptions(
    jobs: list[Job],
    filters: dict[str, Any],
) -> int:
    """
    Fetch full descriptions only for jobs whose titles look promising.

    For Greenhouse jobs we hit the single-job endpoint.
    Lever and Ashby descriptions were already in the payload (stripped earlier);
    we re-score after back-fill.

    Returns count of descriptions successfully loaded.
    """
    gh_jobs = [j for j in jobs if j.source == "greenhouse" and not j.description]
    if not gh_jobs:
        return 0

    logger.info("Back-filling descriptions for %d promising Greenhouse jobs", len(gh_jobs))
    loaded = 0
    failed = 0

    def _fetch_one_description(job: Job) -> Job:
        """Per-thread: create own session + collector, fetch description."""
        session = get_thread_session()
        gh = GreenhouseCollector(session=session)
        return gh.fetch_description(job)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_fetch_one_description, j): j for j in gh_jobs}
        for future in as_completed(futures):
            try:
                result = future.result()
                if result.description:
                    loaded += 1
            except Exception as exc:
                failed += 1
                logger.debug("Description backfill failed: %s", exc)

    if failed:
        logger.info("Backfill: %d loaded, %d failed", loaded, failed)

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
        description="ATS-first job aggregation tool (Greenhouse + Lever + Ashby + SmartRecruiters)",
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
    run_parser.add_argument(
        "--probe-unknown", action="store_true", default=False,
        help="Probe unknown companies to suggest ATS overrides",
    )
    run_parser.add_argument(
        "--probe-limit", type=int, default=20,
        help="Max companies to probe (default: 20)",
    )
    run_parser.add_argument(
        "--diff", action="store_true", default=False,
        help="Generate a diff report (output/diff_report.md) showing changes since last run",
    )
    run_parser.add_argument(
        "--diff-limit-top", type=int, default=25,
        help="Max top matches in diff report (default: 25)",
    )
    run_parser.add_argument(
        "--diff-limit-uk", type=int, default=40,
        help="Max UK jobs in diff report (default: 40)",
    )
    run_parser.add_argument(
        "--diff-limit-other", type=int, default=25,
        help="Max other jobs in diff report (default: 25)",
    )
    run_parser.add_argument(
        "--diff-score", type=int, default=3,
        help="Min score for top matches in diff report (default: 3)",
    )
    run_parser.add_argument(
        "--shortlist", type=int, default=0,
        help="Generate shortlist of top N jobs (default: 0/off)",
    )
    run_parser.add_argument(
        "--shortlist-uk-only", action=argparse.BooleanOptionalAction, default=True,
        help="Only include UK jobs in shortlist (default: True; use --no-shortlist-uk-only to disable)",
    )
    run_parser.add_argument(
        "--exclude-seniority", type=str, default="intern,graduate,apprentice,junior",
        help="Comma-separated seniority terms to exclude (default: intern,graduate,apprentice,junior)",
    )
    run_parser.add_argument(
        "--intent", action="store_true", default=False,
        help="Enable intent-aware scoring for shortlist ranking",
    )
    run_parser.add_argument(
        "--intent-config", type=str, default="config/intent.yaml",
        help="Path to intent rules YAML (default: config/intent.yaml)",
    )
    run_parser.add_argument(
        "--intent-apply-to-report", action="store_true", default=False,
        help="Also apply intent scoring to diff/report outputs (default: off)",
    )
    run_parser.add_argument(
        "--taxonomy-config", type=str, default="config/taxonomy.yaml",
        help="Path to taxonomy rules YAML (default: config/taxonomy.yaml)",
    )
    run_parser.add_argument(
        "--only-tags", type=str, default="",
        help="Comma-separated tags to include in shortlist (e.g. APPLIED_AI_FDE,DEVREL_EVANGELISM)",
    )
    run_parser.add_argument(
        "--exclude-tags", type=str, default="",
        help="Comma-separated tags to exclude from shortlist (e.g. RISK_FRAUD,OTHER)",
    )
    run_parser.add_argument(
        "--brief", action=argparse.BooleanOptionalAction, default=True,
        help="Generate brief.md when shortlist is enabled (default: True)",
    )
    run_parser.add_argument(
        "--llm-pack", action=argparse.BooleanOptionalAction, default=True,
        help="Generate llm_payload.json + llm_prompt.md when shortlist is enabled (default: True)",
    )
    run_parser.add_argument(
        "--recency", action=argparse.BooleanOptionalAction, default=True,
        help="Enable recency scoring (boost fresh jobs, default: True; --no-recency to disable)",
    )
    run_parser.add_argument(
        "--recency-config", type=str, default="config/recency.yaml",
        help="Path to recency rules YAML (default: config/recency.yaml)",
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
        probe_unknown=args.probe_unknown,
        probe_limit=args.probe_limit,
        diff=args.diff,
        diff_limit_top=args.diff_limit_top,
        diff_limit_uk=args.diff_limit_uk,
        diff_limit_other=args.diff_limit_other,
        diff_score=args.diff_score,
        shortlist=args.shortlist,
        shortlist_uk_only=args.shortlist_uk_only,
        exclude_seniority=args.exclude_seniority,
        intent=args.intent,
        intent_config=args.intent_config,
        intent_apply_to_report=args.intent_apply_to_report,
        taxonomy_config=args.taxonomy_config,
        only_tags=args.only_tags,
        exclude_tags=args.exclude_tags,
        brief=args.brief,
        llm_pack=args.llm_pack,
        recency=args.recency,
        recency_config=args.recency_config,
    )


if __name__ == "__main__":
    main()
