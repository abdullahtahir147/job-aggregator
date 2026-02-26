# CLAUDE.md — Project Guidelines for Claude Code

This file is read automatically by Claude Code (locally and via GitHub Actions).
Follow these guidelines for every change made in this repository.

---

## What this project is

`job-aggregator` is a Python CLI tool that aggregates job postings from UK tech companies.
It fetches from Greenhouse and Lever ATS systems, scores them for relevance, stores results
in SQLite, and generates a Markdown report.

Run it with:
```bash
pip install -r requirements.txt
python -m src.main run
```

Full docs: `README.md`.

---

## Repository layout

```
job-aggregator/           ← git repo root
├── src/
│   ├── main.py           ← CLI entry point + pipeline orchestration
│   ├── core/
│   │   ├── models.py     ← Job + RunRecord dataclasses
│   │   ├── db.py         ← SQLite layer (JobDB) — upsert + stale-marking
│   │   ├── scoring.py    ← Keyword-based scoring engine
│   │   ├── reporting.py  ← Markdown report + unknown_companies CSV
│   │   └── utils.py      ← CSV/YAML loaders, is_uk_role, logging
│   └── collectors/
│       ├── detect.py     ← ATS detection: URL patterns + HTML scrape + JSON cache
│       ├── greenhouse.py ← Greenhouse JSON API collector
│       └── lever.py      ← Lever JSON API collector
├── config/
│   ├── companies.csv     ← 50 UK target companies with ATS hints
│   └── filters.yaml      ← include/exclude titles and keywords for scoring
├── data/                 ← gitignored — generated at runtime
│   ├── jobs.db
│   └── ats_cache.json
├── output/               ← gitignored — generated at runtime
│   ├── report.md
│   └── unknown_companies.csv
├── requirements.txt
└── README.md
```

---

## Core conventions

- **Deduplication:** jobs are keyed on `url` (UNIQUE in DB); upsert on every run
- **ATS detection priority:** manual override → JSON cache → URL pattern → HTML scrape
- **Scoring:** `+3` title match, `-5` title exclude, `+1` keyword, `-2` keyword exclude
- **UK filtering:** `is_uk_role(location)` in `core/utils.py`
- **Concurrency:** `ThreadPoolExecutor(max_workers=8)` for company fetching and description backfill
- **All text matching is case-insensitive** (lowercased before comparison)

---

## What Claude SHOULD do

- Make **minimal, targeted changes** — touch only what the request requires
- Follow the existing collector pattern when adding a new ATS:
  1. Create `src/collectors/<name>.py` mirroring `greenhouse.py` / `lever.py`
  2. Add a slug-extraction regex in `detect.py::_match_url`
  3. Add the new type to the `if/elif` chain in `main.py::_fetch_company`
  4. Export the class from `src/collectors/__init__.py`
- Add new packages to `requirements.txt` whenever a new import is introduced
- Verify the CLI still works after changes:
  ```bash
  pip install -r requirements.txt
  python -m src.main run --help
  ```
- Prefer `pathlib.Path` over `os.path`
- Use `logger.error(...)` for errors and `logger.debug(...)` for diagnostics
- Open a **branch + PR** for every change — never push directly to `main`

---

## What Claude MUST NOT do

- **Do NOT modify `.github/workflows/`** unless the user explicitly asks to change CI
- **Do NOT read, modify, or create** `.env` files, secret files, or credential stores
- **Do NOT commit** `data/jobs.db`, `data/ats_cache.json`, `output/report.md`,
  or `output/unknown_companies.csv` — these are gitignored runtime artifacts
- **Do NOT refactor** code that is unrelated to the requested change
- **Do NOT add** docstrings, comments, or type annotations to code you didn't change
- **Do NOT push to `main`** — always work on a feature branch
- **Do NOT break** the existing collector interface: `fetch_jobs()` must return `list[Job]`

---

## Preferred scope of changes

Unless the user says otherwise, limit file modifications to:

| Path | What's allowed |
|------|---------------|
| `src/` | All application code |
| `config/companies.csv` | Add/edit company rows |
| `config/filters.yaml` | Add/edit scoring rules |
| `requirements.txt` | Add dependencies only when needed |
| `README.md` | Only when user asks for doc updates |

Changes outside this scope (CI, secrets, infrastructure) require explicit user approval.

---

## How to add a new ATS connector (reference pattern)

```python
# src/collectors/ashby.py  — minimal example
class AshbyCollector:
    source = "ashby"

    def __init__(self, session=None):
        self.session = session or requests.Session()

    def fetch_jobs(self, company_name, ats_slug, *, fetch_descriptions=False):
        url = f"https://api.ashbyhq.com/posting-api/job-board/{ats_slug}"
        resp = self.session.get(url, timeout=10)
        resp.raise_for_status()
        # normalise each posting into Job(...)
        ...
```

Then wire it up in `detect.py` (regex) and `main.py` (`_fetch_company`).
