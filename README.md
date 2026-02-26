# Job Aggregator

ATS-first job aggregation tool that pulls postings from **Greenhouse** and **Lever** career boards, scores them against your filters, and generates a daily Markdown report.

## Quick Start

```bash
# 1. Install dependencies
pip3 install -r requirements.txt

# 2. Run the aggregator
cd job-aggregator
python3 -m src.main run
```

That's it. Results appear in `output/report.md`.

## CLI Options

```bash
python3 -m src.main run \
    --companies config/companies.csv \
    --filters   config/filters.yaml \
    --db        data/jobs.db \
    --report    output/report.md \
    --verbose
```

| Flag | Default | Description |
|------|---------|-------------|
| `--companies` | `config/companies.csv` | Path to your companies list |
| `--filters` | `config/filters.yaml` | Scoring filter config |
| `--db` | `data/jobs.db` | SQLite database path (created automatically) |
| `--report` | `output/report.md` | Output report path |
| `--verbose` / `-v` | off | Debug logging |

## How It Works

1. **Load** companies from CSV
2. **Detect** ATS type per company (Greenhouse / Lever / unknown) using URL patterns and HTML scraping
3. **Fetch** jobs from each supported ATS via their public JSON APIs
4. **Normalize** into a common schema
5. **Store** in SQLite with deduplication (upsert on URL) and first_seen/last_seen tracking
6. **Score** against your keyword filters
7. **Generate** a Markdown report with new jobs, top matches, and unknown-ATS companies

## Adding Companies

Edit `config/companies.csv`. Required columns:

```
company,sector,uk_base,ats_hint,careers_url,notes
```

### ATS Overrides

If the tool can't auto-detect a company's ATS, add two optional columns:

```
company,...,ats_type_override,ats_slug_override
```

Example:

```csv
Stripe,...,greenhouse,stripe
GitLab,...,lever,gitlab
```

The override takes priority over auto-detection.

### How to Find the Slug

- **Greenhouse**: Go to the company's job board. If the URL is `boards.greenhouse.io/stripe`, the slug is `stripe`.
- **Lever**: Go to the company's job board. If the URL is `jobs.lever.co/gitlab`, the slug is `gitlab`.

If the careers page is a custom domain, view the page source and search for `greenhouse.io` or `lever.co` — the slug is in those URLs.

## Configuring Filters

Edit `config/filters.yaml`:

```yaml
include_titles:
  - "data scientist"
  - "product analyst"

exclude_titles:
  - "ml engineer"
  - "platform"

include_keywords:
  - "experimentation"
  - "causal"

exclude_keywords:
  - "phd"
  - "computer vision"
```

**Scoring rules:**
- `+3` for each matching include_titles phrase in the job title
- `-5` for each matching exclude_titles phrase in the job title
- `+1` for each include_keyword found in title + description
- `-2` for each exclude_keyword found in title + description

Jobs with score ≥ 1 appear in the "Top Matches" section.

## Troubleshooting Unknown ATS

The report's "Unknown ATS / Errors" section lists companies that couldn't be matched. To fix:

1. Visit the company's careers page
2. Look for links to `boards.greenhouse.io/<slug>` or `jobs.lever.co/<slug>`
3. Add `ats_type_override` and `ats_slug_override` columns to your CSV
4. If the company uses Workday, SmartRecruiters, or a custom ATS — these aren't supported yet (v2)

## Project Structure

```
job-aggregator/
├── config/
│   ├── companies.csv       # Your target companies
│   └── filters.yaml        # Scoring keywords
├── data/
│   └── jobs.db             # SQLite DB (auto-created)
├── output/
│   └── report.md           # Generated report
├── src/
│   ├── __init__.py
│   ├── __main__.py          # python -m src.main entry
│   ├── main.py              # CLI + pipeline orchestration
│   ├── core/
│   │   ├── models.py        # Job / RunRecord dataclasses
│   │   ├── db.py            # SQLite storage layer
│   │   ├── scoring.py       # Filter-based scoring engine
│   │   ├── reporting.py     # Markdown report generator
│   │   └── utils.py         # CSV/YAML loaders, logging
│   └── collectors/
│       ├── detect.py        # ATS detection heuristics
│       ├── greenhouse.py    # Greenhouse API collector
│       └── lever.py         # Lever API collector
├── requirements.txt
└── README.md
```
