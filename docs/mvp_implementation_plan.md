# FDA Medical Device Regulatory Risk Intelligence Dashboard — MVP Implementation Plan

## Context

This project builds a BI/data product that integrates 4 public openFDA data sources (adverse events, device classification, recalls, 510(k) clearances) into an interactive Tableau Public dashboard. The goal is to identify medical device categories, product codes, and manufacturers with higher observable post-market regulatory exposure. No code exists yet — only the design spec at `docs/project_design.md`.

The pipeline follows a lakehouse pattern: **raw** (JSON/CSV) → **clean** (parquet) → **mart** (parquet) → **app** (CSV for Tableau). Tech stack: Python, pandas, duckdb, pyarrow, requests, rapidfuzz, python-dotenv, tqdm. Time window: 2019-01-01 to 2025-12-31.

---

## Phase 0: Project Scaffolding

| # | Task | Files | Complexity |
|---|------|-------|------------|
| 0.1 | Create directory structure | `src/{__init__,api/,extraction/,cleaning/,mapping/,marts/,qa/}`, `data/{raw,clean,mart,app}/.gitkeep`, `notebooks/`, `tableau/{workbook,export_csv}/`, `tests/` | Simple |
| 0.2 | Create `requirements.txt` | pandas, duckdb, pyarrow, requests, rapidfuzz, python-dotenv, tqdm, jupyter, pytest, ruff | Simple |
| 0.3 | Create `.env.example` and config module | `.env.example` (FDA_API_KEY), `src/config.py` (paths, constants, API base URLs, time window) | Simple |
| 0.4 | Create `pyproject.toml` for linting | ruff config | Simple |

**Verify:** `pip install -r requirements.txt` succeeds; `ruff check .` runs clean; `pytest` discovers test directory.

---

## Phase 1: Source Validation and Extraction

| # | Task | Files | Complexity | Depends |
|---|------|-------|------------|---------|
| 1.1 | API client with rate limiting, pagination, retry, caching | `src/api/client.py` | Medium | 0.3 |
| 1.2 | Adverse event extractor (bulk ZIP preferred, API fallback) | `src/extraction/adverse_events.py` → `data/raw/adverse_events/` | Complex | 1.1 |
| 1.3 | Classification extractor (full API pull, no time filter) | `src/extraction/classification.py` → `data/raw/classification/` | Medium | 1.1 |
| 1.4 | Recall/enforcement extractor (by year) | `src/extraction/recalls.py` → `data/raw/recalls/` | Medium | 1.1 |
| 1.5 | 510(k) extractor (by year) | `src/extraction/clearances.py` → `data/raw/clearances/` | Medium | 1.1 |
| 1.6 | Source validation notebook (small sample, field inventory) | `notebooks/01_source_validation.ipynb` | Medium | 1.2–1.5 |
| 1.7 | Full data extraction notebook | `notebooks/02_data_extraction.ipynb` | Medium | 1.6 |

**Key decisions:**
- Store raw API responses as JSON files (one per page/batch) for reproducibility
- Respect openFDA 240 req/min limit (120/min without key); exponential backoff with 3 retries
- Stream MAUDE bulk files to disk year-by-year to manage memory

**Verify:** `data/raw/` has files for all 4 sources; row counts logged and exceed 2,000 minimum for adverse events; field inventory documents available vs. missing fields.

---

## Phase 2: Cleaning and Core Dimensions

| # | Task | Files | Complexity | Depends |
|---|------|-------|------------|---------|
| 2.1 | Adverse event cleaner — flatten nested arrays to device-level grain, dedup on `mdr_report_key` (keep latest), create seriousness flags | `src/cleaning/adverse_events.py` → `data/clean/clean_event_device_level.parquet` | Complex | 1.7 |
| 2.2 | Classification dimension builder | `src/cleaning/classification.py` → `data/clean/dim_product_code.parquet` | Simple | 1.7 |
| 2.3 | Manufacturer name standardizer — 5-step pipeline (uppercase, punctuation cleanup, legal suffix removal, rapidfuzz clustering at threshold 90, alias table) | `src/mapping/manufacturer.py` → `data/clean/dim_manufacturer_alias.parquet` | Complex | 2.1 |
| 2.4 | QA module foundation (row counts, null rates, dedup reports, coverage checks) | `src/qa/checks.py` | Medium | — |
| 2.5 | Event cleaning notebook | `notebooks/03_event_cleaning.ipynb` | Medium | 2.1–2.4 |

**Key decisions:**
- Each device entry in an adverse event becomes its own row; `event_record_id` = `mdr_report_key` + device index
- Patient outcomes aggregated across all patients → event-level seriousness flags
- Manufacturer fuzzy matching on deduplicated unique names only (reduces millions of rows to ~tens of thousands)
- Manual review CSV for top 200 firms by event count

**Verify:** Parquet files have correct grain (no duplicate keys); dedup count < raw count; `dim_product_code` has unique product codes; manufacturer alias shows consolidation; QA checks pass.

---

## Phase 3: Recall and 510(k) Integration

| # | Task | Files | Complexity | Depends |
|---|------|-------|------------|---------|
| 3.1 | Recall cleaner (dates, recall_class, firm standardization) | `src/cleaning/recalls.py` | Medium | 1.7, 2.3 |
| 3.2 | Recall-to-classification mapper — 4-tier mapping: exact product_code match, high-confidence text match (rapidfuzz >= 85), low-confidence (60–84), unmapped. Only tiers 1–2 in default dashboard view. | `src/mapping/recall_product_code.py` → `data/clean/clean_recall.parquet` | Complex | 3.1, 2.2 |
| 3.3 | 510(k) cleaner (dates, applicant standardization, product code alignment) | `src/cleaning/clearances.py` → `data/clean/clean_510k.parquet` | Medium | 1.7, 2.2, 2.3 |
| 3.4 | Mapping and dimensions notebook (coverage breakdown, join rates) | `notebooks/04_mapping_and_dimensions.ipynb` | Medium | 3.2, 3.3 |

**Key decisions:**
- Recall mapper parses `product_description` and `code_info` fields when `product_code` is missing
- Match score stored alongside `mapping_quality` for auditability
- `include_in_core_dashboard` flag: True for exact + high-confidence only

**Verify:** `clean_recall.parquet` has all 4 mapping tiers; tiers 1+2 coverage > 60%; `clean_510k.parquet` has > 90% product code match rate.

---

## Phase 4: Mart and KPI Construction

| # | Task | Files | Complexity | Depends |
|---|------|-------|------------|---------|
| 4.1 | Mart builder — `build_mart_panel_year()`, `build_mart_product_code_year()`, `build_mart_firm_product_year()` using DuckDB SQL over parquet | `src/marts/builder.py` → `data/mart/*.parquet` | Complex | Phase 2+3 |
| 4.2 | KPI helpers — `events_per_100_clearances()`, `recalls_per_100_clearances()`, `severe_recall_share()`, `firm_share()` with min denominator thresholds | `src/marts/kpis.py` | Simple | — |
| 4.3 | App-layer CSV export — `app_overview`, `app_category_product`, `app_manufacturer`, `app_methodology` | `src/marts/export.py` → `data/app/*.csv` | Medium | 4.1 |
| 4.4 | QA summary generator (refresh-cycle quality report) | `src/qa/summary.py` → `data/app/qa_summary.csv` | Medium | 4.1 |
| 4.5 | Metric building notebook (spot-check KPIs, quality gates) | `notebooks/05_metric_building.ipynb` | Medium | 4.1–4.4 |

**Key decisions:**
- Min denominator: 10 clearances for normalized KPIs (NULL when threshold not met, not zero)
- `severe_recall_share` = Class I / total (only when total >= 3)
- App CSVs pre-aggregated and filtered for Tableau performance: overview < 5K rows, category < 20K rows, manufacturer < 30K rows

**Verify:** Mart tables have correct grain (no duplicate keys); KPIs hand-checked for 3–5 known categories; normalized KPIs return NULL for small denominators; app CSVs appropriately sized.

---

## Phase 5: Dashboard Development and Publishing

| # | Task | Files | Complexity | Depends |
|---|------|-------|------------|---------|
| 5.1 | Dashboard export notebook (final export run) | `notebooks/06_dashboard_export.ipynb` | Simple | 4.3 |
| 5.2 | Tableau: Executive Overview (KPI cards, trend line, top panels, filters) | `tableau/workbook/fda_dashboard.twbx` | Complex | 5.1 |
| 5.3 | Tableau: Category & Product Explorer (scatter plot, Pareto, detail table) | (same workbook) | Complex | 5.2 |
| 5.4 | Tableau: Manufacturer Concentration (top firms, heatmap, profile card) | (same workbook) | Complex | 5.2 |
| 5.5 | Tableau: Methodology & Limitations (coverage cards, caveats) | (same workbook) | Medium | 5.2 |
| 5.6 | Documentation | `docs/methodology.md`, `docs/data_dictionary.md` | Medium | Phase 4 |
| 5.7 | README finalization (summary, screenshot, business questions, reproducibility) | `README.md` | Medium | 5.2–5.6 |
| 5.8 | Dashboard screenshots | `docs/screenshots/` | Simple | 5.2–5.5 |
| 5.9 | Publish to Tableau Public | — | Simple | 5.2–5.5 |

**Verify:** All 4 pages load within 5s; filters work across pages; KPIs match mart values; README is self-contained; methodology page addresses all risks from design spec Section 13.

---

## Top Risks

1. **MAUDE bulk data volume** — multi-GB files. *Mitigation:* stream year-by-year, validate with 1-year sample first.
2. **Recall mapping coverage** — biggest analytical risk. *Mitigation:* invest in text-matching logic; add manual mapping CSV for top 50 categories if automated coverage < 50%.
3. **Manufacturer clustering quality** — false positives/negatives. *Mitigation:* conservative threshold (90+), manual review of top 200 firms.
4. **2025 data incompleteness** — partial year. *Mitigation:* flag as "partial year" in all visualizations.
5. **openFDA API stability** — rate limits, outages. *Mitigation:* retry logic, page-level caching for resumable extraction.

---

## Dependency Graph

```
Phase 0 (Scaffolding)
  → Phase 1 (Extraction): 1.1 API client → 1.2-1.5 extractors (parallel) → 1.6 validation → 1.7 full extraction
    → Phase 2 (Cleaning): 2.2 classification dim (independent) | 2.1 event cleaner → 2.3 manufacturer standardizer → 2.5 notebook
      → Phase 3 (Recall/510k): 3.1 recall cleaner + 3.3 510k cleaner (parallel) → 3.2 recall mapper → 3.4 notebook
        → Phase 4 (Marts): 4.2 KPI helpers (independent) | 4.1 mart builder → 4.3 export + 4.4 QA → 4.5 notebook
          → Phase 5 (Dashboard): 5.1 export → 5.2-5.5 Tableau → 5.6-5.9 docs & publish
```
