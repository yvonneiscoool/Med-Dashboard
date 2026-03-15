# App CSV Alignment with Project Design Spec

**Date:** 2026-03-15
**Status:** Draft
**Context:** Verification of exported app CSVs against `docs/project_design.md` revealed three alignment issues.

---

## Problem Statement

The pipeline runs end-to-end and produces four app CSV files, but their content diverges from the project design spec in three ways:

1. **Year range leakage** — Mart tables and app CSVs contain data from years outside the spec's 2019–2025 analysis window (years 1900, 1991–1999, 2026). This happens because the mart builder applies no year filter; adverse event cleaned data spans all historical years while recalls/clearances are already scoped to 2019–2025 during extraction.

2. **Sparse `app_methodology`** — Only 11 rows with basic source counts and date ranges. The design spec (sections 7.4, 8.4, 11.6) calls for deduplication coverage, recall mapping quality breakdown, manufacturer normalization coverage, and analysis window metadata.

3. **Null `review_panel` in `app_overview`** — 8 rows with null panel names from events whose product codes have no match in `dim_product_code`. These would render as blanks in Tableau.

---

## Design

### 1. Year Range Filtering via Config Constants

**Goal:** Ensure all mart tables and downstream app CSVs contain only 2019–2025 data.

**`src/config.py`:**
- Add two constants derived from existing `DATE_START`/`DATE_END`:
  ```python
  ANALYSIS_YEAR_MIN = 2019
  ANALYSIS_YEAR_MAX = 2025
  ```

**`src/marts/builder.py` — `_register_views()`:**
- Accept `year_min` and `year_max` parameters (defaulting to the config constants).
- Modify view definitions to filter at registration time:
  ```sql
  CREATE VIEW events AS
    SELECT * FROM read_parquet('{events_path}')
    WHERE event_year BETWEEN {year_min} AND {year_max}

  CREATE VIEW recalls AS
    SELECT * FROM read_parquet('{recalls_path}')
    WHERE recall_year BETWEEN {year_min} AND {year_max}

  CREATE VIEW clearances AS
    SELECT * FROM read_parquet('{clearances_path}')
    WHERE decision_year BETWEEN {year_min} AND {year_max}
  ```
- The existing `dim_pc` view registration is preserved unchanged (no year filter needed on dimension table).
- Each builder function passes the config constants through to `_register_views()`.
- Individual SQL queries within the builders do not need year filter changes.

**`src/marts/builder.py` — `build_mart_firm_product_year()`:**
- Currently this function creates its own inline views instead of calling `_register_views()`. Refactor it to use `_register_views()` so it inherits the year filter. Since this builder does not use clearances, make the `clearances_path` parameter optional in `_register_views()` — when `None`, skip creating the clearances view.

**Impact:** All three mart parquets (`mart_panel_year`, `mart_product_code_year`, `mart_firm_product_year`) and all four app CSVs will automatically contain only 2019–2025 data.

### 2. Enriched `app_methodology`

**Goal:** Provide the methodology metrics called for in the design spec (sections 8.4, 11.6).

**`src/marts/export.py` — `export_app_methodology()`:**

Add the following rows to the existing source/metric/value table (existing metrics retained):

| source | metric | computation |
|---|---|---|
| `adverse_events` | `raw_count` | Total rows in clean events parquet |
| `adverse_events` | `dedup_count` | Rows where `is_latest_version == True` |
| `adverse_events` | `duplicate_rate_pct` | `(1 - dedup/raw) * 100` — percentage of records that were duplicates |
| `adverse_events` | `missing_product_code_pct` | `null product_code / total * 100` |
| `adverse_events` | `manufacturer_fill_rate_pct` | `non-null manufacturer_d_name / total * 100` |
| `recalls` | `mapping_quality_exact` | Count where `mapping_quality == 'exact_product_code_match'` |
| `recalls` | `mapping_quality_high` | Count where `mapping_quality == 'high_confidence_text_match'` |
| `recalls` | `mapping_quality_low` | Count where `mapping_quality == 'low_confidence_text_match'` |
| `recalls` | `mapping_quality_unmapped` | Count where `mapping_quality == 'unmapped'` |
| `recalls` | `core_dashboard_coverage_pct` | `include_in_core_dashboard / total * 100` |
| `analysis_window` | `year_min` | 2019 |
| `analysis_window` | `year_max` | 2025 |

The function reads from the clean parquets (not the filtered marts) to give full-picture coverage statistics.

### 3. Null Review Panel Handling

**Goal:** Remove blank panel rows from `app_overview`.

**`src/marts/builder.py` — `build_mart_panel_year()`:**
- Add `AND d.review_panel IS NOT NULL` to the events, recalls, and clearances SQL WHERE clauses. The inner join on `product_code` does not exclude rows where `review_panel` is NULL within `dim_pc`, so an explicit filter is needed on all three queries.

**Not affected:** `mart_product_code_year` and `mart_firm_product_year` do not group by panel at their grain, so unmapped-panel events remain captured there.

---

## Files Modified

| File | Change |
|---|---|
| `src/config.py` | Add `ANALYSIS_YEAR_MIN`, `ANALYSIS_YEAR_MAX` |
| `src/marts/builder.py` | Filter views by year range, drop null review_panel |
| `src/marts/export.py` | Enrich `export_app_methodology()` with additional metrics |

## Files Not Modified

- Clean layer (`src/cleaning/`) — no changes needed
- Dimension tables — no changes needed
- Other mart builders beyond view filtering — no changes needed
- Export functions for overview, category_product, manufacturer — no changes needed (they inherit the fix from filtered marts)

---

## Verification Plan

After implementation, re-run the pipeline and verify:

1. All mart parquets contain only years 2019–2025
2. All app CSVs contain only years 2019–2025
3. `app_overview` has no null `review_panel` rows
4. `app_methodology` contains all specified metrics
5. Null rates on normalized metrics (events_per_100_clearances, etc.) are substantially reduced
6. Existing tests pass
7. Add/update unit tests for `export_app_methodology()` asserting all expected metric names are present and values are non-null
8. Row counts are reasonable (no accidental data loss beyond expected out-of-range filtering)
