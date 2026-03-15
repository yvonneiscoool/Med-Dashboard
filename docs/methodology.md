# Methodology & Limitations

## Data Sources

This dashboard uses four public FDA data sources, covering **2019-01-01 to 2025-12-31**:

| Source | Role | Primary Key | Records |
|--------|------|-------------|---------|
| openFDA Device Adverse Events (MAUDE) | Primary post-market surveillance fact table | `mdr_report_key` | Varies by extraction |
| openFDA Device Classification | Product code dimension and category hierarchy | `product_code` | ~7,000 codes |
| openFDA Device Recall/Enforcement | Secondary fact table for recall burden | `recall_number` | Varies by extraction |
| openFDA Device 510(k) Clearances | Approximate market-entry denominator | `k_number` | Varies by extraction |

**Important:** These sources describe public regulatory records, not true incidence, prevalence, sales volume, or installed base. This dashboard measures **observable regulatory exposure**, not actual clinical risk.

## Data Processing

### Adverse Event Deduplication

MAUDE records may contain multiple versions of the same report. Our pipeline:

1. Flattens nested device arrays to device-level grain (one row per device per report).
2. Assigns `event_record_id` = `mdr_report_key` + device index.
3. Ranks versions by report date; retains only the latest version (`is_latest_version = True`) for deduplicated counts.
4. Preserves both raw and deduplicated counts for transparency.

### Seriousness Classification

Patient outcomes are aggregated to event-level binary flags:

- **`has_death`** — at least one patient outcome indicates death
- **`has_serious_injury`** — at least one outcome indicates serious injury (life-threatening, hospitalization, disability)
- **`has_malfunction`** — device malfunction reported

### Recall Mapping

Recall records do not always carry a product code. Our 4-tier mapping strategy:

| Tier | Method | Dashboard Inclusion |
|------|--------|-------------------|
| 1 — Exact match | `product_code` field matches classification table | Yes (default) |
| 2 — High-confidence text match | rapidfuzz score >= 85 against product descriptions | Yes (default) |
| 3 — Low-confidence text match | rapidfuzz score 60-84 | No |
| 4 — Unmapped | No viable match found | No |

Only Tiers 1 and 2 are included in the default dashboard view. Mapping quality is stored per record for auditability.

### Manufacturer Standardization

Manufacturer names are standardized via a 5-step pipeline:

1. Uppercase normalization
2. Punctuation and whitespace cleanup
3. Legal suffix removal (LLC, INC, CORP, etc.)
4. rapidfuzz clustering (threshold >= 90) for alias detection
5. Manual review for top firms by event volume

### 510(k) as Denominator

510(k) clearance volume is used as a rough proxy for market-entry scale. It does **not** represent:

- Installed base or units in use
- Patient exposure or utilization volume
- Actual market size or revenue

Normalized metrics (e.g., "events per 100 clearances") should be interpreted as rough comparisons, not true rates.

## KPI Framework

### Absolute Burden Metrics

| KPI | Definition |
|-----|-----------|
| Deduplicated adverse event count | Count of latest-version device-level event records |
| Recall count | Count of high-confidence-mapped recall records |
| Class I recall count | Count of most-severe (Class I) recalls |
| Death-related reports | Events where at least one patient death was reported |
| Serious injury reports | Events with life-threatening, hospitalization, or disability outcomes |

### Normalized Metrics

| KPI | Formula | Minimum Denominator |
|-----|---------|-------------------|
| Events per 100 clearances | (event_count_dedup / clearance_count) * 100 | 10 clearances |
| Recalls per 100 clearances | (recall_count / clearance_count) * 100 | 10 clearances |
| Recall-to-event ratio | recall_count / event_count_dedup | 1 event |

### Structural Metrics

| KPI | Formula | Minimum Denominator |
|-----|---------|-------------------|
| Severe recall share | class_i_recall_count / recall_count | 3 total recalls |
| Firm share within product code | firm events / total events for that product_code + year | 1 event |
| Firm share within panel | firm events / total events for that panel + year | 1 event |

### Why Both Absolute and Normalized Views Are Required

Absolute counts favor large categories (more devices = more reports). Normalized metrics may exaggerate small categories with thin denominators. Both perspectives must be shown together for balanced interpretation.

### Safeguards

- Normalized KPIs return NULL (not zero) when denominators fall below minimum thresholds.
- Rankings exclude categories below minimum thresholds.
- All normalized metrics are labeled as "proxy-normalized" rather than true rates.
- 2025 data is flagged as a partial year in all visualizations.

## Known Limitations

1. **Reporting bias:** MAUDE is a passive surveillance system. Not all adverse events are reported, and reporting rates vary by device type, manufacturer, and time period.

2. **No causality:** An adverse event report does not establish that the device caused the outcome. Reports reflect association, not causation.

3. **Denominator limitations:** 510(k) clearance volume is not equivalent to devices in use. Categories with many legacy devices (cleared pre-2019) will appear under-counted in the denominator.

4. **Recall mapping gaps:** Not all recalls could be mapped to product codes. Unmapped recalls are excluded from category-level analysis, which may understate recall burden for some categories.

5. **Manufacturer standardization limits:** Corporate subsidiaries, acquisitions, and name variations mean some manufacturer groupings may be incomplete. Concentration metrics may understate true concentration.

6. **Partial year data:** The most recent year (2025) may contain incomplete data depending on when extraction was performed.

7. **No composite score:** V1 does not include a composite "risk score." Individual KPIs should be evaluated independently rather than combined into a single ranking.
