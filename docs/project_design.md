# U.S. Medical Device Regulatory Risk Intelligence Dashboard

## 0. Document Purpose

This document reframes the original project plan into a more execution-ready, portfolio-quality business analysis design for an English-speaking audience. The project is positioned as a data product from a Business Analyst perspective: it uses public FDA regulatory data to identify where post-market regulatory exposure appears more concentrated across device categories, product codes, and manufacturers.

The dashboard is not intended to estimate true clinical risk, incidence, or causality. Instead, it measures observable regulatory exposure in public systems, including reported adverse event burden, recall burden, and normalized post-market regulatory pressure.

---

## 1. Project Overview

### 1.1 Project Name

**U.S. Medical Device Regulatory Risk Intelligence Dashboard**

### 1.2 One-Sentence Definition

An interactive business intelligence dashboard that integrates public FDA adverse event, recall, classification, and 510(k) clearance data to identify medical device categories, product codes, and manufacturers with higher observable post-market regulatory exposure.

### 1.3 Core Business Problem

In medical device markets, listing status or clearance status alone does not provide enough insight for downstream business decisions. For market research, category screening, competitive intelligence, and strategic diligence, stakeholders often need to understand where public regulatory pressure appears more concentrated.

This project is designed to answer questions such as:

- Which device categories appear to carry higher observable regulatory burden in public FDA records?
- Which product codes repeatedly show elevated adverse event or recall exposure?
- Which manufacturers appear more concentrated in specific high-exposure categories?
- After a rough normalization by market-entry volume, which categories still look unusually pressured?

### 1.4 Business Value

This dashboard can support several practical use cases:

- **Market screening:** Identify categories with potentially higher regulatory pressure.
- **Competitive intelligence:** Monitor which firms are more exposed in selected product segments.
- **Commercial diligence:** Support category-level discussions around compliance burden and reputational exposure.
- **Portfolio demonstration:** Showcase end-to-end BA capability across data acquisition, metric design, data quality governance, and dashboard storytelling.

### 1.5 Primary Deliverables

The project delivers two public-facing outputs:

1. **An interactive dashboard** for exploration and business storytelling.
2. **A GitHub project page** documenting the business problem, data sources, methodology, limitations, and reproducibility approach.

---

## 2. Project Goals and Success Criteria

### 2.1 Goals

The goal is to build a business-facing regulatory intelligence product that is analytically transparent, easy to explain, and strong enough for both portfolio presentation and interview discussion.

Specific goals:

1. Build a reproducible FDA data ingestion and cleaning pipeline.
2. Define a consistent KPI framework across panel, product code, and manufacturer levels.
3. Launch an interpretable dashboard with clear business narrative.
4. Make the project robust enough to withstand questions about methodology, metric validity, and data limitations.

### 2.2 Success Criteria

#### Data Success Criteria

- At least 2,000 rows of real-world data are ingested and cleaned.
- Data extraction and cleaning logic are reproducible.
- Key joins and mappings are measurable through explicit coverage metrics.
- Core KPIs can be recomputed from source-level transformed tables.

#### Analytical Success Criteria

- The dashboard can clearly identify high-exposure categories, high-attention product codes, and concentrated manufacturer exposure.
- Both absolute burden and normalized burden are available.
- Users can understand why results should not be interpreted as clinical risk rates.

#### Product Success Criteria

- The dashboard structure is coherent and executive-friendly.
- Page load time is acceptable for public demonstration.
- Filters, KPIs, and narrative are aligned.
- The GitHub README is sufficient for a reviewer to understand the project without additional explanation.

#### Portfolio Success Criteria

- The project can be explained in a concise 2-minute version.
- The project can also be defended in a deeper 5-minute walkthrough.
- The rationale behind data choices, KPI design, and product design is easy to justify.

---

## 3. Target Users and Decision Scenarios

### 3.1 Target Users

This product is intended for business-oriented users rather than clinicians or patients.

Primary users include:

- Healthcare industry researchers
- Business analysts and strategy analysts
- Market intelligence or investment research professionals
- Consulting candidates or portfolio reviewers evaluating business problem solving

### 3.2 Decision Scenarios

#### Scenario A: Category Screening
A user wants to compare medical device categories and quickly identify segments with higher visible regulatory burden.

#### Scenario B: Competitor Monitoring
A user wants to inspect a specific panel or product code to see whether exposure is distributed broadly or concentrated in several manufacturers.

#### Scenario C: Entry Risk Review
A user wants a rough normalized view of post-market regulatory pressure using 510(k) clearances as a proxy for market-entry scale.

#### Scenario D: Executive Summary Review
A hiring manager or stakeholder wants a fast answer to: where are the main exposure hotspots, and how confident should we be in the comparisons?

---

## 4. Scope Definition

### 4.1 MVP Scope

The MVP includes:

- Main analysis window: **2019-01-01 to 2025-12-31**
- Four core sources: adverse events, classification, recall/enforcement, and 510(k)
- Three analytical levels: panel, product code, and manufacturer
- Three business dashboard pages plus one methodology page
- A core set of absolute and normalized KPIs
- High-confidence mapping logic for dashboard-facing views

### 4.2 Out of Scope for MVP

The following items are intentionally excluded from Version 1:

- Advanced predictive machine learning models
- Full historical visualization across all years
- PMA, UDI, registration, or ownership-structure expansion
- NLP-based recall reason classification
- Parent-company consolidation across complex corporate entities

### 4.3 MVP Design Principles

The MVP should prioritize:

1. data credibility,
2. metric interpretability,
3. dashboard usability,
4. portfolio readiness.

---

## 5. Data Source Strategy

### 5.1 Core Data Sources

The project uses four FDA public data sources:

1. **openFDA Device Adverse Event**
2. **openFDA Device Classification**
3. **openFDA Device Recall / Enforcement**
4. **openFDA Device 510(k)**

### 5.2 Role of Each Source

#### A. Device Adverse Event
**Role:** Primary post-market surveillance fact table.

**Primary use cases:**
- Adverse event counting
- Trend analysis
- Seriousness-related feature construction
- Manufacturer and product-code linkage

#### B. Device Classification
**Role:** Core dimension table for product code, class, and panel structure.

**Primary use cases:**
- Device category hierarchy
- Panel and class-based slicing
- Product code standardization

#### C. Device Recall / Enforcement
**Role:** Secondary fact table capturing recall burden.

**Primary use cases:**
- Recall count tracking
- Recall severity mix analysis
- Product and manufacturer exposure review

#### D. Device 510(k)
**Role:** Rough proxy denominator for market-entry scale.

**Primary use cases:**
- Clearance count estimation
- Approximate normalization of post-market burden
- Entry-to-post-market comparison

### 5.3 Data Use Principles

- Adverse event data is the main fact base.
- Classification data is the core dimensional backbone.
- Recall data provides an additional regulatory burden view.
- 510(k) data is used only as an approximate denominator, not a market-size measure.
- All outputs must include limitation language to prevent overclaiming.

### 5.4 Time Window Rationale

The primary window is **2019-01-01 to 2025-12-31** because it:

- focuses on recent regulatory patterns,
- provides sufficient scale for analysis and visualization,
- avoids unnecessary distortion from older schema inconsistencies,
- supports a more modern and portfolio-relevant narrative.

### 5.5 Data Retrieval Strategy

#### Adverse Events
Use downloadable bulk files where possible for baseline ingestion, then use API calls for validation or incremental refresh.

#### Classification
Use API-based full extraction.

#### Recall / Enforcement
Extract by year and cache raw responses.

#### 510(k)
Extract by year and cache raw responses.

### 5.6 Critical Data Caveat

These sources describe **public regulatory records**, not true incidence, prevalence, sales volume, installed base, or real-world exposure. Therefore, this dashboard measures **observable regulatory exposure**, not actual clinical risk.

---

## 6. Technology Stack and Delivery Architecture

### 6.1 Technology Principles

Technology choices should prioritize:

- speed to execution,
- reproducibility,
- transparency,
- public portfolio presentation.

### 6.2 Recommended Stack

#### Data Extraction and Processing
- Python
- requests
- pandas
- duckdb
- pyarrow
- rapidfuzz
- tqdm
- python-dotenv

#### Data Storage
- **Raw layer:** JSON / CSV
- **Clean layer:** Parquet
- **Mart layer:** Parquet or CSV for dashboard export

#### Visualization and Publishing
- Tableau Public for the main dashboard
- GitHub for repository management
- GitHub Pages for project landing page and documentation

### 6.3 Suggested Repository Structure

```text
medical-device-risk-dashboard/
  README.md
  requirements.txt
  .env.example
  data/
    raw/
    clean/
    mart/
  notebooks/
    01_source_validation.ipynb
    02_data_extraction.ipynb
    03_event_cleaning.ipynb
    04_mapping_and_dimensions.ipynb
    05_metric_building.ipynb
    06_dashboard_export.ipynb
  src/
    api/
    extraction/
    cleaning/
    mapping/
    marts/
    qa/
  tableau/
    workbook/
    export_csv/
  docs/
    methodology.md
    data_dictionary.md
    dashboard_spec.md
```

---

## 7. Data Model Design

### 7.1 Layered Model

The data model should be structured into four layers:

1. **Raw:** source extracts with minimal modification
2. **Clean:** standardized, quality-controlled tables
3. **Mart:** business-ready analytical tables with recomputable KPIs
4. **App:** light dashboard-facing extracts optimized for Tableau

### 7.2 Core Fact and Dimension Tables

#### Fact Table 1: `clean_event_device_level`
**Grain:** one device-level adverse event record.

**Purpose:**
- adverse event counting,
- seriousness aggregation,
- product and manufacturer linkage.

**Candidate fields:**
- event_record_id
- mdr_report_key
- date_received
- event_year
- product_code
- brand_name
- generic_name
- manufacturer_std
- event_type
- adverse_event_flag
- product_problem_flag
- has_death
- has_serious_injury
- has_malfunction
- source_type
- remedial_action_flag
- followup_rank
- is_latest_version

#### Fact Table 2: `clean_recall`
**Grain:** one recall record.

**Purpose:**
- recall counting,
- severity-level analysis,
- product and manufacturer exposure assessment.

**Candidate fields:**
- recall_number
- recall_initiation_date
- recall_year
- recall_class
- product_code
- product_description
- recalling_firm_std
- reason_for_recall
- status
- voluntary_flag
- mapping_quality
- include_in_core_dashboard

#### Fact Table 3: `clean_510k`
**Grain:** one 510(k) clearance record.

**Purpose:**
- approximate denominator construction,
- market-entry volume tracking.

**Candidate fields:**
- k_number
- decision_date
- decision_year
- product_code
- applicant_std
- advisory_committee
- decision_lag_days

#### Dimension Table 1: `dim_product_code`
**Purpose:** category backbone for product-level analysis.

**Candidate fields:**
- product_code
- device_name
- medical_specialty
- medical_specialty_description
- review_panel
- device_class
- implant_flag
- life_sustain_support_flag
- regulation_number

#### Dimension Table 2: `dim_manufacturer_alias`
**Purpose:** manufacturer standardization and confidence tracking.

**Candidate fields:**
- raw_name
- standardized_name
- normalization_rule
- confidence_level
- manual_review_flag

### 7.3 Mart Tables

#### `mart_panel_year`
**Grain:** panel-year

**Key metrics:**
- event_count_raw
- event_count_dedup
- death_related_reports
- serious_injury_reports
- malfunction_reports
- recall_count
- class_i_recall_count
- class_ii_recall_count
- class_iii_recall_count
- clearance_count
- events_per_100_clearances
- recalls_per_100_clearances
- severe_recall_share

#### `mart_product_code_year`
**Grain:** product_code-year

**Key metrics:**
- event_count_dedup
- recall_count
- class_i_recall_count
- clearance_count
- events_per_100_clearances
- recalls_per_100_clearances
- recall_to_event_ratio
- severe_recall_share

#### `mart_firm_product_year`
**Grain:** manufacturer-product_code-year

**Key metrics:**
- event_count_dedup
- recall_count
- severe_recall_count
- firm_share_within_product
- firm_share_within_panel

### 7.4 Dashboard-Facing App Tables

#### `app_overview`
For executive summary metrics and trend visuals.

#### `app_category_product`
For panel and product exploration.

#### `app_manufacturer`
For manufacturer concentration analysis.

#### `app_methodology`
For coverage, mapping confidence, and methodological caveats.

---

## 8. Data Cleaning and Quality Governance

### 8.1 Adverse Event Cleaning

#### Structural Handling
Because adverse event records contain nested device and patient information, the main analytical grain should be device-level to maximize linkage feasibility with product-code classification.

#### Deduplication Strategy
- Use `mdr_report_key` as a report-level key candidate.
- When multiple versions exist for the same report, retain only the latest version for deduplicated reporting.
- Preserve both raw and deduplicated counts for transparency.

#### Seriousness Features
Convert patient-level outcomes into dashboard-friendly binary indicators such as:
- `has_death`
- `has_serious_injury`
- `has_malfunction`

### 8.2 Recall Cleaning

#### Mapping Strategy
Recall-to-classification linkage should not assume full coverage. Mapping quality must be explicitly stored and surfaced.

Suggested mapping classes:
- `exact_product_code_match`
- `high_confidence_text_match`
- `low_confidence_text_match`
- `unmapped`

Only the first two classes should be included in the default dashboard view.

#### Manufacturer Standardization
Standardize firm names using a rule-based pipeline:
1. uppercase normalization,
2. punctuation and whitespace cleanup,
3. legal suffix removal,
4. alias table creation,
5. manual review for frequent firms.

### 8.3 510(k) Cleaning

Primary steps:
- normalize `decision_date`,
- derive `decision_year`,
- standardize applicant names,
- align product codes with the classification dimension.

### 8.4 Quality-Control Requirements

Every refresh cycle should output a QA summary covering:

- raw row count vs cleaned row count,
- difference between raw and deduplicated event counts,
- missing product-code rate,
- recall mapping coverage,
- manufacturer normalization coverage,
- unusual yearly volume shifts,
- outlier checks by panel and product code.

### 8.5 Critical BA Quality Gates

The following should be treated as go/no-go thresholds for dashboard publication:

- minimum recall mapping coverage threshold,
- minimum manufacturer normalization coverage threshold,
- validation that top-ranked outputs are not driven mainly by unmapped or low-confidence records,
- validation that normalized KPI rankings are not dominated by extremely small denominator categories without thresholding.

---

## 9. KPI Framework

### 9.1 KPI Design Principles

KPIs must be:

- understandable for non-technical users,
- comparable across categories,
- transparent about limitations,
- usable in ranking, filtering, and narrative explanation.

### 9.2 Core KPIs

#### Absolute Burden Metrics
- Deduplicated adverse event count
- Recall count
- Class I recall count
- Serious injury related report count
- Death related report count

#### Normalized Metrics
- Events per 100 clearances
- Recalls per 100 clearances
- Recall-to-event ratio

#### Structural Metrics
- Severe recall share
- Firm share within product code
- Firm share within panel

### 9.3 KPI Interpretation Rules

#### Why both absolute and normalized views are required
Absolute counts favor large categories, while normalized metrics may exaggerate small categories with thin denominators. Both perspectives must be shown together to support balanced interpretation.

#### Why 510(k) is used as a denominator
510(k) clearance volume does not represent installed base, patient utilization, or actual unit exposure. It is used only as a rough market-entry proxy where no better public denominator is consistently available.

### 9.4 Composite Score Policy

The first release should **not** rely primarily on a single composite score.

Recommended policy:
- **V1:** show core KPIs only.
- **V2:** optionally introduce a `Regulatory Exposure Score` for exploratory sorting.
- Any composite score must be labeled as an exploratory heuristic rather than a true risk rating.

### 9.5 Recommended Metric Safeguards

- Apply minimum denominator thresholds before ranking normalized KPIs.
- Label normalized metrics clearly as proxy-normalized rather than true rates.
- Allow toggling between raw and deduplicated event views when useful.
- Expose data coverage or confidence notes on pages with ranked outputs.

---

## 10. Analytical Design

### 10.1 Analysis A: Panel-Level Overview
**Question:** Which device panels show the highest observable burden over time?

**Outputs:**
- annual panel trend charts,
- panel ranking visuals,
- severe recall share comparisons.

### 10.2 Analysis B: Product Code Deep Dive
**Question:** Which product codes look important under both absolute and normalized views?

**Outputs:**
- product-code scatter plots,
- Pareto-style ranking views,
- detailed comparison tables.

### 10.3 Analysis C: Manufacturer Concentration
**Question:** Is exposure spread broadly across a category, or concentrated in a small number of firms?

**Outputs:**
- top firms within selected categories,
- firm-by-product matrices,
- concentration summaries.

### 10.4 Analysis D: Entry-to-Post-Market Comparison
**Question:** After rough normalization by clearance volume, which categories still appear pressured?

**Outputs:**
- events per 100 clearances scatter plots,
- recall burden vs clearance count comparisons,
- category watchlist logic.

### 10.5 Optional Analysis E: Confidence Review
**Question:** How much of the result depends on high-confidence linkage and standardized entities?

**Outputs:**
- mapping coverage cards,
- confidence-segmented summary tables,
- methodology-side warnings.

---

## 11. Dashboard Product Design

### 11.1 Dashboard Structure

The dashboard should contain four pages:

1. **Executive Overview**
2. **Category & Product Explorer**
3. **Manufacturer Concentration View**
4. **Methodology & Limitations**

### 11.2 Recommended Story Flow

The dashboard should follow a business decision narrative:

1. Where is the visible regulatory exposure highest?
2. Which product segments remain notable after rough normalization?
3. Is exposure concentrated in a few firms or spread across the category?
4. What are the main caveats and confidence limits?

### 11.3 Page 1: Executive Overview
**Goal:** Provide the core story within 30 seconds.

**Suggested modules:**
- KPI cards
- 2019–2025 event and recall trend line
- top panels ranking chart
- top product-code summary panel
- global filters: year, panel, device class

**Key takeaway questions:**
- Which categories show the highest overall burden?
- How has the burden evolved over time?
- Which categories deserve deeper inspection?

### 11.4 Page 2: Category & Product Explorer
**Goal:** Support structured comparison at finer granularity.

**Suggested modules:**
- panel ranking chart
- product-code scatter plot
- Pareto or treemap view
- product-level detail table

**Key takeaway questions:**
- Which product codes sit in high-burden and high-severity zones?
- Within a panel, which subsegments deserve attention?

### 11.5 Page 3: Manufacturer Concentration View
**Goal:** Determine whether category burden is concentrated or diffuse.

**Suggested modules:**
- top-firm chart
- concentration summary chart
- firm × product matrix
- selected manufacturer profile card

**Key takeaway questions:**
- Are recalls and adverse events concentrated in several firms?
- Are there category-specific manufacturer hotspots?

### 11.6 Page 4: Methodology & Limitations
**Goal:** Improve credibility and prevent misuse.

**Suggested modules:**
- data-source summary
- time coverage
- deduplication logic
- mapping coverage
- denominator caveats
- interpretation boundaries for MAUDE and recall data

---

## 12. Filters and Interaction Rules

### 12.1 Global Filters

Recommended standard filters:

- year
- medical specialty / review panel
- device class
- implant flag
- life-sustain-support flag

### 12.2 Default Rules

- Default time window: **2019–2025**
- Default recall view: **high-confidence mapped records only**
- Default event view: **deduplicated counts**
- Default ranking views: **minimum sample threshold applied**

### 12.3 Interaction Principles

- Overview page should orient the user.
- Explorer page should support comparison and investigation.
- Manufacturer page should explain concentration structure.
- Methodology page should build trust and prevent overreading.

---

## 13. Risks and Mitigation Plan

### 13.1 Risk 1: Incomplete Recall Mapping
**Impact:** Distorts category and manufacturer comparisons.

**Mitigation:**
- retain `mapping_quality`,
- default to high-confidence mappings only,
- disclose coverage on the methodology page,
- explicitly state that unmapped records do not imply no risk.

### 13.2 Risk 2: Denominator Misinterpretation
**Impact:** Users may treat normalized metrics as true risk rates.

**Mitigation:**
- avoid the word `rate` in dashboard copy where possible,
- label the metric as `per 100 clearances`,
- explain clearly that 510(k) is only a rough proxy for market-entry volume.

### 13.3 Risk 3: Incomplete Manufacturer Standardization
**Impact:** Understates concentration and mis-splits exposure.

**Mitigation:**
- manually review high-frequency manufacturers,
- maintain an alias rule table,
- publish normalization coverage.

### 13.4 Risk 4: Dashboard Performance Issues
**Impact:** Weakens usability in interviews and public sharing.

**Mitigation:**
- pre-aggregate app-layer tables,
- reduce unnecessary detail columns,
- keep drill-down data separate from default views,
- limit default row volume.

### 13.5 Risk 5: Overinterpretation by Viewers
**Impact:** Users may infer true clinical safety rankings.

**Mitigation:**
- include repeated limitation language,
- place confidence notes near ranked lists,
- separate `observable regulatory exposure` from `clinical risk` in wording.

---

## 14. Implementation Plan

### 14.1 Phase 1: Source Validation and Extraction
**Goal:** Validate field availability, extraction stability, and time-window feasibility.

**Outputs:**
- source validation notebook,
- raw data sample,
- source field inventory.

**Acceptance criteria:**
- extraction scripts run successfully,
- key source fields required for analysis are confirmed,
- time-window coverage is sufficient.

### 14.2 Phase 2: Cleaning and Core Dimensions
**Goal:** Build the event base table, product dimension, and manufacturer standardization layer.

**Outputs:**
- `clean_event_device_level`
- `dim_product_code`
- `dim_manufacturer_alias`

**Acceptance criteria:**
- deduplication logic is stable,
- product-code coverage is measured,
- manufacturer normalization process is documented.

### 14.3 Phase 3: Recall and 510(k) Integration
**Goal:** Add recall burden and denominator logic.

**Outputs:**
- `clean_recall`
- `clean_510k`
- mapping coverage summary

**Acceptance criteria:**
- recall mapping quality is classified,
- 510(k) linkage is usable at product and panel levels,
- denominator caveats are documented.

### 14.4 Phase 4: Mart and KPI Construction
**Goal:** Build business-ready analytical marts.

**Outputs:**
- `mart_panel_year`
- `mart_product_code_year`
- `mart_firm_product_year`
- KPI dictionary

**Acceptance criteria:**
- KPIs are reproducible,
- definitions are consistent across pages,
- ranking logic is threshold-controlled.

### 14.5 Phase 5: Dashboard Development and Publishing
**Goal:** Publish a portfolio-ready dashboard and repository.

**Outputs:**
- Tableau workbook
- dashboard screenshots
- README
- methodology document
- project landing page

**Acceptance criteria:**
- dashboard story flow is coherent,
- filters behave consistently,
- README and dashboard language match,
- limitations are clearly visible.

---

## 15. GitHub and Portfolio Packaging

### 15.1 README Must Answer These Questions

1. What business problem does this project solve?
2. Why are these data sources appropriate for the question?
3. How are the KPIs constructed?
4. How should users interpret the outputs correctly?

### 15.2 Recommended Repository Homepage Structure

- one-sentence project summary,
- dashboard preview image,
- business questions,
- data sources,
- methodology,
- key insights,
- limitations,
- technology stack,
- reproducibility notes.
---

## 16. Execution Priorities

At all times, execution priority should be:

1. **Data credibility first**
2. **Metric interpretability second**
3. **Dashboard polish third**

This ordering is important because a visually strong dashboard without defensible data logic will not hold up in interviews or stakeholder review.

---

## 17. Final Positioning Statement

This project should be presented not as a clinical safety study, but as a **regulatory risk intelligence product** for business users. Its value comes from translating fragmented FDA public records into an interpretable decision-support view of where observable regulatory burden appears concentrated.

That positioning makes the project stronger for BA, strategy, healthcare analytics, and market intelligence applications because it demonstrates not only data handling and visualization skills, but also sound business framing, metric discipline, and limitation-aware analysis.

---

## 18. Official Source References

- openFDA Device Adverse Event: https://open.fda.gov/apis/device/event/
- openFDA Device Classification: https://open.fda.gov/apis/device/classification/
- openFDA Device Recall / Enforcement: https://open.fda.gov/apis/device/enforcement/
- openFDA Device 510(k): https://open.fda.gov/apis/device/510k/
- FDA MAUDE downloadable files: https://www.fda.gov/medical-devices/medical-device-reporting-mdr-how-report-medical-device-problems/mdr-data-files
