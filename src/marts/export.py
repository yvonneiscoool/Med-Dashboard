"""App-layer CSV export for Tableau dashboard consumption.

Reads mart parquets, enriches with dimension data, writes CSVs to data/app/.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.config import ANALYSIS_YEAR_MAX, ANALYSIS_YEAR_MIN, DATA_APP, DATA_CLEAN, DATA_MART

# ── Default paths ─────────────────────────────────────────────────────────────
_MART_PANEL_YEAR = DATA_MART / "mart_panel_year.parquet"
_MART_PC_YEAR = DATA_MART / "mart_product_code_year.parquet"
_MART_FIRM_PRODUCT_YEAR = DATA_MART / "mart_firm_product_year.parquet"
_DIM_PC_PATH = DATA_CLEAN / "dim_product_code.parquet"
_EVENTS_PATH = DATA_CLEAN / "clean_event_device_level.parquet"
_RECALLS_PATH = DATA_CLEAN / "clean_recall.parquet"
_CLEARANCES_PATH = DATA_CLEAN / "clean_510k.parquet"

_MAX_CATEGORY_ROWS = 20_000
_MAX_MANUFACTURER_ROWS = 30_000


def export_app_overview(
    mart_panel_year_path: str | Path | None = None,
    output_path: str | Path | None = None,
) -> pd.DataFrame:
    """Export panel-year overview for Tableau. Sorted pass-through of mart."""
    mart_path = Path(mart_panel_year_path) if mart_panel_year_path else _MART_PANEL_YEAR
    output_path = Path(output_path) if output_path else DATA_APP / "app_overview.csv"

    df = pd.read_parquet(mart_path)
    # Label empty/missing review_panel as "Unknown"
    df["review_panel"] = df["review_panel"].fillna("").str.strip()
    df.loc[df["review_panel"] == "", "review_panel"] = "Unknown"
    df = df.sort_values(["review_panel", "year"]).reset_index(drop=True)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    return df


def export_app_category_product(
    mart_pc_year_path: str | Path | None = None,
    dim_product_code_path: str | Path | None = None,
    output_path: str | Path | None = None,
) -> pd.DataFrame:
    """Export product-code-year with dimension enrichment for Tableau."""
    mart_path = Path(mart_pc_year_path) if mart_pc_year_path else _MART_PC_YEAR
    dim_path = Path(dim_product_code_path) if dim_product_code_path else _DIM_PC_PATH
    output_path = Path(output_path) if output_path else DATA_APP / "app_category_product.csv"

    df = pd.read_parquet(mart_path)
    # Filter out junk product codes (e.g. "-", "---")
    df = df[~df["product_code"].str.fullmatch(r"-+", na=False)]
    dim = pd.read_parquet(
        dim_path,
        columns=[
            "product_code",
            "device_name",
            "review_panel",
            "device_class",
            "medical_specialty_description",
        ],
    )
    df = df.merge(dim, on="product_code", how="left")

    # Filter to top rows by event volume if exceeds limit
    if len(df) > _MAX_CATEGORY_ROWS:
        df = df.nlargest(_MAX_CATEGORY_ROWS, "event_count_dedup")

    df = df.sort_values(["product_code", "year"]).reset_index(drop=True)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    return df


def export_app_manufacturer(
    mart_firm_product_year_path: str | Path | None = None,
    dim_product_code_path: str | Path | None = None,
    output_path: str | Path | None = None,
) -> pd.DataFrame:
    """Export manufacturer-product-year with dimension enrichment for Tableau."""
    mart_path = Path(mart_firm_product_year_path) if mart_firm_product_year_path else _MART_FIRM_PRODUCT_YEAR
    dim_path = Path(dim_product_code_path) if dim_product_code_path else _DIM_PC_PATH
    output_path = Path(output_path) if output_path else DATA_APP / "app_manufacturer.csv"

    df = pd.read_parquet(mart_path)
    # Replace empty/missing manufacturer with "Unknown"
    df["manufacturer"] = df["manufacturer"].fillna("").str.strip()
    df.loc[df["manufacturer"] == "", "manufacturer"] = "Unknown"
    dim = pd.read_parquet(dim_path, columns=["product_code", "device_name", "review_panel"])
    df = df.merge(dim, on="product_code", how="left")

    # Filter top N firms per product_code if too many rows
    if len(df) > _MAX_MANUFACTURER_ROWS:
        df = df.nlargest(_MAX_MANUFACTURER_ROWS, "event_count_dedup")

    df = df.sort_values(["manufacturer", "product_code", "year"]).reset_index(drop=True)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    return df


def export_app_methodology(
    events_path: str | Path | None = None,
    recalls_path: str | Path | None = None,
    clearances_path: str | Path | None = None,
    dim_product_code_path: str | Path | None = None,
    output_path: str | Path | None = None,
) -> pd.DataFrame:
    """Export methodology/source stats table for Tableau transparency sheet."""
    events_path = Path(events_path) if events_path else _EVENTS_PATH
    recalls_path = Path(recalls_path) if recalls_path else _RECALLS_PATH
    clearances_path = Path(clearances_path) if clearances_path else _CLEARANCES_PATH
    dim_path = Path(dim_product_code_path) if dim_product_code_path else _DIM_PC_PATH
    output_path = Path(output_path) if output_path else DATA_APP / "app_methodology.csv"

    rows = []

    # Source stats
    for name, path, date_col in [
        ("adverse_events", events_path, "date_received"),
        ("recalls", recalls_path, "recall_initiation_date"),
        ("clearances", clearances_path, "decision_date"),
    ]:
        df = pd.read_parquet(path)
        row = {
            "source": name,
            "metric": "row_count",
            "value": len(df),
        }
        rows.append(row)

        if date_col in df.columns and len(df) > 0:
            dates = pd.to_datetime(df[date_col], errors="coerce").dropna()
            if len(dates) > 0:
                rows.append({"source": name, "metric": "date_min", "value": str(dates.min().date())})
                rows.append({"source": name, "metric": "date_max", "value": str(dates.max().date())})

    # Dimension coverage
    dim_df = pd.read_parquet(dim_path)
    rows.append(
        {
            "source": "dim_product_code",
            "metric": "row_count",
            "value": len(dim_df),
        }
    )

    # Product code coverage in events
    if events_path.exists() and dim_path.exists():
        events_df = pd.read_parquet(events_path, columns=["product_code"])
        valid_codes = set(dim_df["product_code"].dropna())
        event_codes = set(events_df["product_code"].dropna())
        if event_codes:
            coverage = len(event_codes & valid_codes) / len(event_codes) * 100
            rows.append(
                {
                    "source": "mapping_coverage",
                    "metric": "event_product_code_coverage_pct",
                    "value": round(coverage, 1),
                }
            )

    # ── Enriched adverse-event quality metrics ────────────────────────────────
    ae_df = pd.read_parquet(
        events_path,
        columns=["is_latest_version", "product_code", "manufacturer_d_name"],
    )
    ae_raw = len(ae_df)
    ae_dedup = int(ae_df["is_latest_version"].sum()) if "is_latest_version" in ae_df.columns else ae_raw
    rows.append({"source": "adverse_events", "metric": "raw_count", "value": ae_raw})
    rows.append({"source": "adverse_events", "metric": "dedup_count", "value": ae_dedup})
    dup_rate = round((1 - ae_dedup / ae_raw) * 100, 1) if ae_raw > 0 else 0.0
    rows.append({"source": "adverse_events", "metric": "duplicate_rate_pct", "value": dup_rate})
    missing_pc = ae_df["product_code"].isna().sum() if "product_code" in ae_df.columns else 0
    rows.append(
        {
            "source": "adverse_events",
            "metric": "missing_product_code_pct",
            "value": round(missing_pc / ae_raw * 100, 1) if ae_raw > 0 else 0.0,
        }
    )
    mfr_filled = ae_df["manufacturer_d_name"].notna().sum() if "manufacturer_d_name" in ae_df.columns else 0
    rows.append(
        {
            "source": "adverse_events",
            "metric": "manufacturer_fill_rate_pct",
            "value": round(mfr_filled / ae_raw * 100, 1) if ae_raw > 0 else 0.0,
        }
    )

    # ── Enriched recall mapping-quality metrics ───────────────────────────────
    rc_df = pd.read_parquet(
        recalls_path,
        columns=["mapping_quality", "include_in_core_dashboard"],
    )
    rc_total = len(rc_df)
    for quality_label, metric_name in [
        ("exact_product_code_match", "mapping_quality_exact"),
        ("high_confidence_text_match", "mapping_quality_high"),
        ("low_confidence_text_match", "mapping_quality_low"),
        ("unmapped", "mapping_quality_unmapped"),
    ]:
        count = int((rc_df["mapping_quality"] == quality_label).sum()) if "mapping_quality" in rc_df.columns else 0
        rows.append({"source": "recalls", "metric": metric_name, "value": count})
    core_sum = int(rc_df["include_in_core_dashboard"].sum()) if "include_in_core_dashboard" in rc_df.columns else 0
    rows.append(
        {
            "source": "recalls",
            "metric": "core_dashboard_coverage_pct",
            "value": round(core_sum / rc_total * 100, 1) if rc_total > 0 else 0.0,
        }
    )

    # ── Analysis window constants ─────────────────────────────────────────────
    rows.append({"source": "analysis_window", "metric": "year_min", "value": ANALYSIS_YEAR_MIN})
    rows.append({"source": "analysis_window", "metric": "year_max", "value": ANALYSIS_YEAR_MAX})

    result = pd.DataFrame(rows)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(output_path, index=False)
    return result


def export_all(
    output_dir: str | Path | None = None,
    mart_dir: str | Path | None = None,
    clean_dir: str | Path | None = None,
) -> dict[str, pd.DataFrame]:
    """Export all app-layer CSVs. Returns dict of name -> DataFrame."""
    output_dir = Path(output_dir) if output_dir else DATA_APP
    mart_dir = Path(mart_dir) if mart_dir else DATA_MART
    clean_dir = Path(clean_dir) if clean_dir else DATA_CLEAN

    overview = export_app_overview(
        mart_panel_year_path=mart_dir / "mart_panel_year.parquet",
        output_path=output_dir / "app_overview.csv",
    )
    category = export_app_category_product(
        mart_pc_year_path=mart_dir / "mart_product_code_year.parquet",
        dim_product_code_path=clean_dir / "dim_product_code.parquet",
        output_path=output_dir / "app_category_product.csv",
    )
    manufacturer = export_app_manufacturer(
        mart_firm_product_year_path=mart_dir / "mart_firm_product_year.parquet",
        dim_product_code_path=clean_dir / "dim_product_code.parquet",
        output_path=output_dir / "app_manufacturer.csv",
    )
    methodology = export_app_methodology(
        events_path=clean_dir / "clean_event_device_level.parquet",
        recalls_path=clean_dir / "clean_recall.parquet",
        clearances_path=clean_dir / "clean_510k.parquet",
        dim_product_code_path=clean_dir / "dim_product_code.parquet",
        output_path=output_dir / "app_methodology.csv",
    )

    return {
        "app_overview": overview,
        "app_category_product": category,
        "app_manufacturer": manufacturer,
        "app_methodology": methodology,
    }
