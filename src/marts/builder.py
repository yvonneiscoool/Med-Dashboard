"""Mart builders using DuckDB for SQL aggregation and KPI post-processing.

Three mart tables at different grains:
- mart_panel_year: review_panel + year
- mart_product_code_year: product_code + year
- mart_firm_product_year: manufacturer + product_code + year
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from src.config import DATA_CLEAN, DATA_MART
from src.marts.kpis import (
    events_per_100_clearances,
    recall_to_event_ratio,
    recalls_per_100_clearances,
    severe_recall_share,
)

# ── Default paths ─────────────────────────────────────────────────────────────
_EVENTS_PATH = DATA_CLEAN / "clean_event_device_level.parquet"
_RECALLS_PATH = DATA_CLEAN / "clean_recall.parquet"
_CLEARANCES_PATH = DATA_CLEAN / "clean_510k.parquet"
_DIM_PC_PATH = DATA_CLEAN / "dim_product_code.parquet"

_MART_PANEL_YEAR = DATA_MART / "mart_panel_year.parquet"
_MART_PC_YEAR = DATA_MART / "mart_product_code_year.parquet"
_MART_FIRM_PRODUCT_YEAR = DATA_MART / "mart_firm_product_year.parquet"


def _register_views(
    con: duckdb.DuckDBPyConnection,
    events_path: Path,
    recalls_path: Path,
    clearances_path: Path,
    dim_product_code_path: Path | None = None,
) -> None:
    """Register parquet files as DuckDB views."""
    con.execute(f"CREATE VIEW events AS SELECT * FROM read_parquet('{events_path}')")
    con.execute(f"CREATE VIEW recalls AS SELECT * FROM read_parquet('{recalls_path}')")
    con.execute(f"CREATE VIEW clearances AS SELECT * FROM read_parquet('{clearances_path}')")
    if dim_product_code_path:
        con.execute(f"CREATE VIEW dim_pc AS SELECT * FROM read_parquet('{dim_product_code_path}')")


def build_mart_panel_year(
    events_path: str | Path | None = None,
    recalls_path: str | Path | None = None,
    clearances_path: str | Path | None = None,
    dim_product_code_path: str | Path | None = None,
    output_path: str | Path | None = None,
) -> pd.DataFrame:
    """Build mart_panel_year: grain is review_panel + year."""
    events_path = Path(events_path) if events_path else _EVENTS_PATH
    recalls_path = Path(recalls_path) if recalls_path else _RECALLS_PATH
    clearances_path = Path(clearances_path) if clearances_path else _CLEARANCES_PATH
    dim_product_code_path = Path(dim_product_code_path) if dim_product_code_path else _DIM_PC_PATH
    output_path = Path(output_path) if output_path else _MART_PANEL_YEAR

    con = duckdb.connect()
    _register_views(con, events_path, recalls_path, clearances_path, dim_product_code_path)

    # Events aggregation by review_panel + year
    events_sql = """
    SELECT
        d.review_panel,
        e.event_year AS year,
        COUNT(*) AS event_count_raw,
        COUNT(*) FILTER (WHERE e.is_latest_version) AS event_count_dedup,
        COUNT(*) FILTER (WHERE e.has_death) AS death_related_reports,
        COUNT(*) FILTER (WHERE e.has_serious_injury) AS serious_injury_reports,
        COUNT(*) FILTER (WHERE e.has_malfunction) AS malfunction_reports
    FROM events e
    JOIN dim_pc d ON e.product_code = d.product_code
    WHERE e.event_year IS NOT NULL
    GROUP BY d.review_panel, e.event_year
    """

    # Recalls aggregation by review_panel + year
    recalls_sql = """
    SELECT
        d.review_panel,
        r.recall_year AS year,
        COUNT(*) AS recall_count,
        COUNT(*) FILTER (WHERE r.recall_class = 'Class I') AS class_i_recall_count,
        COUNT(*) FILTER (WHERE r.recall_class = 'Class II') AS class_ii_recall_count,
        COUNT(*) FILTER (WHERE r.recall_class = 'Class III') AS class_iii_recall_count
    FROM recalls r
    JOIN dim_pc d ON COALESCE(r.matched_product_code, r.product_code) = d.product_code
    WHERE r.include_in_core_dashboard = true
      AND r.recall_year IS NOT NULL
    GROUP BY d.review_panel, r.recall_year
    """

    # Clearances aggregation by review_panel + year
    clearances_sql = """
    SELECT
        d.review_panel,
        c.decision_year AS year,
        COUNT(*) AS clearance_count
    FROM clearances c
    JOIN dim_pc d ON c.product_code = d.product_code
    WHERE c.decision_year IS NOT NULL
    GROUP BY d.review_panel, c.decision_year
    """

    ev_df = con.execute(events_sql).fetchdf()
    rc_df = con.execute(recalls_sql).fetchdf()
    cl_df = con.execute(clearances_sql).fetchdf()
    con.close()

    # Full outer join on (review_panel, year)
    df = ev_df.merge(rc_df, on=["review_panel", "year"], how="outer")
    df = df.merge(cl_df, on=["review_panel", "year"], how="outer")

    # Fill NaN counts with 0
    count_cols = [
        "event_count_raw",
        "event_count_dedup",
        "death_related_reports",
        "serious_injury_reports",
        "malfunction_reports",
        "recall_count",
        "class_i_recall_count",
        "class_ii_recall_count",
        "class_iii_recall_count",
        "clearance_count",
    ]
    for col in count_cols:
        if col in df.columns:
            df[col] = df[col].fillna(0).astype(int)

    # Apply KPIs
    df["events_per_100_clearances"] = events_per_100_clearances(
        df["event_count_dedup"],
        df["clearance_count"],
    )
    df["recalls_per_100_clearances"] = recalls_per_100_clearances(
        df["recall_count"],
        df["clearance_count"],
    )
    df["severe_recall_share"] = severe_recall_share(
        df["class_i_recall_count"],
        df["recall_count"],
    )

    df = df.sort_values(["review_panel", "year"]).reset_index(drop=True)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    return df


def build_mart_product_code_year(
    events_path: str | Path | None = None,
    recalls_path: str | Path | None = None,
    clearances_path: str | Path | None = None,
    output_path: str | Path | None = None,
) -> pd.DataFrame:
    """Build mart_product_code_year: grain is product_code + year."""
    events_path = Path(events_path) if events_path else _EVENTS_PATH
    recalls_path = Path(recalls_path) if recalls_path else _RECALLS_PATH
    clearances_path = Path(clearances_path) if clearances_path else _CLEARANCES_PATH
    output_path = Path(output_path) if output_path else _MART_PC_YEAR

    con = duckdb.connect()
    _register_views(con, events_path, recalls_path, clearances_path)

    events_sql = """
    SELECT
        e.product_code,
        e.event_year AS year,
        COUNT(*) FILTER (WHERE e.is_latest_version) AS event_count_dedup
    FROM events e
    WHERE e.event_year IS NOT NULL
    GROUP BY e.product_code, e.event_year
    """

    recalls_sql = """
    SELECT
        COALESCE(r.matched_product_code, r.product_code) AS product_code,
        r.recall_year AS year,
        COUNT(*) AS recall_count,
        COUNT(*) FILTER (WHERE r.recall_class = 'Class I') AS class_i_recall_count
    FROM recalls r
    WHERE r.include_in_core_dashboard = true
      AND r.recall_year IS NOT NULL
    GROUP BY COALESCE(r.matched_product_code, r.product_code), r.recall_year
    """

    clearances_sql = """
    SELECT
        c.product_code,
        c.decision_year AS year,
        COUNT(*) AS clearance_count
    FROM clearances c
    WHERE c.decision_year IS NOT NULL
    GROUP BY c.product_code, c.decision_year
    """

    ev_df = con.execute(events_sql).fetchdf()
    rc_df = con.execute(recalls_sql).fetchdf()
    cl_df = con.execute(clearances_sql).fetchdf()
    con.close()

    df = ev_df.merge(rc_df, on=["product_code", "year"], how="outer")
    df = df.merge(cl_df, on=["product_code", "year"], how="outer")

    count_cols = ["event_count_dedup", "recall_count", "class_i_recall_count", "clearance_count"]
    for col in count_cols:
        if col in df.columns:
            df[col] = df[col].fillna(0).astype(int)

    df["events_per_100_clearances"] = events_per_100_clearances(
        df["event_count_dedup"],
        df["clearance_count"],
    )
    df["recalls_per_100_clearances"] = recalls_per_100_clearances(
        df["recall_count"],
        df["clearance_count"],
    )
    df["recall_to_event_ratio"] = recall_to_event_ratio(
        df["recall_count"],
        df["event_count_dedup"],
    )
    df["severe_recall_share"] = severe_recall_share(
        df["class_i_recall_count"],
        df["recall_count"],
    )

    df = df.sort_values(["product_code", "year"]).reset_index(drop=True)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    return df


def build_mart_firm_product_year(
    events_path: str | Path | None = None,
    recalls_path: str | Path | None = None,
    dim_product_code_path: str | Path | None = None,
    output_path: str | Path | None = None,
) -> pd.DataFrame:
    """Build mart_firm_product_year: grain is manufacturer + product_code + year."""
    events_path = Path(events_path) if events_path else _EVENTS_PATH
    recalls_path = Path(recalls_path) if recalls_path else _RECALLS_PATH
    dim_product_code_path = Path(dim_product_code_path) if dim_product_code_path else _DIM_PC_PATH
    output_path = Path(output_path) if output_path else _MART_FIRM_PRODUCT_YEAR

    con = duckdb.connect()
    con.execute(f"CREATE VIEW events AS SELECT * FROM read_parquet('{events_path}')")
    con.execute(f"CREATE VIEW recalls AS SELECT * FROM read_parquet('{recalls_path}')")
    con.execute(f"CREATE VIEW dim_pc AS SELECT * FROM read_parquet('{dim_product_code_path}')")

    events_sql = """
    SELECT
        e.manufacturer_d_name AS manufacturer,
        e.product_code,
        e.event_year AS year,
        COUNT(*) FILTER (WHERE e.is_latest_version) AS event_count_dedup
    FROM events e
    WHERE e.event_year IS NOT NULL AND e.manufacturer_d_name IS NOT NULL
    GROUP BY e.manufacturer_d_name, e.product_code, e.event_year
    """

    recalls_sql = """
    SELECT
        r.recalling_firm AS manufacturer,
        COALESCE(r.matched_product_code, r.product_code) AS product_code,
        r.recall_year AS year,
        COUNT(*) AS recall_count,
        COUNT(*) FILTER (WHERE r.recall_class = 'Class I') AS severe_recall_count
    FROM recalls r
    WHERE r.include_in_core_dashboard = true
      AND r.recall_year IS NOT NULL
      AND r.recalling_firm IS NOT NULL
    GROUP BY r.recalling_firm, COALESCE(r.matched_product_code, r.product_code), r.recall_year
    """

    ev_df = con.execute(events_sql).fetchdf()
    rc_df = con.execute(recalls_sql).fetchdf()
    con.close()

    df = ev_df.merge(rc_df, on=["manufacturer", "product_code", "year"], how="outer")

    count_cols = ["event_count_dedup", "recall_count", "severe_recall_count"]
    for col in count_cols:
        if col in df.columns:
            df[col] = df[col].fillna(0).astype(int)

    # firm_share_within_product: firm events / total events for that product_code+year
    product_year_total = df.groupby(["product_code", "year"])["event_count_dedup"].transform("sum")
    df["firm_share_within_product"] = df["event_count_dedup"] / product_year_total
    df.loc[product_year_total == 0, "firm_share_within_product"] = pd.NA

    # firm_share_within_panel: join dim_pc for review_panel, then compute
    dim_pc = pd.read_parquet(dim_product_code_path, columns=["product_code", "review_panel"])
    df = df.merge(dim_pc, on="product_code", how="left")

    panel_year_total = df.groupby(["review_panel", "year"])["event_count_dedup"].transform("sum")
    df["firm_share_within_panel"] = df["event_count_dedup"] / panel_year_total
    df.loc[panel_year_total == 0, "firm_share_within_panel"] = pd.NA

    # Drop review_panel helper column — it's not in the final schema
    df = df.drop(columns=["review_panel"])

    df = df.sort_values(["manufacturer", "product_code", "year"]).reset_index(drop=True)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    return df


def build_all_marts(
    events_path: str | Path | None = None,
    recalls_path: str | Path | None = None,
    clearances_path: str | Path | None = None,
    dim_product_code_path: str | Path | None = None,
    output_dir: str | Path | None = None,
) -> dict[str, pd.DataFrame]:
    """Build all three mart tables. Returns dict of name -> DataFrame."""
    output_dir = Path(output_dir) if output_dir else DATA_MART

    panel = build_mart_panel_year(
        events_path=events_path,
        recalls_path=recalls_path,
        clearances_path=clearances_path,
        dim_product_code_path=dim_product_code_path,
        output_path=output_dir / "mart_panel_year.parquet",
    )
    product = build_mart_product_code_year(
        events_path=events_path,
        recalls_path=recalls_path,
        clearances_path=clearances_path,
        output_path=output_dir / "mart_product_code_year.parquet",
    )
    firm = build_mart_firm_product_year(
        events_path=events_path,
        recalls_path=recalls_path,
        dim_product_code_path=dim_product_code_path,
        output_path=output_dir / "mart_firm_product_year.parquet",
    )

    return {
        "mart_panel_year": panel,
        "mart_product_code_year": product,
        "mart_firm_product_year": firm,
    }
