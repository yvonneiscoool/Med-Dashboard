"""Refresh-cycle QA summary report across clean, mart, and app layers."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.config import DATA_APP, DATA_CLEAN, DATA_MART
from src.qa.checks import (
    QAResult,
    check_null_rate,
    check_row_count,
    check_uniqueness,
    run_checks,
)

_MAX_APP_OVERVIEW_ROWS = 5_000
_MAX_APP_CATEGORY_ROWS = 20_000
_MAX_APP_MANUFACTURER_ROWS = 30_000


def build_qa_summary(
    clean_dir: str | Path | None = None,
    mart_dir: str | Path | None = None,
    app_dir: str | Path | None = None,
    output_path: str | Path | None = None,
) -> pd.DataFrame:
    """Run all QA checks and return a summary DataFrame."""
    clean_dir = Path(clean_dir) if clean_dir else DATA_CLEAN
    mart_dir = Path(mart_dir) if mart_dir else DATA_MART
    app_dir = Path(app_dir) if app_dir else DATA_APP
    output_path = Path(output_path) if output_path else DATA_MART / "qa_summary.parquet"

    checks: list[QAResult] = []
    checks.extend(_check_clean_layer(clean_dir))
    checks.extend(_check_mart_layer(mart_dir))
    checks.extend(_check_app_layer(app_dir))

    summary_df = run_checks(checks)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_df.to_parquet(output_path, index=False)
    return summary_df


def _check_clean_layer(clean_dir: Path) -> list[QAResult]:
    """QA checks on clean parquet files."""
    results: list[QAResult] = []

    for name, filename, key_cols, null_check_cols in [
        ("events", "clean_event_device_level.parquet", ["event_record_id"], ["product_code", "event_year"]),
        ("recalls", "clean_recall.parquet", ["recall_number"], ["product_code", "recall_year"]),
        ("clearances", "clean_510k.parquet", ["k_number"], ["product_code", "decision_year"]),
        ("dim_product_code", "dim_product_code.parquet", ["product_code"], ["review_panel", "device_class"]),
    ]:
        path = clean_dir / filename
        if not path.exists():
            results.append(
                QAResult(
                    check_name=f"file_exists_{name}",
                    status="fail",
                    metric_value=None,
                    threshold=None,
                    details=f"{filename} not found",
                )
            )
            continue

        df = pd.read_parquet(path)
        results.append(check_row_count(df, name, min_rows=1))
        results.append(check_uniqueness(df, key_cols, name))
        for col in null_check_cols:
            results.append(check_null_rate(df, col, max_rate=0.05))

    return results


def _check_mart_layer(mart_dir: Path) -> list[QAResult]:
    """QA checks on mart parquet files."""
    results: list[QAResult] = []

    mart_specs = [
        ("mart_panel_year", "mart_panel_year.parquet", ["review_panel", "year"]),
        ("mart_product_code_year", "mart_product_code_year.parquet", ["product_code", "year"]),
        ("mart_firm_product_year", "mart_firm_product_year.parquet", ["manufacturer", "product_code", "year"]),
    ]

    for name, filename, grain_cols in mart_specs:
        path = mart_dir / filename
        if not path.exists():
            results.append(
                QAResult(
                    check_name=f"file_exists_{name}",
                    status="fail",
                    metric_value=None,
                    threshold=None,
                    details=f"{filename} not found",
                )
            )
            continue

        df = pd.read_parquet(path)
        results.append(check_row_count(df, name, min_rows=1))
        results.append(check_uniqueness(df, grain_cols, name))

    return results


def _check_app_layer(app_dir: Path) -> list[QAResult]:
    """QA checks on app CSV files."""
    results: list[QAResult] = []

    app_limits = [
        ("app_overview", "app_overview.csv", _MAX_APP_OVERVIEW_ROWS),
        ("app_category_product", "app_category_product.csv", _MAX_APP_CATEGORY_ROWS),
        ("app_manufacturer", "app_manufacturer.csv", _MAX_APP_MANUFACTURER_ROWS),
    ]

    for name, filename, max_rows in app_limits:
        path = app_dir / filename
        if not path.exists():
            results.append(
                QAResult(
                    check_name=f"file_exists_{name}",
                    status="fail",
                    metric_value=None,
                    threshold=None,
                    details=f"{filename} not found",
                )
            )
            continue

        df = pd.read_csv(path)
        results.append(check_row_count(df, name, min_rows=1))

        row_count = len(df)
        status = "pass" if row_count <= max_rows else "fail"
        results.append(
            QAResult(
                check_name=f"row_limit_{name}",
                status=status,
                metric_value=row_count,
                threshold=max_rows,
                details=f"{name}: {row_count:,} rows (max {max_rows:,})",
            )
        )

    return results


def evaluate_quality_gate(summary_df: pd.DataFrame) -> bool:
    """Return True if no checks have 'fail' status."""
    if summary_df.empty:
        return True
    return bool((summary_df["status"] != "fail").all())
