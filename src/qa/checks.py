"""Reusable QA check functions returning structured QAResult objects."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass
class QAResult:
    """Result of a single QA check."""

    check_name: str
    status: str  # "pass", "warn", "fail"
    metric_value: float | int | None
    threshold: float | int | None
    details: str


def check_row_count(df: pd.DataFrame, name: str, min_rows: int = 1) -> QAResult:
    """Check that a DataFrame has at least min_rows rows."""
    count = len(df)
    status = "pass" if count >= min_rows else "fail"
    return QAResult(
        check_name=f"row_count_{name}",
        status=status,
        metric_value=count,
        threshold=min_rows,
        details=f"{name}: {count:,} rows (min {min_rows:,})",
    )


def check_null_rate(df: pd.DataFrame, column: str, max_rate: float = 0.05) -> QAResult:
    """Check that null rate for a column is below threshold."""
    if column not in df.columns:
        return QAResult(
            check_name=f"null_rate_{column}",
            status="fail",
            metric_value=None,
            threshold=max_rate,
            details=f"Column '{column}' not found",
        )
    null_rate = df[column].isna().mean() if len(df) > 0 else 0.0
    if null_rate <= max_rate:
        status = "pass"
    elif null_rate <= max_rate * 2:
        status = "warn"
    else:
        status = "fail"
    return QAResult(
        check_name=f"null_rate_{column}",
        status=status,
        metric_value=round(null_rate, 4),
        threshold=max_rate,
        details=f"{column}: {null_rate:.1%} null (max {max_rate:.1%})",
    )


def check_uniqueness(df: pd.DataFrame, columns: list[str], name: str) -> QAResult:
    """Check that columns form a unique key (no duplicates)."""
    total = len(df)
    if total == 0:
        return QAResult(
            check_name=f"uniqueness_{name}",
            status="pass",
            metric_value=0,
            threshold=0,
            details=f"{name}: empty DataFrame",
        )
    unique = df.drop_duplicates(subset=columns).shape[0]
    dup_count = total - unique
    status = "pass" if dup_count == 0 else "fail"
    return QAResult(
        check_name=f"uniqueness_{name}",
        status=status,
        metric_value=dup_count,
        threshold=0,
        details=f"{name}: {dup_count:,} duplicates on {columns}",
    )


def check_dedup_ratio(raw_count: int, dedup_count: int, name: str, max_ratio: float = 0.5) -> QAResult:
    """Check that dedup didn't remove too many rows (ratio = removed/raw)."""
    if raw_count == 0:
        return QAResult(
            check_name=f"dedup_ratio_{name}",
            status="pass",
            metric_value=0.0,
            threshold=max_ratio,
            details=f"{name}: no raw rows",
        )
    ratio = (raw_count - dedup_count) / raw_count
    if ratio <= max_ratio:
        status = "pass"
    elif ratio <= max_ratio + 0.1:
        status = "warn"
    else:
        status = "fail"
    return QAResult(
        check_name=f"dedup_ratio_{name}",
        status=status,
        metric_value=round(ratio, 4),
        threshold=max_ratio,
        details=f"{name}: {ratio:.1%} removed ({raw_count:,} -> {dedup_count:,})",
    )


def check_coverage(
    df: pd.DataFrame,
    column: str,
    reference_values: set | list | None = None,
) -> QAResult:
    """Check non-null coverage of a column, optionally against reference values."""
    if column not in df.columns:
        return QAResult(
            check_name=f"coverage_{column}",
            status="fail",
            metric_value=None,
            threshold=None,
            details=f"Column '{column}' not found",
        )
    non_null = df[column].notna().sum()
    total = len(df)
    coverage = non_null / total if total > 0 else 0.0

    if reference_values is not None:
        ref_set = set(reference_values)
        actual = set(df[column].dropna().unique())
        matched = len(actual & ref_set)
        ref_coverage = matched / len(ref_set) if ref_set else 0.0
        status = "pass" if ref_coverage >= 0.7 else ("warn" if ref_coverage >= 0.5 else "fail")
        return QAResult(
            check_name=f"coverage_{column}",
            status=status,
            metric_value=round(ref_coverage, 4),
            threshold=0.7,
            details=f"{column}: {matched}/{len(ref_set)} ref values ({ref_coverage:.1%}), row coverage {coverage:.1%}",
        )

    status = "pass" if coverage >= 0.7 else ("warn" if coverage >= 0.5 else "fail")
    return QAResult(
        check_name=f"coverage_{column}",
        status=status,
        metric_value=round(coverage, 4),
        threshold=0.7,
        details=f"{column}: {coverage:.1%} non-null ({non_null:,}/{total:,})",
    )


def check_volume_shift(
    df: pd.DataFrame,
    year_column: str,
    count_column: str | None = None,
    max_yoy_change: float = 0.5,
) -> QAResult:
    """Check for anomalous year-over-year volume shifts."""
    if year_column not in df.columns:
        return QAResult(
            check_name=f"volume_shift_{year_column}",
            status="fail",
            metric_value=None,
            threshold=max_yoy_change,
            details=f"Column '{year_column}' not found",
        )
    if count_column:
        yearly = df.groupby(year_column)[count_column].sum().sort_index()
    else:
        yearly = df.groupby(year_column).size().sort_index()

    if len(yearly) < 2:
        return QAResult(
            check_name=f"volume_shift_{year_column}",
            status="pass",
            metric_value=0.0,
            threshold=max_yoy_change,
            details="Not enough years to compare",
        )

    max_shift = 0.0
    worst_pair = ""
    for i in range(1, len(yearly)):
        prev = yearly.iloc[i - 1]
        curr = yearly.iloc[i]
        if prev == 0:
            continue
        change = abs(curr - prev) / prev
        if change > max_shift:
            max_shift = change
            worst_pair = f"{yearly.index[i - 1]}->{yearly.index[i]}"

    if max_shift <= max_yoy_change:
        status = "pass"
    elif max_shift <= max_yoy_change * 1.5:
        status = "warn"
    else:
        status = "fail"
    return QAResult(
        check_name=f"volume_shift_{year_column}",
        status=status,
        metric_value=round(max_shift, 4),
        threshold=max_yoy_change,
        details=f"Max YoY change: {max_shift:.1%} ({worst_pair})",
    )


def run_checks(checks: list[QAResult]) -> pd.DataFrame:
    """Aggregate QA results into a summary DataFrame."""
    rows = []
    for r in checks:
        rows.append(
            {
                "check_name": r.check_name,
                "status": r.status,
                "metric_value": r.metric_value,
                "threshold": r.threshold,
                "details": r.details,
            }
        )
    return pd.DataFrame(rows)
