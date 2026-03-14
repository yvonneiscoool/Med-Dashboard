"""Tests for QA checks module."""

import pandas as pd

from src.qa.checks import (
    QAResult,
    check_coverage,
    check_dedup_ratio,
    check_null_rate,
    check_row_count,
    check_uniqueness,
    check_volume_shift,
    run_checks,
)

# ── check_row_count ──────────────────────────────────────────────────────────


def test_row_count_pass():
    df = pd.DataFrame({"a": [1, 2, 3]})
    result = check_row_count(df, "test", min_rows=2)
    assert result.status == "pass"
    assert result.metric_value == 3


def test_row_count_fail():
    df = pd.DataFrame({"a": []})
    result = check_row_count(df, "test", min_rows=1)
    assert result.status == "fail"
    assert result.metric_value == 0


# ── check_null_rate ──────────────────────────────────────────────────────────


def test_null_rate_pass():
    df = pd.DataFrame({"col": [1, 2, 3, 4, 5]})
    result = check_null_rate(df, "col", max_rate=0.05)
    assert result.status == "pass"
    assert result.metric_value == 0.0


def test_null_rate_warn():
    df = pd.DataFrame({"col": [1, None, 3, 4, 5, 6, 7, 8, 9, 10]})
    result = check_null_rate(df, "col", max_rate=0.05)
    assert result.status == "warn"  # 10% null, max 5%, warn at <=10%


def test_null_rate_fail():
    df = pd.DataFrame({"col": [1, None, None, None, 5]})
    result = check_null_rate(df, "col", max_rate=0.05)
    assert result.status == "fail"  # 60% null


def test_null_rate_missing_column():
    df = pd.DataFrame({"other": [1]})
    result = check_null_rate(df, "missing")
    assert result.status == "fail"


# ── check_uniqueness ─────────────────────────────────────────────────────────


def test_uniqueness_pass():
    df = pd.DataFrame({"id": [1, 2, 3]})
    result = check_uniqueness(df, ["id"], "test")
    assert result.status == "pass"
    assert result.metric_value == 0


def test_uniqueness_fail():
    df = pd.DataFrame({"id": [1, 1, 2]})
    result = check_uniqueness(df, ["id"], "test")
    assert result.status == "fail"
    assert result.metric_value == 1


def test_uniqueness_empty():
    df = pd.DataFrame({"id": []})
    result = check_uniqueness(df, ["id"], "test")
    assert result.status == "pass"


# ── check_dedup_ratio ────────────────────────────────────────────────────────


def test_dedup_ratio_pass():
    result = check_dedup_ratio(100, 80, "test", max_ratio=0.5)
    assert result.status == "pass"
    assert result.metric_value == 0.2


def test_dedup_ratio_warn():
    result = check_dedup_ratio(100, 45, "test", max_ratio=0.5)
    assert result.status == "warn"  # 55% removed, threshold 50%, warn at <=60%


def test_dedup_ratio_fail():
    result = check_dedup_ratio(100, 10, "test", max_ratio=0.5)
    assert result.status == "fail"  # 90% removed


def test_dedup_ratio_zero_raw():
    result = check_dedup_ratio(0, 0, "test")
    assert result.status == "pass"


# ── check_coverage ───────────────────────────────────────────────────────────


def test_coverage_pass():
    df = pd.DataFrame({"col": ["a", "b", "c", "d"]})
    result = check_coverage(df, "col")
    assert result.status == "pass"
    assert result.metric_value == 1.0


def test_coverage_with_reference():
    df = pd.DataFrame({"col": ["a", "b", "c"]})
    result = check_coverage(df, "col", reference_values={"a", "b", "c", "d"})
    assert result.status == "pass"  # 3/4 = 75% >= 70%


def test_coverage_fail_with_reference():
    df = pd.DataFrame({"col": ["a"]})
    result = check_coverage(df, "col", reference_values={"a", "b", "c", "d", "e"})
    assert result.status == "fail"  # 1/5 = 20%


def test_coverage_missing_column():
    df = pd.DataFrame({"other": [1]})
    result = check_coverage(df, "missing")
    assert result.status == "fail"


# ── check_volume_shift ───────────────────────────────────────────────────────


def test_volume_shift_pass():
    df = pd.DataFrame({"year": [2020, 2020, 2021, 2021, 2021]})
    result = check_volume_shift(df, "year", max_yoy_change=0.5)
    assert result.status == "pass"  # 2 -> 3 = 50%


def test_volume_shift_fail():
    df = pd.DataFrame({"year": [2020, 2021, 2021, 2021, 2021, 2021]})
    result = check_volume_shift(df, "year", max_yoy_change=0.5)
    assert result.status == "fail"  # 1 -> 5 = 400%


def test_volume_shift_single_year():
    df = pd.DataFrame({"year": [2020, 2020]})
    result = check_volume_shift(df, "year")
    assert result.status == "pass"


def test_volume_shift_missing_column():
    df = pd.DataFrame({"other": [1]})
    result = check_volume_shift(df, "year")
    assert result.status == "fail"


# ── run_checks ───────────────────────────────────────────────────────────────


def test_run_checks_aggregation():
    checks = [
        QAResult("check_a", "pass", 100, 10, "ok"),
        QAResult("check_b", "fail", 0, 1, "bad"),
        QAResult("check_c", "warn", 0.08, 0.05, "borderline"),
    ]
    summary = run_checks(checks)
    assert isinstance(summary, pd.DataFrame)
    assert len(summary) == 3
    assert list(summary.columns) == [
        "check_name",
        "status",
        "metric_value",
        "threshold",
        "details",
    ]
    assert summary[summary["status"] == "fail"].shape[0] == 1
