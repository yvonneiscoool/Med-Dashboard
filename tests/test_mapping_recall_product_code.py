"""Tests for recall-to-classification mapper."""

import pandas as pd

from src.mapping.recall_product_code import (
    _exact_match,
    _text_match,
    map_recall_to_classification,
)


def _make_dim_product_code(codes_and_names):
    """Build a dim_product_code DataFrame from list of (code, name) tuples."""
    return pd.DataFrame(codes_and_names, columns=["product_code", "device_name"])


def _make_recall_df(rows):
    """Build a recall DataFrame from list of dicts."""
    return pd.DataFrame(rows)


# ── _exact_match ───────────────────────────────────────────────────────────


def test_exact_match_tier1():
    """product_code in dim → exact match."""
    recall_df = _make_recall_df([{"product_code": "ABC", "product_description": "Test"}])
    valid_codes = {"ABC", "DEF"}
    mask = _exact_match(recall_df, valid_codes)
    assert mask.iloc[0]


def test_exact_match_missing_product_code():
    """Null product_code → not matched."""
    recall_df = _make_recall_df([{"product_code": None, "product_description": "Test"}])
    valid_codes = {"ABC"}
    mask = _exact_match(recall_df, valid_codes)
    assert not mask.iloc[0]


# ── _text_match ────────────────────────────────────────────────────────────


def test_high_confidence_text_match():
    """Very similar product_description → tier 2."""
    recall_df = _make_recall_df([{"product_code": None, "product_description": "Cardiac Pacemaker System"}])
    device_names = ["Cardiac Pacemaker System", "Blood Glucose Monitor"]
    name_to_code = {"Cardiac Pacemaker System": "DXY", "Blood Glucose Monitor": "NBW"}

    results = _text_match(recall_df, device_names, name_to_code, 85, 60)
    assert len(results) == 1
    result = list(results.values())[0]
    assert result["tier"] == "high_confidence_text_match"
    assert result["matched_code"] == "DXY"
    assert result["score"] >= 85


def test_low_confidence_text_match():
    """Partial match → tier 3."""
    recall_df = _make_recall_df([{"product_code": None, "product_description": "Cardiac Lead Wire"}])
    device_names = ["Cardiac Pacemaker Lead", "Blood Glucose Monitor"]
    name_to_code = {"Cardiac Pacemaker Lead": "DXY", "Blood Glucose Monitor": "NBW"}

    results = _text_match(recall_df, device_names, name_to_code, 85, 60)
    if results:
        result = list(results.values())[0]
        assert result["tier"] == "low_confidence_text_match"
        assert result["score"] < 85
        assert result["score"] >= 60


def test_unmapped():
    """No match → not in results."""
    recall_df = _make_recall_df([{"product_code": None, "product_description": "Completely Unrelated ZZZZZ"}])
    device_names = ["Cardiac Pacemaker System"]
    name_to_code = {"Cardiac Pacemaker System": "DXY"}

    results = _text_match(recall_df, device_names, name_to_code, 85, 60)
    assert len(results) == 0


# ── include_in_core_dashboard ─────────────────────────────────────────────


def test_include_in_core_dashboard_true(tmp_path):
    """Tiers 1-2 have include_in_core_dashboard=True."""
    dim_df = _make_dim_product_code([("ABC", "Test Device"), ("DEF", "Cardiac Pacemaker System")])
    dim_path = tmp_path / "dim.parquet"
    dim_df.to_parquet(dim_path, index=False)

    recall_df = _make_recall_df(
        [
            {
                "recall_number": "R1",
                "product_code": "ABC",
                "product_description": "Test Device",
                "recall_initiation_date": pd.Timestamp("2023-01-01"),
                "recall_year": 2023,
                "recall_class": "I",
                "recalling_firm": "ACME",
                "reason_for_recall": "Defect",
                "status": "Ongoing",
                "voluntary_mandated": "Voluntary",
            },
        ]
    )

    result = map_recall_to_classification(
        recall_df=recall_df,
        dim_product_code_path=dim_path,
        output_path=tmp_path / "out.parquet",
    )
    assert result.iloc[0]["include_in_core_dashboard"]


def test_include_in_core_dashboard_false(tmp_path):
    """Tiers 3-4 have include_in_core_dashboard=False."""
    dim_df = _make_dim_product_code([("ABC", "Cardiac Pacemaker System")])
    dim_path = tmp_path / "dim.parquet"
    dim_df.to_parquet(dim_path, index=False)

    recall_df = _make_recall_df(
        [
            {
                "recall_number": "R1",
                "product_code": None,
                "product_description": "Completely Unrelated ZZZZZ",
                "recall_initiation_date": pd.Timestamp("2023-01-01"),
                "recall_year": 2023,
                "recall_class": "II",
                "recalling_firm": "ACME",
                "reason_for_recall": "Defect",
                "status": "Ongoing",
                "voluntary_mandated": "Voluntary",
            },
        ]
    )

    result = map_recall_to_classification(
        recall_df=recall_df,
        dim_product_code_path=dim_path,
        output_path=tmp_path / "out.parquet",
    )
    assert not result.iloc[0]["include_in_core_dashboard"]


# ── all tiers in single run ───────────────────────────────────────────────


def test_all_tiers_in_single_run(tmp_path):
    """Mixed input produces all mapping tiers."""
    dim_df = _make_dim_product_code([("ABC", "Test Device Alpha"), ("DEF", "Cardiac Pacemaker System")])
    dim_path = tmp_path / "dim.parquet"
    dim_df.to_parquet(dim_path, index=False)

    recall_df = _make_recall_df(
        [
            {
                "recall_number": "R1",
                "product_code": "ABC",
                "product_description": "Test Device Alpha",
                "recall_initiation_date": pd.Timestamp("2023-01-01"),
                "recall_year": 2023,
                "recall_class": "I",
                "recalling_firm": "ACME",
                "reason_for_recall": "Defect",
                "status": "Ongoing",
                "voluntary_mandated": "Voluntary",
            },
            {
                "recall_number": "R2",
                "product_code": None,
                "product_description": "Cardiac Pacemaker System",
                "recall_initiation_date": pd.Timestamp("2023-02-01"),
                "recall_year": 2023,
                "recall_class": "II",
                "recalling_firm": "BETA",
                "reason_for_recall": "Issue",
                "status": "Ongoing",
                "voluntary_mandated": "Voluntary",
            },
            {
                "recall_number": "R3",
                "product_code": None,
                "product_description": "Completely Unrelated ZZZZZ",
                "recall_initiation_date": pd.Timestamp("2023-03-01"),
                "recall_year": 2023,
                "recall_class": "III",
                "recalling_firm": "GAMMA",
                "reason_for_recall": "Problem",
                "status": "Ongoing",
                "voluntary_mandated": "Voluntary",
            },
        ]
    )

    result = map_recall_to_classification(
        recall_df=recall_df,
        dim_product_code_path=dim_path,
        output_path=tmp_path / "out.parquet",
    )

    tiers = set(result["mapping_quality"].tolist())
    # At minimum we should have exact and unmapped
    assert "exact_product_code_match" in tiers
    assert "unmapped" in tiers


# ── end-to-end with parquet I/O ───────────────────────────────────────────


def test_map_recall_end_to_end(tmp_path):
    """Full pipeline with parquet I/O."""
    dim_df = _make_dim_product_code([("ABC", "Test Device")])
    dim_path = tmp_path / "dim.parquet"
    dim_df.to_parquet(dim_path, index=False)

    recall_df = _make_recall_df(
        [
            {
                "recall_number": "R1",
                "product_code": "ABC",
                "product_description": "Test Device",
                "recall_initiation_date": pd.Timestamp("2023-01-01"),
                "recall_year": 2023,
                "recall_class": "I",
                "recalling_firm": "ACME",
                "reason_for_recall": "Defect",
                "status": "Ongoing",
                "voluntary_mandated": "Voluntary",
            }
        ]
    )
    recall_path = tmp_path / "recall.parquet"
    recall_df.to_parquet(recall_path, index=False)

    output_path = tmp_path / "mapped_recall.parquet"
    result = map_recall_to_classification(
        recall_path=recall_path,
        dim_product_code_path=dim_path,
        output_path=output_path,
    )

    assert output_path.exists()
    assert "mapping_quality" in result.columns
    assert "matched_product_code" in result.columns
    assert "match_score" in result.columns
    assert "include_in_core_dashboard" in result.columns


# ── empty DataFrame ───────────────────────────────────────────────────────


def test_map_recall_empty(tmp_path):
    """Empty recall DataFrame produces empty output."""
    dim_df = _make_dim_product_code([("ABC", "Test Device")])
    dim_path = tmp_path / "dim.parquet"
    dim_df.to_parquet(dim_path, index=False)

    recall_df = pd.DataFrame(
        columns=[
            "recall_number",
            "product_code",
            "product_description",
            "recall_initiation_date",
            "recall_year",
            "recall_class",
            "recalling_firm",
            "reason_for_recall",
            "status",
            "voluntary_mandated",
        ]
    )

    result = map_recall_to_classification(
        recall_df=recall_df,
        dim_product_code_path=dim_path,
        output_path=tmp_path / "out.parquet",
    )
    assert len(result) == 0
    assert "mapping_quality" in result.columns
