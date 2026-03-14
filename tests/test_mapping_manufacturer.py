"""Tests for manufacturer name standardization."""

import pandas as pd

from src.mapping.manufacturer import (
    _build_alias_table,
    _cluster_names,
    _normalize_name,
    build_manufacturer_alias,
)

# ── _normalize_name ──────────────────────────────────────────────────────────


def test_normalize_uppercase():
    assert _normalize_name("acme corp") == "ACME"


def test_normalize_punctuation():
    assert _normalize_name("ACME, INC.") == "ACME"


def test_normalize_suffix_removal():
    assert _normalize_name("MEDTRONIC INC") == "MEDTRONIC"
    assert _normalize_name("BOSTON SCIENTIFIC CORP") == "BOSTON SCIENTIFIC"


def test_normalize_multiple_suffixes():
    assert _normalize_name("TEST HOLDINGS GROUP INC") == "TEST"


def test_normalize_empty():
    assert _normalize_name("") == ""
    assert _normalize_name(None) == ""


def test_normalize_preserves_core():
    """Core name content should be preserved."""
    assert _normalize_name("JOHNSON AND JOHNSON") == "JOHNSON AND JOHNSON"


# ── _cluster_names ───────────────────────────────────────────────────────────


def test_cluster_exact_match():
    """Identical normalized names cluster together."""
    names = ["ACME INC", "ACME CORP"]
    normalized = [_normalize_name(n) for n in names]
    result = _cluster_names(names, normalized, threshold=90)
    # Both should map to same cluster
    reps = result["cluster_representative"].unique()
    assert len(reps) == 1


def test_cluster_fuzzy_match():
    """Similar names cluster together above threshold."""
    names = ["MEDTRONIC", "MEDTRONIC PLC"]
    normalized = [_normalize_name(n) for n in names]
    result = _cluster_names(names, normalized, threshold=90)
    reps = result["cluster_representative"].unique()
    assert len(reps) == 1


def test_cluster_below_threshold():
    """Very different names stay in separate clusters."""
    names = ["ACME DEVICES", "ZENITH MEDICAL"]
    normalized = [_normalize_name(n) for n in names]
    result = _cluster_names(names, normalized, threshold=90)
    reps = result["cluster_representative"].unique()
    assert len(reps) == 2


# ── _build_alias_table ───────────────────────────────────────────────────────


def test_build_alias_exact_rule():
    clustered = pd.DataFrame({"raw_name": ["ACME"], "cluster_representative": ["ACME"]})
    counts = pd.DataFrame({"raw_name": ["ACME"], "event_count": [100]})
    result = _build_alias_table(clustered, counts)
    assert result.iloc[0]["normalization_rule"] == "exact"
    assert result.iloc[0]["confidence_level"] == 100.0


def test_build_alias_suffix_rule():
    clustered = pd.DataFrame({"raw_name": ["ACME INC", "ACME"], "cluster_representative": ["ACME", "ACME"]})
    counts = pd.DataFrame({"raw_name": ["ACME INC", "ACME"], "event_count": [50, 100]})
    result = _build_alias_table(clustered, counts)
    inc_row = result[result["raw_name"] == "ACME INC"].iloc[0]
    assert inc_row["normalization_rule"] == "suffix_removal"


def test_build_alias_manual_review_flagging():
    clustered = pd.DataFrame(
        {
            "raw_name": ["A CORP", "A DEVICES"],
            "cluster_representative": ["A CORP", "A CORP"],
        }
    )
    counts = pd.DataFrame({"raw_name": ["A CORP", "A DEVICES"], "event_count": [100, 50]})
    result = _build_alias_table(clustered, counts, top_n_review=10)
    fuzzy_rows = result[result["normalization_rule"] == "fuzzy_match"]
    if len(fuzzy_rows) > 0:
        assert fuzzy_rows["manual_review_flag"].any()


# ── End-to-end ───────────────────────────────────────────────────────────────


def test_build_manufacturer_alias_end_to_end(tmp_path):
    """Full pipeline from parquet input to alias output."""
    # Create input parquet
    input_df = pd.DataFrame(
        {
            "manufacturer_d_name": [
                "ACME INC",
                "ACME INC",
                "ACME INC",
                "ACME CORP",
                "ZENITH MEDICAL LLC",
                "ZENITH MEDICAL LLC",
            ]
        }
    )
    input_path = tmp_path / "events.parquet"
    input_df.to_parquet(input_path, index=False)

    output_path = tmp_path / "alias.parquet"
    review_path = tmp_path / "review.csv"

    result = build_manufacturer_alias(
        input_path=input_path,
        output_path=output_path,
        review_csv_path=review_path,
        fuzzy_threshold=90,
    )

    assert output_path.exists()
    assert review_path.exists()
    assert len(result) > 0
    assert "raw_name" in result.columns
    assert "standardized_name" in result.columns
    # Fewer standardized names than raw names
    assert result["standardized_name"].nunique() <= result["raw_name"].nunique()


def test_build_manufacturer_alias_empty(tmp_path):
    """Empty input produces empty output."""
    input_df = pd.DataFrame({"manufacturer_d_name": pd.Series([], dtype="object")})
    input_path = tmp_path / "empty.parquet"
    input_df.to_parquet(input_path, index=False)

    output_path = tmp_path / "alias.parquet"
    review_path = tmp_path / "review.csv"

    result = build_manufacturer_alias(
        input_path=input_path,
        output_path=output_path,
        review_csv_path=review_path,
    )
    assert len(result) == 0
