"""Tests for 510(k) clearance cleaner."""

import json

import pandas as pd

from src.cleaning.clearances import (
    _extract_product_code,
    _parse_clearance_dates,
    _standardize_applicant,
    clean_clearances,
)


def _make_clearance_record(
    k_number="K230001",
    decision_date="2023-01-15",
    product_code="ABC",
    applicant="ACME MEDICAL INC",
    advisory_committee="SU",
    clearance_type="Traditional",
    decision_code="SESE",
    use_openfda=False,
):
    """Helper to build a synthetic 510(k) clearance record."""
    rec = {
        "k_number": k_number,
        "decision_date": decision_date,
        "applicant": applicant,
        "advisory_committee": advisory_committee,
        "clearance_type": clearance_type,
        "decision_code": decision_code,
    }
    if use_openfda:
        rec["openfda"] = {"product_code": [product_code]}
    else:
        rec["product_code"] = product_code
    return rec


def _write_json(tmp_path, records, filename="clearances_2023.json"):
    """Write records as JSON file in expected directory structure."""
    year_dir = tmp_path / "2023"
    year_dir.mkdir(parents=True, exist_ok=True)
    json_path = year_dir / filename
    json_path.write_text(json.dumps({"results": records}))
    return json_path


# ── _extract_product_code ──────────────────────────────────────────────────


def test_extract_product_code_direct():
    """Direct product_code field is preferred."""
    rec = {"product_code": "XYZ", "openfda": {"product_code": ["OTHER"]}}
    assert _extract_product_code(rec) == "XYZ"


def test_extract_product_code_openfda_fallback():
    """Falls back to openfda when direct field is absent."""
    rec = {"openfda": {"product_code": ["FBK"]}}
    assert _extract_product_code(rec) == "FBK"


# ── _parse_clearance_dates ─────────────────────────────────────────────────


def test_parse_clearance_dates():
    df = pd.DataFrame({"decision_date": ["2023-01-15", "20220301"]})
    result = _parse_clearance_dates(df)
    assert result["decision_date"].iloc[0] == pd.Timestamp("2023-01-15")
    assert result["decision_year"].iloc[0] == 2023


# ── _standardize_applicant ─────────────────────────────────────────────────


def test_applicant_standardization():
    """Legal suffixes are removed."""
    df = pd.DataFrame({"applicant": ["ACME MEDICAL INC"]})
    result = _standardize_applicant(df)
    assert result["applicant_std"].iloc[0] == "ACME MEDICAL"


def test_applicant_standardization_empty():
    """None stays empty."""
    df = pd.DataFrame({"applicant": [None]})
    result = _standardize_applicant(df)
    assert result["applicant_std"].iloc[0] is None


# ── dedup ──────────────────────────────────────────────────────────────────


def test_dedup_on_k_number(tmp_path):
    """Duplicate k_number keeps first occurrence."""
    records = [
        _make_clearance_record(k_number="K230001", applicant="FIRST"),
        _make_clearance_record(k_number="K230001", applicant="SECOND"),
        _make_clearance_record(k_number="K230002"),
    ]
    _write_json(tmp_path, records)
    df = clean_clearances(input_dir=tmp_path, output_path=tmp_path / "out.parquet")
    assert len(df) == 2
    k1 = df[df["k_number"] == "K230001"]
    assert k1.iloc[0]["applicant"] == "FIRST"


# ── output schema ─────────────────────────────────────────────────────────


def test_output_schema(tmp_path):
    """Column order matches _OUTPUT_COLUMNS."""
    records = [_make_clearance_record()]
    _write_json(tmp_path, records)
    df = clean_clearances(input_dir=tmp_path, output_path=tmp_path / "out.parquet")

    from src.cleaning.clearances import _OUTPUT_COLUMNS

    assert list(df.columns) == _OUTPUT_COLUMNS


# ── decision_year derived ─────────────────────────────────────────────────


def test_decision_year_derived(tmp_path):
    records = [_make_clearance_record(decision_date="2021-06-01")]
    _write_json(tmp_path, records)
    df = clean_clearances(input_dir=tmp_path, output_path=tmp_path / "out.parquet")
    assert df["decision_year"].iloc[0] == 2021


# ── end-to-end ─────────────────────────────────────────────────────────────


def test_clean_clearances_end_to_end(tmp_path):
    """Full pipeline: JSON → parquet."""
    records = [
        _make_clearance_record(k_number="K230001", product_code="AAA"),
        _make_clearance_record(k_number="K230002", product_code="BBB", use_openfda=True),
    ]
    _write_json(tmp_path, records)
    output = tmp_path / "clean_510k.parquet"
    df = clean_clearances(input_dir=tmp_path, output_path=output)

    assert output.exists()
    assert len(df) == 2
    assert df["k_number"].is_unique
    assert "applicant_std" in df.columns


def test_date_window_filters_out_of_range_clearances(tmp_path):
    """Clearances outside the 2019-2025 window should be excluded."""
    records = [
        {
            "k_number": "K180001",
            "decision_date": "2018-03-15",
            "product_code": "ABC",
            "applicant": "ACME INC",
            "advisory_committee": "SU",
            "clearance_type": "Traditional",
            "decision_code": "SESE",
        },
        {
            "k_number": "K200002",
            "decision_date": "2020-06-01",
            "product_code": "DEF",
            "applicant": "ACME INC",
            "advisory_committee": "CV",
            "clearance_type": "Traditional",
            "decision_code": "SESE",
        },
    ]
    input_dir = tmp_path / "clearances" / "all"
    input_dir.mkdir(parents=True)
    (input_dir / "clearances_all.json").write_text(json.dumps(records))
    output_path = tmp_path / "clean_510k.parquet"
    df = clean_clearances(input_dir=tmp_path / "clearances", output_path=output_path)
    assert len(df) == 1
    assert df.iloc[0]["k_number"] == "K200002"


def test_clean_clearances_empty(tmp_path):
    """Empty input produces empty parquet with correct schema."""
    input_dir = tmp_path / "empty"
    input_dir.mkdir()
    output = tmp_path / "out.parquet"

    df = clean_clearances(input_dir=input_dir, output_path=output)
    assert len(df) == 0
    assert output.exists()

    from src.cleaning.clearances import _OUTPUT_COLUMNS

    assert list(df.columns) == _OUTPUT_COLUMNS
