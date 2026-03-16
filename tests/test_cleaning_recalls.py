"""Tests for recall cleaner."""

import json

import pandas as pd

from src.cleaning.recalls import (
    _extract_product_code,
    _parse_recall_class,
    _parse_recall_dates,
    clean_recalls,
)


def _make_recall_record(
    recall_number="Z-1234-2023",
    recall_initiation_date="20230115",
    classification="Class II",
    product_code="ABC",
    product_description="Test Device",
    recalling_firm="ACME MEDICAL INC",
    reason_for_recall="Device may malfunction",
    status="Ongoing",
    voluntary_mandated="Voluntary: Firm Initiated",
):
    """Helper to build a synthetic recall/enforcement record."""
    rec = {
        "recall_number": recall_number,
        "recall_initiation_date": recall_initiation_date,
        "classification": classification,
        "product_description": product_description,
        "recalling_firm": recalling_firm,
        "reason_for_recall": reason_for_recall,
        "status": status,
        "voluntary_mandated": voluntary_mandated,
    }
    if product_code is not None:
        rec["openfda"] = {"product_code": [product_code]}
    return rec


def _write_json(tmp_path, records, filename="recalls_2023.json"):
    """Write records as JSON file in expected directory structure."""
    year_dir = tmp_path / "2023"
    year_dir.mkdir(parents=True, exist_ok=True)
    json_path = year_dir / filename
    json_path.write_text(json.dumps({"results": records}))
    return json_path


# ── _extract_product_code ──────────────────────────────────────────────────


def test_extract_product_code_from_openfda():
    """Nested openfda extraction works."""
    rec = {"openfda": {"product_code": ["XYZ"]}}
    assert _extract_product_code(rec) == "XYZ"


def test_extract_product_code_missing_openfda():
    """Missing openfda returns None."""
    rec = {"some_field": "value"}
    assert _extract_product_code(rec) is None


def test_extract_product_code_empty_array():
    """Empty product_code array returns None."""
    rec = {"openfda": {"product_code": []}}
    assert _extract_product_code(rec) is None


# ── _parse_recall_class ────────────────────────────────────────────────────


def test_parse_recall_class_I():
    df = pd.DataFrame({"classification": ["Class I"]})
    result = _parse_recall_class(df)
    assert result["recall_class"].iloc[0] == "I"


def test_parse_recall_class_II():
    df = pd.DataFrame({"classification": ["Class II"]})
    result = _parse_recall_class(df)
    assert result["recall_class"].iloc[0] == "II"


def test_parse_recall_class_III():
    df = pd.DataFrame({"classification": ["Class III"]})
    result = _parse_recall_class(df)
    assert result["recall_class"].iloc[0] == "III"


def test_parse_recall_class_invalid():
    df = pd.DataFrame({"classification": ["Unknown"]})
    result = _parse_recall_class(df)
    assert result["recall_class"].iloc[0] is None


# ── _parse_recall_dates ────────────────────────────────────────────────────


def test_parse_dates_yyyymmdd():
    df = pd.DataFrame({"recall_initiation_date": ["20230115"]})
    result = _parse_recall_dates(df)
    assert result["recall_initiation_date"].iloc[0] == pd.Timestamp("2023-01-15")
    assert result["recall_year"].iloc[0] == 2023


def test_parse_dates_iso():
    df = pd.DataFrame({"recall_initiation_date": ["2023-01-15"]})
    result = _parse_recall_dates(df)
    assert result["recall_initiation_date"].iloc[0] == pd.Timestamp("2023-01-15")


def test_parse_dates_invalid():
    df = pd.DataFrame({"recall_initiation_date": ["BADDATE"]})
    result = _parse_recall_dates(df)
    assert pd.isna(result["recall_initiation_date"].iloc[0])


# ── dedup ──────────────────────────────────────────────────────────────────


def test_dedup_on_recall_number(tmp_path):
    """Duplicate recall_number keeps first occurrence."""
    records = [
        _make_recall_record(recall_number="R1", recalling_firm="FIRST"),
        _make_recall_record(recall_number="R1", recalling_firm="SECOND"),
        _make_recall_record(recall_number="R2"),
    ]
    _write_json(tmp_path, records)
    df = clean_recalls(input_dir=tmp_path, output_path=tmp_path / "out.parquet")
    assert len(df) == 2
    r1 = df[df["recall_number"] == "R1"]
    assert r1.iloc[0]["recalling_firm"] == "FIRST"


# ── recall_year derived ───────────────────────────────────────────────────


def test_recall_year_derived(tmp_path):
    records = [_make_recall_record(recall_initiation_date="20210601")]
    _write_json(tmp_path, records)
    df = clean_recalls(input_dir=tmp_path, output_path=tmp_path / "out.parquet")
    assert df["recall_year"].iloc[0] == 2021


# ── end-to-end ─────────────────────────────────────────────────────────────


def test_clean_recalls_end_to_end(tmp_path):
    """Full pipeline: JSON → parquet with correct schema and PK uniqueness."""
    records = [
        _make_recall_record(recall_number="R1", classification="Class I"),
        _make_recall_record(recall_number="R2", classification="Class III", product_code="DEF"),
    ]
    _write_json(tmp_path, records)
    output = tmp_path / "clean_recall.parquet"
    df = clean_recalls(input_dir=tmp_path, output_path=output)

    assert output.exists()
    assert len(df) == 2
    assert df["recall_number"].is_unique

    # Schema check
    from src.cleaning.recalls import _OUTPUT_COLUMNS

    assert list(df.columns) == _OUTPUT_COLUMNS

    # Recall class parsed correctly
    assert set(df["recall_class"].tolist()) == {"I", "III"}


def test_date_window_filters_out_of_range_records(tmp_path):
    """Records outside the 2019-2025 window should be excluded."""
    records = [
        {
            "recall_number": "Z-0001-2018",
            "recall_initiation_date": "20180315",
            "classification": "Class I",
            "product_description": "Old device",
            "recalling_firm": "ACME",
            "reason_for_recall": "Defect",
            "status": "Terminated",
            "voluntary_mandated": "Voluntary",
            "openfda": {"product_code": ["ABC"]},
        },
        {
            "recall_number": "Z-0002-2020",
            "recall_initiation_date": "20200601",
            "classification": "Class II",
            "product_description": "Current device",
            "recalling_firm": "ACME",
            "reason_for_recall": "Defect",
            "status": "Ongoing",
            "voluntary_mandated": "Voluntary",
            "openfda": {"product_code": ["DEF"]},
        },
        {
            "recall_number": "Z-0003-2026",
            "recall_initiation_date": "20260115",
            "classification": "Class III",
            "product_description": "Future device",
            "recalling_firm": "ACME",
            "reason_for_recall": "Defect",
            "status": "Ongoing",
            "voluntary_mandated": "Voluntary",
            "openfda": {"product_code": ["GHI"]},
        },
    ]
    input_dir = tmp_path / "recalls" / "all"
    input_dir.mkdir(parents=True)
    (input_dir / "recalls_all.json").write_text(json.dumps(records))
    output_path = tmp_path / "clean_recall.parquet"
    df = clean_recalls(input_dir=tmp_path / "recalls", output_path=output_path)
    assert len(df) == 1
    assert df.iloc[0]["recall_number"] == "Z-0002-2020"


# ── product_code enrichment ───────────────────────────────────────────────


def test_product_code_enriched_from_recall_lookup(tmp_path):
    """Product code should be populated from recall API lookup when openfda is empty."""
    enforcement_records = [
        {
            "recall_number": "Z-0001-2023",
            "recall_initiation_date": "20230115",
            "classification": "Class II",
            "product_description": "Test Device Alpha",
            "recalling_firm": "ACME MEDICAL",
            "reason_for_recall": "Defect found",
            "status": "Ongoing",
            "voluntary_mandated": "Voluntary: Firm Initiated",
            "openfda": {},
        },
        {
            "recall_number": "Z-0002-2023",
            "recall_initiation_date": "20230601",
            "classification": "Class I",
            "product_description": "Test Device Beta",
            "recalling_firm": "BETA CORP",
            "reason_for_recall": "Safety issue",
            "status": "Terminated",
            "voluntary_mandated": "Voluntary: Firm Initiated",
            "openfda": {},
        },
    ]

    recall_lookup = [
        {"product_res_number": "Z-0001-2023", "product_code": "DXN"},
        {"product_res_number": "Z-0002-2023", "product_code": "FRN"},
        {"product_res_number": "Z-9999-2023", "product_code": "ABC"},
    ]

    enf_dir = tmp_path / "recalls" / "all"
    enf_dir.mkdir(parents=True)
    (enf_dir / "recalls_all.json").write_text(json.dumps(enforcement_records))

    lookup_dir = tmp_path / "recall_product_codes" / "all"
    lookup_dir.mkdir(parents=True)
    (lookup_dir / "recall_product_codes_all.json").write_text(json.dumps(recall_lookup))

    output = tmp_path / "clean_recall.parquet"
    df = clean_recalls(
        input_dir=tmp_path / "recalls",
        output_path=output,
        recall_pc_dir=tmp_path / "recall_product_codes",
    )

    assert len(df) == 2
    assert df.loc[df["recall_number"] == "Z-0001-2023", "product_code"].iloc[0] == "DXN"
    assert df.loc[df["recall_number"] == "Z-0002-2023", "product_code"].iloc[0] == "FRN"


def test_product_code_from_openfda_not_overwritten(tmp_path):
    """If openfda already has a product_code, it should NOT be overwritten by lookup."""
    enforcement_records = [
        {
            "recall_number": "Z-0001-2023",
            "recall_initiation_date": "20230115",
            "classification": "Class II",
            "product_description": "Test Device",
            "recalling_firm": "ACME",
            "reason_for_recall": "Defect",
            "status": "Ongoing",
            "voluntary_mandated": "Voluntary",
            "openfda": {"product_code": ["ORIG"]},
        },
    ]
    recall_lookup = [
        {"product_res_number": "Z-0001-2023", "product_code": "NEW"},
    ]

    enf_dir = tmp_path / "recalls" / "all"
    enf_dir.mkdir(parents=True)
    (enf_dir / "recalls_all.json").write_text(json.dumps(enforcement_records))

    lookup_dir = tmp_path / "recall_product_codes" / "all"
    lookup_dir.mkdir(parents=True)
    (lookup_dir / "recall_product_codes_all.json").write_text(json.dumps(recall_lookup))

    output = tmp_path / "clean_recall.parquet"
    df = clean_recalls(
        input_dir=tmp_path / "recalls",
        output_path=output,
        recall_pc_dir=tmp_path / "recall_product_codes",
    )

    assert df.iloc[0]["product_code"] == "ORIG"


def test_product_code_trailing_dashes_stripped(tmp_path):
    """Product codes with trailing dashes should be cleaned."""
    enforcement_records = [
        {
            "recall_number": "Z-0001-2023",
            "recall_initiation_date": "20230115",
            "classification": "Class II",
            "product_description": "X-Ray System",
            "recalling_firm": "ACME",
            "reason_for_recall": "Defect",
            "status": "Ongoing",
            "voluntary_mandated": "Voluntary",
            "openfda": {},
        },
    ]
    recall_lookup = [
        {"product_res_number": "Z-0001-2023", "product_code": "IZL--"},
    ]

    enf_dir = tmp_path / "recalls" / "all"
    enf_dir.mkdir(parents=True)
    (enf_dir / "recalls_all.json").write_text(json.dumps(enforcement_records))

    lookup_dir = tmp_path / "recall_product_codes" / "all"
    lookup_dir.mkdir(parents=True)
    (lookup_dir / "recall_product_codes_all.json").write_text(json.dumps(recall_lookup))

    output = tmp_path / "clean_recall.parquet"
    df = clean_recalls(
        input_dir=tmp_path / "recalls",
        output_path=output,
        recall_pc_dir=tmp_path / "recall_product_codes",
    )

    assert df.iloc[0]["product_code"] == "IZL"


def test_clean_recalls_works_without_lookup_dir(tmp_path):
    """clean_recalls should still work if recall_pc_dir is not provided or missing."""
    records = [_make_recall_record(recall_number="R1", product_code=None)]
    _write_json(tmp_path, records)
    output = tmp_path / "clean_recall.parquet"
    df = clean_recalls(input_dir=tmp_path, output_path=output)
    assert len(df) == 1
    assert pd.isna(df.iloc[0]["product_code"])


def test_clean_recalls_empty(tmp_path):
    """Empty input produces empty parquet with correct schema."""
    input_dir = tmp_path / "empty"
    input_dir.mkdir()
    output = tmp_path / "out.parquet"

    df = clean_recalls(input_dir=input_dir, output_path=output)
    assert len(df) == 0
    assert output.exists()

    from src.cleaning.recalls import _OUTPUT_COLUMNS

    assert list(df.columns) == _OUTPUT_COLUMNS
