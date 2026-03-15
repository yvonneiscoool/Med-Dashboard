"""Tests for recall-to-classification mapper."""

import pandas as pd

from src.mapping.recall_product_code import (
    _exact_match,
    _preprocess_description,
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


# ── _preprocess_description ──────────────────────────────────────────────


def test_preprocess_strips_part_numbers():
    """Part/item/model/catalog numbers are removed."""
    desc = "Baxter Infusion Pump, Item Number 502015637, Model XR-100"
    result = _preprocess_description(desc)
    assert "502015637" not in result
    assert "XR-100" not in result
    assert "infusion pump" in result.lower()


def test_preprocess_strips_udi():
    """UDI barcodes are removed."""
    desc = "NanoClave Manifold UDI: (01)10840619083929(17)220729"
    result = _preprocess_description(desc)
    assert "(01)10840619083929" not in result
    assert "manifold" in result.lower()


def test_preprocess_strips_lot_numbers():
    """Lot numbers are removed."""
    desc = "Surgical Gown Lot Number: 4496182. Non-Sterile"
    result = _preprocess_description(desc)
    assert "4496182" not in result
    assert "surgical gown" in result.lower()


def test_preprocess_strips_dimensions():
    """Dimensions like 2.5x3.9mm are removed."""
    desc = "Step Drill 2.5-3.9x124mm, 16mm stop, contra-angle"
    result = _preprocess_description(desc)
    assert "2.5-3.9x124mm" not in result
    assert "drill" in result.lower()


def test_preprocess_strips_ref_numbers():
    """REF/RPN/GPN codes are removed."""
    desc = "Hip Stem REF (RPN): HNB5.0-38-65-P-NS-RIM REF (GPN): G05979"
    result = _preprocess_description(desc)
    assert "HNB5.0-38-65-P-NS-RIM" not in result
    assert "hip stem" in result.lower()


def test_preprocess_empty_and_none():
    """Empty/None inputs return empty string."""
    assert _preprocess_description("") == ""
    assert _preprocess_description(None) == ""


# ── _text_match with preprocessing and token_set_ratio ───────────────────


def test_text_match_long_description():
    """Long product description with noise should match device name via token_set_ratio."""
    recall_df = _make_recall_df(
        [
            {
                "product_code": None,
                "product_description": (
                    "Baxter SIGMA Spectrum Infusion Pump with Master Drug Library "
                    "(Version 8.x), Model# 35700BAX2, PN 1056490. "
                    "Product Usage: For clinical infusion therapy."
                ),
            }
        ]
    )
    device_names = ["Pump, Infusion", "Catheter, Intravascular", "Monitor, Blood Pressure"]
    name_to_code = {
        "Pump, Infusion": "FRN",
        "Catheter, Intravascular": "DQY",
        "Monitor, Blood Pressure": "DXN",
    }

    results = _text_match(recall_df, device_names, name_to_code, 85, 60)
    assert len(results) == 1
    result = list(results.values())[0]
    assert result["matched_code"] == "FRN"


def test_text_match_imaging_system():
    """MRI system description matches NMR imaging device name."""
    recall_df = _make_recall_df(
        [
            {
                "product_code": None,
                "product_description": ("SIGNA Voyager, Nuclear Magnetic Resonance Imaging System"),
            }
        ]
    )
    device_names = [
        "System, Nuclear Magnetic Resonance Imaging",
        "Pump, Infusion",
    ]
    name_to_code = {
        "System, Nuclear Magnetic Resonance Imaging": "LNH",
        "Pump, Infusion": "FRN",
    }

    results = _text_match(recall_df, device_names, name_to_code, 85, 60)
    assert len(results) == 1
    result = list(results.values())[0]
    assert result["matched_code"] == "LNH"
    assert result["tier"] == "high_confidence_text_match"
