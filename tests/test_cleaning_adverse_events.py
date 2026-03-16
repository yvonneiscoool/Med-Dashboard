"""Tests for adverse event cleaner."""

import json
import zipfile

import pandas as pd

from src.cleaning.adverse_events import (
    _aggregate_patient_outcomes,
    _dedup_reports,
    _flatten_devices,
    _normalize_dates,
    _normalize_flags,
    _read_zip_json,
    clean_adverse_events,
)


def _make_record(
    mdr_key="12345",
    date_received="20230115",
    devices=None,
    patients=None,
    event_type="Injury",
    adverse_event_flag="Y",
    product_problem_flag="N",
    source_type="Manufacturer report",
    remedial_action=None,
):
    """Helper to build a synthetic adverse event record."""
    if devices is None:
        devices = [
            {
                "device_report_product_code": "ABC",
                "brand_name": "TestBrand",
                "generic_name": "Test Generic",
                "manufacturer_d_name": "ACME CORP",
            }
        ]
    rec = {
        "mdr_report_key": mdr_key,
        "date_received": date_received,
        "device": devices,
        "event_type": event_type,
        "adverse_event_flag": adverse_event_flag,
        "product_problem_flag": product_problem_flag,
        "source_type": source_type,
    }
    if patients is not None:
        rec["patient"] = patients
    if remedial_action is not None:
        rec["remedial_action"] = remedial_action
    return rec


def _make_zip(tmp_path, records, filename="test.zip"):
    """Create a ZIP file containing a JSON with results array."""
    zip_path = tmp_path / filename
    json_data = json.dumps({"meta": {}, "results": records})
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("data.json", json_data)
    return zip_path


# ── _read_zip_json ───────────────────────────────────────────────────────────


def test_read_zip_json(tmp_path):
    """Read records from a ZIP containing JSON."""
    records = [_make_record()]
    zip_path = _make_zip(tmp_path, records)
    result = _read_zip_json(zip_path)
    assert len(result) == 1
    assert result[0]["mdr_report_key"] == "12345"


def test_read_zip_json_multiple_files(tmp_path):
    """Read records from ZIP with multiple JSON files."""
    zip_path = tmp_path / "multi.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("part1.json", json.dumps({"results": [_make_record(mdr_key="A")]}))
        zf.writestr("part2.json", json.dumps({"results": [_make_record(mdr_key="B")]}))
    result = _read_zip_json(zip_path)
    assert len(result) == 2


# ── _flatten_devices ─────────────────────────────────────────────────────────


def test_flatten_single_device():
    records = [_make_record()]
    df = _flatten_devices(records)
    assert len(df) == 1
    assert df.iloc[0]["event_record_id"] == "12345_0"
    assert df.iloc[0]["product_code"] == "ABC"


def test_flatten_multi_device():
    devices = [
        {"device_report_product_code": "AAA", "brand_name": "B1"},
        {"device_report_product_code": "BBB", "brand_name": "B2"},
    ]
    records = [_make_record(devices=devices)]
    df = _flatten_devices(records)
    assert len(df) == 2
    assert df.iloc[0]["event_record_id"] == "12345_0"
    assert df.iloc[1]["event_record_id"] == "12345_1"


def test_flatten_no_device():
    """Records with no device array are skipped."""
    records = [_make_record(devices=[])]
    df = _flatten_devices(records)
    assert len(df) == 0


def test_flatten_no_mdr_key():
    """Records without mdr_report_key are skipped."""
    records = [_make_record(mdr_key=None)]
    df = _flatten_devices(records)
    assert len(df) == 0


def test_flatten_source_type_list():
    """Source type as list takes first element."""
    records = [_make_record(source_type=["Manufacturer report", "Other"])]
    df = _flatten_devices(records)
    assert df.iloc[0]["source_type"] == "Manufacturer report"


def test_flatten_remedial_action():
    """Remedial action flag is True when remedial_action is non-empty."""
    records = [_make_record(remedial_action=["Recall"])]
    df = _flatten_devices(records)
    assert df.iloc[0]["remedial_action_flag"]

    records2 = [_make_record(remedial_action=[])]
    df2 = _flatten_devices(records2)
    assert df2.iloc[0]["remedial_action_flag"] == False  # noqa: E712


# ── _aggregate_patient_outcomes ──────────────────────────────────────────────


def test_outcomes_death():
    records = [_make_record(patients=[{"sequence_number_outcome": ["D"]}])]
    df = _aggregate_patient_outcomes(records)
    assert df.iloc[0]["has_death"]
    assert df.iloc[0]["has_serious_injury"] == False  # noqa: E712


def test_outcomes_serious_injury():
    records = [_make_record(patients=[{"sequence_number_outcome": ["L"]}])]
    df = _aggregate_patient_outcomes(records)
    assert df.iloc[0]["has_death"] == False  # noqa: E712
    assert df.iloc[0]["has_serious_injury"]


def test_outcomes_no_patients():
    records = [_make_record()]  # no patient key
    df = _aggregate_patient_outcomes(records)
    assert df.iloc[0]["has_death"] == False  # noqa: E712
    assert df.iloc[0]["has_serious_injury"] == False  # noqa: E712


def test_outcomes_multi_patient():
    records = [
        _make_record(
            patients=[
                {"sequence_number_outcome": ["H"]},
                {"sequence_number_outcome": ["D"]},
            ]
        )
    ]
    df = _aggregate_patient_outcomes(records)
    assert df.iloc[0]["has_death"]
    assert df.iloc[0]["has_serious_injury"]


def test_outcomes_string_outcome():
    """Handle outcome as a single string instead of list."""
    records = [_make_record(patients=[{"sequence_number_outcome": "D"}])]
    df = _aggregate_patient_outcomes(records)
    assert df.iloc[0]["has_death"]


def test_outcomes_death_full_text():
    """Real-world data uses 'Death' (full text) for death outcome."""
    records = [_make_record(patients=[{"sequence_number_outcome": ["Death"]}])]
    df = _aggregate_patient_outcomes(records)
    assert df.iloc[0]["has_death"]
    assert df.iloc[0]["has_serious_injury"] == False  # noqa: E712


def test_outcomes_death_space_prefixed():
    """Real-world data uses ' D' (space-prefixed code) for death outcome."""
    records = [_make_record(patients=[{"sequence_number_outcome": [" D"]}])]
    df = _aggregate_patient_outcomes(records)
    assert df.iloc[0]["has_death"]


def test_outcomes_serious_injury_full_text():
    """Real-world data uses full text like 'Hospitalization', 'Life Threatening'."""
    records = [_make_record(patients=[{"sequence_number_outcome": ["Hospitalization", "Life Threatening"]}])]
    df = _aggregate_patient_outcomes(records)
    assert df.iloc[0]["has_serious_injury"]
    assert df.iloc[0]["has_death"] == False  # noqa: E712


def test_outcomes_serious_injury_space_prefixed():
    """Real-world data uses ' H', ' L', ' S' (space-prefixed codes)."""
    records = [_make_record(patients=[{"sequence_number_outcome": [" H"]}])]
    df = _aggregate_patient_outcomes(records)
    assert df.iloc[0]["has_serious_injury"]


def test_outcomes_mixed_formats():
    """Mix of full-text and code formats in one record."""
    records = [
        _make_record(
            patients=[
                {"sequence_number_outcome": ["Death"]},
                {"sequence_number_outcome": [" L", "Other"]},
            ]
        )
    ]
    df = _aggregate_patient_outcomes(records)
    assert df.iloc[0]["has_death"]
    assert df.iloc[0]["has_serious_injury"]


def test_outcomes_disability_maps_to_serious():
    """'Disability' and 'Congenital Anomaly' map to serious injury (code S)."""
    records = [_make_record(patients=[{"sequence_number_outcome": ["Disability"]}])]
    df = _aggregate_patient_outcomes(records)
    assert df.iloc[0]["has_serious_injury"]


def test_outcomes_required_intervention_not_serious():
    """'Required Intervention' is non-serious — should not set either flag."""
    records = [_make_record(patients=[{"sequence_number_outcome": ["Required Intervention"]}])]
    df = _aggregate_patient_outcomes(records)
    assert df.iloc[0]["has_death"] == False  # noqa: E712
    assert df.iloc[0]["has_serious_injury"] == False  # noqa: E712


def test_outcomes_empty_string_ignored():
    """Empty string outcomes (most common in real data) should not trigger flags."""
    records = [_make_record(patients=[{"sequence_number_outcome": [""]}])]
    df = _aggregate_patient_outcomes(records)
    assert df.iloc[0]["has_death"] == False  # noqa: E712
    assert df.iloc[0]["has_serious_injury"] == False  # noqa: E712


# ── _normalize_dates ─────────────────────────────────────────────────────────


def test_normalize_dates_valid():
    df = pd.DataFrame({"date_received": ["20230115", "20220301"]})
    result = _normalize_dates(df)
    assert result["date_received"].iloc[0] == pd.Timestamp("2023-01-15")
    assert result["event_year"].iloc[0] == 2023


def test_normalize_dates_invalid():
    df = pd.DataFrame({"date_received": ["BADDATE", "20220301"]})
    result = _normalize_dates(df)
    assert pd.isna(result["date_received"].iloc[0])
    assert result["event_year"].iloc[1] == 2022


# ── _normalize_flags ─────────────────────────────────────────────────────────


def test_normalize_flags():
    df = pd.DataFrame({"adverse_event_flag": ["Y", "N"], "product_problem_flag": ["N", "Y"]})
    result = _normalize_flags(df)
    assert result["adverse_event_flag"].iloc[0]
    assert result["product_problem_flag"].iloc[1]


# ── _dedup_reports ───────────────────────────────────────────────────────────


def test_dedup_multi_version():
    df = pd.DataFrame(
        {
            "mdr_report_key": ["A", "A", "B"],
            "date_received": pd.to_datetime(["2023-01-15", "2023-03-20", "2023-02-01"]),
            "event_record_id": ["A_0_v1", "A_0_v2", "B_0"],
        }
    )
    result = _dedup_reports(df)
    # Latest for A is 2023-03-20
    a_latest = result[(result["mdr_report_key"] == "A") & (result["is_latest_version"])]
    assert len(a_latest) == 1
    assert a_latest.iloc[0]["date_received"] == pd.Timestamp("2023-03-20")


def test_dedup_single_version():
    df = pd.DataFrame(
        {
            "mdr_report_key": ["X"],
            "date_received": pd.to_datetime(["2023-06-01"]),
            "event_record_id": ["X_0"],
        }
    )
    result = _dedup_reports(df)
    assert result.iloc[0]["followup_rank"] == 1
    assert result.iloc[0]["is_latest_version"]


# ── End-to-end ───────────────────────────────────────────────────────────────


def test_clean_adverse_events_end_to_end(tmp_path):
    """Full pipeline from ZIP to parquet."""
    input_dir = tmp_path / "bulk"
    input_dir.mkdir()
    output_path = tmp_path / "clean.parquet"

    records = [
        _make_record(
            mdr_key="R1",
            date_received="20230101",
            patients=[{"sequence_number_outcome": ["D"]}],
            devices=[{"device_report_product_code": "XYZ", "brand_name": "Brand1", "manufacturer_d_name": "MFG"}],
        ),
        _make_record(
            mdr_key="R2",
            date_received="20230215",
            event_type="Malfunction",
            devices=[
                {"device_report_product_code": "ABC", "brand_name": "Brand2"},
                {"device_report_product_code": "DEF", "brand_name": "Brand3"},
            ],
        ),
    ]
    _make_zip(input_dir, records, "part1.zip")

    df = clean_adverse_events(input_dir=input_dir, output_path=output_path)

    assert output_path.exists()
    assert len(df) == 3  # 1 device from R1 + 2 devices from R2
    assert df["event_record_id"].is_unique

    # R1 has death
    r1 = df[df["mdr_report_key"] == "R1"]
    assert r1.iloc[0]["has_death"]

    # R2 is malfunction
    r2 = df[df["mdr_report_key"] == "R2"]
    assert r2.iloc[0]["has_malfunction"]

    # Check schema
    assert "event_year" in df.columns
    assert "is_latest_version" in df.columns


def test_clean_adverse_events_empty(tmp_path):
    """Empty input directory produces empty parquet."""
    input_dir = tmp_path / "empty"
    input_dir.mkdir()
    output = tmp_path / "out.parquet"

    df = clean_adverse_events(input_dir=input_dir, output_path=output)
    assert len(df) == 0
    assert output.exists()
