"""Tests for recall_class value handling in mart builders."""

import pandas as pd
import pytest

from src.marts.builder import build_mart_panel_year


@pytest.fixture()
def mart_data(tmp_path):
    """Create minimal parquet files for mart building."""
    events = pd.DataFrame(
        {
            "product_code": ["ABC"],
            "event_year": [2023],
            "is_latest_version": [True],
            "has_death": [False],
            "has_serious_injury": [False],
            "has_malfunction": [True],
            "manufacturer_d_name": ["ACME"],
        }
    )
    events_path = tmp_path / "events.parquet"
    events.to_parquet(events_path, index=False)

    recalls = pd.DataFrame(
        {
            "recall_number": ["R1"],
            "recall_year": [2023],
            "recall_class": ["I"],
            "product_code": ["ABC"],
            "matched_product_code": ["ABC"],
            "match_score": [100.0],
            "mapping_quality": ["exact_product_code_match"],
            "include_in_core_dashboard": [True],
            "recalling_firm": ["ACME"],
            "product_description": ["Test"],
            "recall_initiation_date": [pd.Timestamp("2023-01-01")],
            "reason_for_recall": ["Defect"],
            "status": ["Ongoing"],
            "voluntary_mandated": ["Voluntary"],
        }
    )
    recalls_path = tmp_path / "recalls.parquet"
    recalls.to_parquet(recalls_path, index=False)

    clearances = pd.DataFrame(
        {
            "product_code": ["ABC"],
            "decision_year": [2023],
            "decision_date": [pd.Timestamp("2023-06-01")],
        }
    )
    clearances_path = tmp_path / "clearances.parquet"
    clearances.to_parquet(clearances_path, index=False)

    dim_pc = pd.DataFrame(
        {
            "product_code": ["ABC"],
            "device_name": ["Test Device"],
            "review_panel": ["AN"],
            "device_class": ["2"],
            "medical_specialty": ["AN"],
            "medical_specialty_description": ["Anesthesiology"],
            "implant_flag": ["N"],
            "life_sustain_support_flag": ["N"],
            "regulation_number": ["868.1234"],
        }
    )
    dim_path = tmp_path / "dim_pc.parquet"
    dim_pc.to_parquet(dim_path, index=False)

    return {
        "events": events_path,
        "recalls": recalls_path,
        "clearances": clearances_path,
        "dim_pc": dim_path,
        "output": tmp_path / "mart.parquet",
    }


def test_recall_class_i_counted(mart_data):
    """Class I recall with recall_class='I' should be counted in class_i_recall_count."""
    result = build_mart_panel_year(
        events_path=mart_data["events"],
        recalls_path=mart_data["recalls"],
        clearances_path=mart_data["clearances"],
        dim_product_code_path=mart_data["dim_pc"],
        output_path=mart_data["output"],
    )
    row = result[result["review_panel"] == "AN"]
    assert len(row) == 1
    assert row.iloc[0]["recall_count"] == 1
    assert row.iloc[0]["class_i_recall_count"] == 1
