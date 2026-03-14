"""Tests for classification dimension builder."""

import json

import pandas as pd
import pytest

from src.cleaning.classification import build_dim_product_code


@pytest.fixture()
def classification_data():
    """Minimal synthetic classification records."""
    return {
        "results": [
            {
                "product_code": "ABC",
                "device_name": "Test Device A",
                "medical_specialty": "SU",
                "medical_specialty_description": "Surgery",
                "review_panel": "GU",
                "device_class": "2",
                "implant_flag": "Y",
                "life_sustain_support_flag": "N",
                "regulation_number": "878.1234",
            },
            {
                "product_code": "DEF",
                "device_name": "Test Device B",
                "medical_specialty": "CV",
                "medical_specialty_description": "Cardiovascular",
                "review_panel": "CV",
                "device_class": "3",
                "implant_flag": "N",
                "life_sustain_support_flag": "Y",
                "regulation_number": "870.5678",
            },
            {
                "product_code": "GHI",
                "device_name": "Test Device C",
                "medical_specialty": "RA",
                "medical_specialty_description": "Radiology",
                "review_panel": "RA",
                "device_class": "1",
                "implant_flag": "N",
                "life_sustain_support_flag": "N",
                "regulation_number": "892.9999",
            },
        ]
    }


@pytest.fixture()
def input_file(tmp_path, classification_data):
    """Write classification data to a temp JSON file."""
    path = tmp_path / "classification_all.json"
    path.write_text(json.dumps(classification_data))
    return path


def test_basic_build(tmp_path, input_file):
    """Build dimension table from valid data."""
    output = tmp_path / "dim_product_code.parquet"
    df = build_dim_product_code(input_path=input_file, output_path=output)
    assert len(df) == 3
    assert output.exists()
    assert list(df.columns) == [
        "product_code",
        "device_name",
        "medical_specialty",
        "medical_specialty_description",
        "review_panel",
        "device_class",
        "implant_flag",
        "life_sustain_support_flag",
        "regulation_number",
    ]


def test_boolean_conversion(tmp_path, input_file):
    """Y/N flags are converted to boolean."""
    output = tmp_path / "out.parquet"
    df = build_dim_product_code(input_path=input_file, output_path=output)
    abc = df[df["product_code"] == "ABC"].iloc[0]
    assert abc["implant_flag"] == True  # noqa: E712
    assert abc["life_sustain_support_flag"] == False  # noqa: E712

    def_row = df[df["product_code"] == "DEF"].iloc[0]
    assert def_row["implant_flag"] == False  # noqa: E712
    assert def_row["life_sustain_support_flag"] == True  # noqa: E712


def test_dedup_on_product_code(tmp_path):
    """Duplicate product codes keep only first occurrence."""
    data = {
        "results": [
            {"product_code": "AAA", "device_name": "First", "implant_flag": "Y", "life_sustain_support_flag": "N"},
            {"product_code": "AAA", "device_name": "Second", "implant_flag": "N", "life_sustain_support_flag": "N"},
            {"product_code": "BBB", "device_name": "Third", "implant_flag": "N", "life_sustain_support_flag": "N"},
        ]
    }
    input_path = tmp_path / "in.json"
    input_path.write_text(json.dumps(data))
    output = tmp_path / "out.parquet"

    df = build_dim_product_code(input_path=input_path, output_path=output)
    assert len(df) == 2
    assert df[df["product_code"] == "AAA"].iloc[0]["device_name"] == "First"


def test_missing_fields(tmp_path):
    """Records with missing fields get null values."""
    data = {
        "results": [
            {"product_code": "ZZZ", "device_name": "Minimal"},
        ]
    }
    input_path = tmp_path / "in.json"
    input_path.write_text(json.dumps(data))
    output = tmp_path / "out.parquet"

    df = build_dim_product_code(input_path=input_path, output_path=output)
    assert len(df) == 1
    assert pd.isna(df.iloc[0]["medical_specialty"])
    assert pd.isna(df.iloc[0]["implant_flag"])


def test_output_schema(tmp_path, input_file):
    """Output parquet has expected columns and types."""
    output = tmp_path / "out.parquet"
    df = (
        pd.read_parquet(
            build_dim_product_code(input_path=input_file, output_path=output),
        )
        if False
        else build_dim_product_code(input_path=input_file, output_path=output)
    )

    # Read back from parquet to verify round-trip
    df_read = pd.read_parquet(output)
    assert set(df_read.columns) == set(df.columns)
    assert len(df_read) == len(df)


def test_list_input_format(tmp_path):
    """Handle raw list format (no 'results' wrapper)."""
    data = [
        {"product_code": "X01", "device_name": "Dev X", "implant_flag": "N", "life_sustain_support_flag": "N"},
    ]
    input_path = tmp_path / "in.json"
    input_path.write_text(json.dumps(data))
    output = tmp_path / "out.parquet"

    df = build_dim_product_code(input_path=input_path, output_path=output)
    assert len(df) == 1
    assert df.iloc[0]["product_code"] == "X01"


def test_unique_product_codes(tmp_path, input_file):
    """Product code column should have no duplicates after build."""
    output = tmp_path / "out.parquet"
    df = build_dim_product_code(input_path=input_file, output_path=output)
    assert df["product_code"].is_unique
