"""Tests for app-layer CSV export."""

import pandas as pd
import pytest

from src.marts.builder import build_all_marts
from src.marts.export import (
    export_all,
    export_app_category_product,
    export_app_manufacturer,
    export_app_methodology,
    export_app_overview,
)


@pytest.fixture
def built_data(tmp_path):
    """Build synthetic clean data and mart tables for export testing."""
    clean_dir = tmp_path / "clean"
    clean_dir.mkdir()
    mart_dir = tmp_path / "mart"
    app_dir = tmp_path / "app"

    # dim_product_code
    dim_pc = pd.DataFrame(
        {
            "product_code": ["ABC", "DEF"],
            "device_name": ["Widget A", "Widget B"],
            "review_panel": ["AN", "CV"],
            "device_class": ["Class II", "Class III"],
            "medical_specialty": ["AN", "CV"],
            "medical_specialty_description": ["Anesthesiology", "Cardiovascular"],
            "implant_flag": [False, True],
            "life_sustain_support_flag": [False, True],
            "regulation_number": ["868.1234", "870.5678"],
        }
    )
    dim_pc.to_parquet(clean_dir / "dim_product_code.parquet", index=False)

    # events
    events = pd.DataFrame(
        {
            "event_record_id": [f"EVT_{i}" for i in range(6)],
            "mdr_report_key": [f"MDR_{i}" for i in range(6)],
            "date_received": pd.to_datetime(["2022-01-01"] * 3 + ["2023-06-15"] * 3),
            "event_year": pd.array([2022] * 3 + [2023] * 3, dtype="Int64"),
            "product_code": ["ABC", "ABC", "DEF", "ABC", "DEF", "DEF"],
            "brand_name": ["B"] * 6,
            "generic_name": ["G"] * 6,
            "manufacturer_d_name": ["Firm X", "Firm Y", "Firm X", "Firm X", "Firm Y", "Firm X"],
            "event_type": ["Malfunction"] * 6,
            "adverse_event_flag": [True] * 6,
            "product_problem_flag": [False] * 6,
            "has_death": [False] * 6,
            "has_serious_injury": [False] * 6,
            "has_malfunction": [True] * 6,
            "source_type": ["manufacturer"] * 6,
            "remedial_action_flag": [False] * 6,
            "followup_rank": [1] * 6,
            "is_latest_version": [True] * 6,
        }
    )
    events.to_parquet(clean_dir / "clean_event_device_level.parquet", index=False)

    # recalls
    recalls = pd.DataFrame(
        {
            "recall_number": ["RCL001", "RCL002"],
            "recall_initiation_date": pd.to_datetime(["2022-03-01", "2023-01-15"]),
            "recall_year": pd.array([2022, 2023], dtype="Int64"),
            "recall_class": ["Class I", "Class II"],
            "product_code": ["ABC", "DEF"],
            "product_description": ["Desc A", "Desc B"],
            "recalling_firm": ["Firm X", "Firm Y"],
            "reason_for_recall": ["Reason"] * 2,
            "status": ["Ongoing"] * 2,
            "voluntary_mandated": ["Voluntary"] * 2,
            "mapping_quality": ["exact_product_code_match"] * 2,
            "matched_product_code": ["ABC", "DEF"],
            "match_score": [100.0, 100.0],
            "include_in_core_dashboard": [True, True],
        }
    )
    recalls.to_parquet(clean_dir / "clean_recall.parquet", index=False)

    # clearances
    clearances = pd.DataFrame(
        {
            "k_number": [f"K{i:06d}" for i in range(12)],
            "decision_date": pd.to_datetime(["2022-06-01"] * 6 + ["2023-04-01"] * 6),
            "decision_year": pd.array([2022] * 6 + [2023] * 6, dtype="Int64"),
            "product_code": ["ABC"] * 4 + ["DEF"] * 2 + ["ABC"] * 3 + ["DEF"] * 3,
            "applicant": ["App"] * 12,
            "applicant_std": ["APP"] * 12,
            "advisory_committee": ["AN"] * 4 + ["CV"] * 2 + ["AN"] * 3 + ["CV"] * 3,
            "clearance_type": ["Traditional"] * 12,
            "decision_code": ["SESE"] * 12,
        }
    )
    clearances.to_parquet(clean_dir / "clean_510k.parquet", index=False)

    # Build marts
    build_all_marts(
        events_path=clean_dir / "clean_event_device_level.parquet",
        recalls_path=clean_dir / "clean_recall.parquet",
        clearances_path=clean_dir / "clean_510k.parquet",
        dim_product_code_path=clean_dir / "dim_product_code.parquet",
        output_dir=mart_dir,
    )

    return {
        "clean_dir": clean_dir,
        "mart_dir": mart_dir,
        "app_dir": app_dir,
    }


class TestExportOverview:
    def test_csv_created(self, built_data):
        out = built_data["app_dir"] / "app_overview.csv"
        export_app_overview(
            mart_panel_year_path=built_data["mart_dir"] / "mart_panel_year.parquet",
            output_path=out,
        )
        assert out.exists()

    def test_has_expected_columns(self, built_data):
        df = export_app_overview(
            mart_panel_year_path=built_data["mart_dir"] / "mart_panel_year.parquet",
            output_path=built_data["app_dir"] / "app_overview.csv",
        )
        assert "review_panel" in df.columns
        assert "events_per_100_clearances" in df.columns


class TestExportCategoryProduct:
    def test_csv_created(self, built_data):
        out = built_data["app_dir"] / "app_category_product.csv"
        export_app_category_product(
            mart_pc_year_path=built_data["mart_dir"] / "mart_product_code_year.parquet",
            dim_product_code_path=built_data["clean_dir"] / "dim_product_code.parquet",
            output_path=out,
        )
        assert out.exists()

    def test_enriched_with_dimension(self, built_data):
        df = export_app_category_product(
            mart_pc_year_path=built_data["mart_dir"] / "mart_product_code_year.parquet",
            dim_product_code_path=built_data["clean_dir"] / "dim_product_code.parquet",
            output_path=built_data["app_dir"] / "app_category_product.csv",
        )
        assert "device_name" in df.columns
        assert "review_panel" in df.columns
        assert "medical_specialty_description" in df.columns


class TestExportManufacturer:
    def test_csv_created(self, built_data):
        out = built_data["app_dir"] / "app_manufacturer.csv"
        export_app_manufacturer(
            mart_firm_product_year_path=built_data["mart_dir"] / "mart_firm_product_year.parquet",
            dim_product_code_path=built_data["clean_dir"] / "dim_product_code.parquet",
            output_path=out,
        )
        assert out.exists()

    def test_enriched_columns(self, built_data):
        df = export_app_manufacturer(
            mart_firm_product_year_path=built_data["mart_dir"] / "mart_firm_product_year.parquet",
            dim_product_code_path=built_data["clean_dir"] / "dim_product_code.parquet",
            output_path=built_data["app_dir"] / "app_manufacturer.csv",
        )
        assert "device_name" in df.columns
        assert "review_panel" in df.columns


class TestExportMethodology:
    def test_csv_created(self, built_data):
        out = built_data["app_dir"] / "app_methodology.csv"
        export_app_methodology(
            events_path=built_data["clean_dir"] / "clean_event_device_level.parquet",
            recalls_path=built_data["clean_dir"] / "clean_recall.parquet",
            clearances_path=built_data["clean_dir"] / "clean_510k.parquet",
            dim_product_code_path=built_data["clean_dir"] / "dim_product_code.parquet",
            output_path=out,
        )
        assert out.exists()

    def test_has_source_rows(self, built_data):
        df = export_app_methodology(
            events_path=built_data["clean_dir"] / "clean_event_device_level.parquet",
            recalls_path=built_data["clean_dir"] / "clean_recall.parquet",
            clearances_path=built_data["clean_dir"] / "clean_510k.parquet",
            dim_product_code_path=built_data["clean_dir"] / "dim_product_code.parquet",
            output_path=built_data["app_dir"] / "app_methodology.csv",
        )
        assert "source" in df.columns
        assert "metric" in df.columns
        assert "value" in df.columns
        sources = df["source"].unique()
        assert "adverse_events" in sources
        assert "recalls" in sources
        assert "clearances" in sources


class TestExportAll:
    def test_returns_four_tables(self, built_data):
        result = export_all(
            output_dir=built_data["app_dir"],
            mart_dir=built_data["mart_dir"],
            clean_dir=built_data["clean_dir"],
        )
        assert len(result) == 4
        for name, df in result.items():
            assert len(df) > 0, f"{name} should have rows"

    def test_all_csvs_written(self, built_data):
        export_all(
            output_dir=built_data["app_dir"],
            mart_dir=built_data["mart_dir"],
            clean_dir=built_data["clean_dir"],
        )
        for name in ["app_overview", "app_category_product", "app_manufacturer", "app_methodology"]:
            assert (built_data["app_dir"] / f"{name}.csv").exists()
