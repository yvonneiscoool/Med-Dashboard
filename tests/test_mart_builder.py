"""Tests for mart builder using synthetic parquet data."""

import pandas as pd
import pytest

from src.marts.builder import (
    build_all_marts,
    build_mart_firm_product_year,
    build_mart_panel_year,
    build_mart_product_code_year,
)

# ── Fixtures: synthetic parquet data ──────────────────────────────────────────


@pytest.fixture
def synthetic_data(tmp_path):
    """Create synthetic parquet files mimicking clean layer schemas."""
    clean_dir = tmp_path / "clean"
    clean_dir.mkdir()

    # dim_product_code
    dim_pc = pd.DataFrame(
        {
            "product_code": ["ABC", "DEF", "GHI"],
            "device_name": ["Widget A", "Widget B", "Widget C"],
            "review_panel": ["AN", "CV", "AN"],
            "device_class": ["Class II", "Class III", "Class I"],
            "medical_specialty": ["AN", "CV", "AN"],
            "medical_specialty_description": ["Anesthesiology", "Cardiovascular", "Anesthesiology"],
            "implant_flag": [False, True, False],
            "life_sustain_support_flag": [False, True, False],
            "regulation_number": ["868.1234", "870.5678", "868.9012"],
        }
    )
    dim_pc.to_parquet(clean_dir / "dim_product_code.parquet", index=False)

    # events
    events = pd.DataFrame(
        {
            "event_record_id": [f"EVT_{i}" for i in range(10)],
            "mdr_report_key": [f"MDR_{i}" for i in range(10)],
            "date_received": pd.to_datetime(["2022-01-01"] * 5 + ["2023-06-15"] * 5),
            "event_year": pd.array([2022] * 5 + [2023] * 5, dtype="Int64"),
            "product_code": ["ABC"] * 3 + ["DEF"] * 2 + ["ABC"] * 2 + ["DEF"] * 2 + ["GHI"],
            "brand_name": ["Brand"] * 10,
            "generic_name": ["Generic"] * 10,
            "manufacturer_d_name": ["Firm X"] * 3 + ["Firm Y"] * 2 + ["Firm X"] * 2 + ["Firm Z"] * 2 + ["Firm X"],
            "event_type": ["Malfunction"] * 4 + ["Injury"] * 3 + ["Death"] * 3,
            "adverse_event_flag": [True] * 10,
            "product_problem_flag": [False] * 10,
            "has_death": [False] * 7 + [True] * 3,
            "has_serious_injury": [False] * 4 + [True] * 3 + [False] * 3,
            "has_malfunction": [True] * 4 + [False] * 6,
            "source_type": ["manufacturer"] * 10,
            "remedial_action_flag": [False] * 10,
            "followup_rank": [1] * 8 + [2, 1],
            "is_latest_version": [True] * 8 + [False, True],
        }
    )
    events.to_parquet(clean_dir / "clean_event_device_level.parquet", index=False)

    # recalls (with mapping columns from recall_product_code.py)
    recalls = pd.DataFrame(
        {
            "recall_number": ["RCL001", "RCL002", "RCL003", "RCL004"],
            "recall_initiation_date": pd.to_datetime(["2022-03-01", "2022-07-01", "2023-01-15", "2023-05-01"]),
            "recall_year": pd.array([2022, 2022, 2023, 2023], dtype="Int64"),
            "recall_class": ["Class I", "Class II", "Class I", "Class III"],
            "product_code": ["ABC", "DEF", "ABC", "XYZ"],
            "product_description": ["Desc A", "Desc B", "Desc C", "Desc D"],
            "recalling_firm": ["Firm X", "Firm Y", "Firm X", "Firm Z"],
            "reason_for_recall": ["Reason"] * 4,
            "status": ["Ongoing"] * 4,
            "voluntary_mandated": ["Voluntary"] * 4,
            "mapping_quality": [
                "exact_product_code_match",
                "exact_product_code_match",
                "exact_product_code_match",
                "low_confidence_text_match",
            ],
            "matched_product_code": ["ABC", "DEF", "ABC", "GHI"],
            "match_score": [100.0, 100.0, 100.0, 55.0],
            "include_in_core_dashboard": [True, True, True, False],
        }
    )
    recalls.to_parquet(clean_dir / "clean_recall.parquet", index=False)

    # clearances
    clearances = pd.DataFrame(
        {
            "k_number": [f"K{i:06d}" for i in range(15)],
            "decision_date": pd.to_datetime(["2022-06-01"] * 8 + ["2023-04-01"] * 7),
            "decision_year": pd.array([2022] * 8 + [2023] * 7, dtype="Int64"),
            "product_code": ["ABC"] * 5 + ["DEF"] * 3 + ["ABC"] * 4 + ["GHI"] * 3,
            "applicant": ["Applicant"] * 15,
            "applicant_std": ["APPLICANT"] * 15,
            "advisory_committee": ["AN"] * 5 + ["CV"] * 3 + ["AN"] * 4 + ["AN"] * 3,
            "clearance_type": ["Traditional"] * 15,
            "decision_code": ["SESE"] * 15,
        }
    )
    clearances.to_parquet(clean_dir / "clean_510k.parquet", index=False)

    return {
        "events": clean_dir / "clean_event_device_level.parquet",
        "recalls": clean_dir / "clean_recall.parquet",
        "clearances": clean_dir / "clean_510k.parquet",
        "dim_pc": clean_dir / "dim_product_code.parquet",
        "mart_dir": tmp_path / "mart",
    }


# ── mart_panel_year ───────────────────────────────────────────────────────────


class TestMartPanelYear:
    def test_grain_uniqueness(self, synthetic_data):
        df = build_mart_panel_year(
            events_path=synthetic_data["events"],
            recalls_path=synthetic_data["recalls"],
            clearances_path=synthetic_data["clearances"],
            dim_product_code_path=synthetic_data["dim_pc"],
            output_path=synthetic_data["mart_dir"] / "mart_panel_year.parquet",
        )
        assert df.duplicated(subset=["review_panel", "year"]).sum() == 0

    def test_output_file_created(self, synthetic_data):
        out = synthetic_data["mart_dir"] / "mart_panel_year.parquet"
        build_mart_panel_year(
            events_path=synthetic_data["events"],
            recalls_path=synthetic_data["recalls"],
            clearances_path=synthetic_data["clearances"],
            dim_product_code_path=synthetic_data["dim_pc"],
            output_path=out,
        )
        assert out.exists()

    def test_count_columns_are_int(self, synthetic_data):
        df = build_mart_panel_year(
            events_path=synthetic_data["events"],
            recalls_path=synthetic_data["recalls"],
            clearances_path=synthetic_data["clearances"],
            dim_product_code_path=synthetic_data["dim_pc"],
            output_path=synthetic_data["mart_dir"] / "mart_panel_year.parquet",
        )
        for col in ["event_count_raw", "event_count_dedup", "recall_count", "clearance_count"]:
            assert df[col].dtype in ("int64", "int32"), f"{col} should be int"

    def test_core_dashboard_filter(self, synthetic_data):
        """Recall RCL004 has include_in_core_dashboard=False, should be excluded."""
        df = build_mart_panel_year(
            events_path=synthetic_data["events"],
            recalls_path=synthetic_data["recalls"],
            clearances_path=synthetic_data["clearances"],
            dim_product_code_path=synthetic_data["dim_pc"],
            output_path=synthetic_data["mart_dir"] / "mart_panel_year.parquet",
        )
        # Total recalls in core dashboard = 3 (RCL001, RCL002, RCL003)
        assert df["recall_count"].sum() == 3

    def test_dedup_filter(self, synthetic_data):
        """event_count_dedup should exclude non-latest versions."""
        df = build_mart_panel_year(
            events_path=synthetic_data["events"],
            recalls_path=synthetic_data["recalls"],
            clearances_path=synthetic_data["clearances"],
            dim_product_code_path=synthetic_data["dim_pc"],
            output_path=synthetic_data["mart_dir"] / "mart_panel_year.parquet",
        )
        # 10 events total, 9 with is_latest_version=True
        assert df["event_count_raw"].sum() == 10
        assert df["event_count_dedup"].sum() == 9

    def test_null_kpi_thresholds(self, synthetic_data):
        """KPIs should be NULL when clearance count is below threshold."""
        df = build_mart_panel_year(
            events_path=synthetic_data["events"],
            recalls_path=synthetic_data["recalls"],
            clearances_path=synthetic_data["clearances"],
            dim_product_code_path=synthetic_data["dim_pc"],
            output_path=synthetic_data["mart_dir"] / "mart_panel_year.parquet",
        )
        # CV panel has only 3 clearances (2022) → below threshold of 10
        cv_rows = df[(df["review_panel"] == "CV") & (df["clearance_count"] < 10)]
        if not cv_rows.empty:
            assert cv_rows["events_per_100_clearances"].isna().all()


# ── mart_product_code_year ────────────────────────────────────────────────────


class TestMartProductCodeYear:
    def test_grain_uniqueness(self, synthetic_data):
        df = build_mart_product_code_year(
            events_path=synthetic_data["events"],
            recalls_path=synthetic_data["recalls"],
            clearances_path=synthetic_data["clearances"],
            output_path=synthetic_data["mart_dir"] / "mart_product_code_year.parquet",
        )
        assert df.duplicated(subset=["product_code", "year"]).sum() == 0

    def test_has_recall_to_event_ratio(self, synthetic_data):
        df = build_mart_product_code_year(
            events_path=synthetic_data["events"],
            recalls_path=synthetic_data["recalls"],
            clearances_path=synthetic_data["clearances"],
            output_path=synthetic_data["mart_dir"] / "mart_product_code_year.parquet",
        )
        assert "recall_to_event_ratio" in df.columns

    def test_output_file_created(self, synthetic_data):
        out = synthetic_data["mart_dir"] / "mart_product_code_year.parquet"
        build_mart_product_code_year(
            events_path=synthetic_data["events"],
            recalls_path=synthetic_data["recalls"],
            clearances_path=synthetic_data["clearances"],
            output_path=out,
        )
        assert out.exists()


# ── mart_firm_product_year ────────────────────────────────────────────────────


class TestMartFirmProductYear:
    def test_grain_uniqueness(self, synthetic_data):
        df = build_mart_firm_product_year(
            events_path=synthetic_data["events"],
            recalls_path=synthetic_data["recalls"],
            dim_product_code_path=synthetic_data["dim_pc"],
            output_path=synthetic_data["mart_dir"] / "mart_firm_product_year.parquet",
        )
        assert df.duplicated(subset=["manufacturer", "product_code", "year"]).sum() == 0

    def test_firm_share_within_product(self, synthetic_data):
        df = build_mart_firm_product_year(
            events_path=synthetic_data["events"],
            recalls_path=synthetic_data["recalls"],
            dim_product_code_path=synthetic_data["dim_pc"],
            output_path=synthetic_data["mart_dir"] / "mart_firm_product_year.parquet",
        )
        assert "firm_share_within_product" in df.columns
        # Shares within a product-year should sum to ~1
        for _, grp in df.groupby(["product_code", "year"]):
            total = grp["firm_share_within_product"].sum()
            if not pd.isna(total):
                assert abs(total - 1.0) < 0.01

    def test_firm_share_within_panel(self, synthetic_data):
        df = build_mart_firm_product_year(
            events_path=synthetic_data["events"],
            recalls_path=synthetic_data["recalls"],
            dim_product_code_path=synthetic_data["dim_pc"],
            output_path=synthetic_data["mart_dir"] / "mart_firm_product_year.parquet",
        )
        assert "firm_share_within_panel" in df.columns

    def test_output_file_created(self, synthetic_data):
        out = synthetic_data["mart_dir"] / "mart_firm_product_year.parquet"
        build_mart_firm_product_year(
            events_path=synthetic_data["events"],
            recalls_path=synthetic_data["recalls"],
            dim_product_code_path=synthetic_data["dim_pc"],
            output_path=out,
        )
        assert out.exists()


# ── build_all_marts ──────────────────────────────────────────────────────────


class TestBuildAllMarts:
    def test_returns_three_tables(self, synthetic_data):
        result = build_all_marts(
            events_path=synthetic_data["events"],
            recalls_path=synthetic_data["recalls"],
            clearances_path=synthetic_data["clearances"],
            dim_product_code_path=synthetic_data["dim_pc"],
            output_dir=synthetic_data["mart_dir"],
        )
        assert set(result.keys()) == {
            "mart_panel_year",
            "mart_product_code_year",
            "mart_firm_product_year",
        }
        for name, df in result.items():
            assert len(df) > 0, f"{name} should have rows"

    def test_all_parquets_written(self, synthetic_data):
        build_all_marts(
            events_path=synthetic_data["events"],
            recalls_path=synthetic_data["recalls"],
            clearances_path=synthetic_data["clearances"],
            dim_product_code_path=synthetic_data["dim_pc"],
            output_dir=synthetic_data["mart_dir"],
        )
        for name in ["mart_panel_year", "mart_product_code_year", "mart_firm_product_year"]:
            assert (synthetic_data["mart_dir"] / f"{name}.parquet").exists()


# ── Empty inputs ──────────────────────────────────────────────────────────────


class TestEmptyInputs:
    @pytest.fixture
    def empty_data(self, tmp_path):
        clean_dir = tmp_path / "clean"
        clean_dir.mkdir()

        pd.DataFrame(
            columns=[
                "product_code",
                "device_name",
                "review_panel",
                "device_class",
                "medical_specialty",
                "medical_specialty_description",
                "implant_flag",
                "life_sustain_support_flag",
                "regulation_number",
            ]
        ).to_parquet(clean_dir / "dim_product_code.parquet", index=False)

        pd.DataFrame(
            columns=[
                "event_record_id",
                "mdr_report_key",
                "date_received",
                "event_year",
                "product_code",
                "brand_name",
                "generic_name",
                "manufacturer_d_name",
                "event_type",
                "adverse_event_flag",
                "product_problem_flag",
                "has_death",
                "has_serious_injury",
                "has_malfunction",
                "source_type",
                "remedial_action_flag",
                "followup_rank",
                "is_latest_version",
            ]
        ).to_parquet(clean_dir / "clean_event_device_level.parquet", index=False)

        pd.DataFrame(
            columns=[
                "recall_number",
                "recall_initiation_date",
                "recall_year",
                "recall_class",
                "product_code",
                "product_description",
                "recalling_firm",
                "reason_for_recall",
                "status",
                "voluntary_mandated",
                "mapping_quality",
                "matched_product_code",
                "match_score",
                "include_in_core_dashboard",
            ]
        ).to_parquet(clean_dir / "clean_recall.parquet", index=False)

        pd.DataFrame(
            columns=[
                "k_number",
                "decision_date",
                "decision_year",
                "product_code",
                "applicant",
                "applicant_std",
                "advisory_committee",
                "clearance_type",
                "decision_code",
            ]
        ).to_parquet(clean_dir / "clean_510k.parquet", index=False)

        return {
            "events": clean_dir / "clean_event_device_level.parquet",
            "recalls": clean_dir / "clean_recall.parquet",
            "clearances": clean_dir / "clean_510k.parquet",
            "dim_pc": clean_dir / "dim_product_code.parquet",
            "mart_dir": tmp_path / "mart",
        }

    def test_empty_panel_year(self, empty_data):
        df = build_mart_panel_year(
            events_path=empty_data["events"],
            recalls_path=empty_data["recalls"],
            clearances_path=empty_data["clearances"],
            dim_product_code_path=empty_data["dim_pc"],
            output_path=empty_data["mart_dir"] / "mart_panel_year.parquet",
        )
        assert len(df) == 0

    def test_empty_product_code_year(self, empty_data):
        df = build_mart_product_code_year(
            events_path=empty_data["events"],
            recalls_path=empty_data["recalls"],
            clearances_path=empty_data["clearances"],
            output_path=empty_data["mart_dir"] / "mart_product_code_year.parquet",
        )
        assert len(df) == 0

    def test_empty_firm_product_year(self, empty_data):
        df = build_mart_firm_product_year(
            events_path=empty_data["events"],
            recalls_path=empty_data["recalls"],
            dim_product_code_path=empty_data["dim_pc"],
            output_path=empty_data["mart_dir"] / "mart_firm_product_year.parquet",
        )
        assert len(df) == 0
