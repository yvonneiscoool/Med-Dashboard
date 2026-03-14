"""Tests for QA summary report."""

import pandas as pd
import pytest

from src.marts.builder import build_all_marts
from src.marts.export import export_all
from src.qa.summary import build_qa_summary, evaluate_quality_gate


@pytest.fixture
def full_pipeline(tmp_path):
    """Build complete pipeline from synthetic data for QA testing."""
    clean_dir = tmp_path / "clean"
    clean_dir.mkdir()
    mart_dir = tmp_path / "mart"
    app_dir = tmp_path / "app"

    # dim_product_code
    pd.DataFrame(
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
    ).to_parquet(clean_dir / "dim_product_code.parquet", index=False)

    # events
    pd.DataFrame(
        {
            "event_record_id": ["E1", "E2", "E3", "E4"],
            "mdr_report_key": ["M1", "M2", "M3", "M4"],
            "date_received": pd.to_datetime(["2022-01-01"] * 2 + ["2023-06-15"] * 2),
            "event_year": pd.array([2022, 2022, 2023, 2023], dtype="Int64"),
            "product_code": ["ABC", "DEF", "ABC", "DEF"],
            "brand_name": ["B"] * 4,
            "generic_name": ["G"] * 4,
            "manufacturer_d_name": ["Firm X", "Firm Y", "Firm X", "Firm Y"],
            "event_type": ["Malfunction"] * 4,
            "adverse_event_flag": [True] * 4,
            "product_problem_flag": [False] * 4,
            "has_death": [False] * 4,
            "has_serious_injury": [False] * 4,
            "has_malfunction": [True] * 4,
            "source_type": ["manufacturer"] * 4,
            "remedial_action_flag": [False] * 4,
            "followup_rank": [1] * 4,
            "is_latest_version": [True] * 4,
        }
    ).to_parquet(clean_dir / "clean_event_device_level.parquet", index=False)

    # recalls
    pd.DataFrame(
        {
            "recall_number": ["R1", "R2"],
            "recall_initiation_date": pd.to_datetime(["2022-03-01", "2023-01-15"]),
            "recall_year": pd.array([2022, 2023], dtype="Int64"),
            "recall_class": ["Class I", "Class II"],
            "product_code": ["ABC", "DEF"],
            "product_description": ["D1", "D2"],
            "recalling_firm": ["Firm X", "Firm Y"],
            "reason_for_recall": ["R"] * 2,
            "status": ["Ongoing"] * 2,
            "voluntary_mandated": ["Voluntary"] * 2,
            "mapping_quality": ["exact_product_code_match"] * 2,
            "matched_product_code": ["ABC", "DEF"],
            "match_score": [100.0, 100.0],
            "include_in_core_dashboard": [True, True],
        }
    ).to_parquet(clean_dir / "clean_recall.parquet", index=False)

    # clearances
    pd.DataFrame(
        {
            "k_number": [f"K{i}" for i in range(12)],
            "decision_date": pd.to_datetime(["2022-06-01"] * 6 + ["2023-04-01"] * 6),
            "decision_year": pd.array([2022] * 6 + [2023] * 6, dtype="Int64"),
            "product_code": ["ABC"] * 6 + ["DEF"] * 6,
            "applicant": ["App"] * 12,
            "applicant_std": ["APP"] * 12,
            "advisory_committee": ["AN"] * 6 + ["CV"] * 6,
            "clearance_type": ["Traditional"] * 12,
            "decision_code": ["SESE"] * 12,
        }
    ).to_parquet(clean_dir / "clean_510k.parquet", index=False)

    # Build marts and exports
    build_all_marts(
        events_path=clean_dir / "clean_event_device_level.parquet",
        recalls_path=clean_dir / "clean_recall.parquet",
        clearances_path=clean_dir / "clean_510k.parquet",
        dim_product_code_path=clean_dir / "dim_product_code.parquet",
        output_dir=mart_dir,
    )
    export_all(output_dir=app_dir, mart_dir=mart_dir, clean_dir=clean_dir)

    return {"clean_dir": clean_dir, "mart_dir": mart_dir, "app_dir": app_dir}


class TestBuildQaSummary:
    def test_returns_dataframe(self, full_pipeline):
        df = build_qa_summary(
            clean_dir=full_pipeline["clean_dir"],
            mart_dir=full_pipeline["mart_dir"],
            app_dir=full_pipeline["app_dir"],
            output_path=full_pipeline["mart_dir"] / "qa_summary.parquet",
        )
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

    def test_has_expected_columns(self, full_pipeline):
        df = build_qa_summary(
            clean_dir=full_pipeline["clean_dir"],
            mart_dir=full_pipeline["mart_dir"],
            app_dir=full_pipeline["app_dir"],
            output_path=full_pipeline["mart_dir"] / "qa_summary.parquet",
        )
        for col in ["check_name", "status", "metric_value", "threshold", "details"]:
            assert col in df.columns

    def test_all_checks_pass_on_good_data(self, full_pipeline):
        df = build_qa_summary(
            clean_dir=full_pipeline["clean_dir"],
            mart_dir=full_pipeline["mart_dir"],
            app_dir=full_pipeline["app_dir"],
            output_path=full_pipeline["mart_dir"] / "qa_summary.parquet",
        )
        failed = df[df["status"] == "fail"]
        assert len(failed) == 0, f"Unexpected failures: {failed['check_name'].tolist()}"

    def test_output_file_created(self, full_pipeline):
        out = full_pipeline["mart_dir"] / "qa_summary.parquet"
        build_qa_summary(
            clean_dir=full_pipeline["clean_dir"],
            mart_dir=full_pipeline["mart_dir"],
            app_dir=full_pipeline["app_dir"],
            output_path=out,
        )
        assert out.exists()


class TestEvaluateQualityGate:
    def test_passes_with_no_failures(self):
        df = pd.DataFrame(
            {
                "check_name": ["a", "b"],
                "status": ["pass", "warn"],
                "metric_value": [1, 2],
                "threshold": [1, 1],
                "details": ["ok", "ok"],
            }
        )
        assert evaluate_quality_gate(df) is True

    def test_fails_with_failure(self):
        df = pd.DataFrame(
            {
                "check_name": ["a", "b"],
                "status": ["pass", "fail"],
                "metric_value": [1, 2],
                "threshold": [1, 1],
                "details": ["ok", "bad"],
            }
        )
        assert evaluate_quality_gate(df) is False

    def test_empty_passes(self):
        assert evaluate_quality_gate(pd.DataFrame()) is True
