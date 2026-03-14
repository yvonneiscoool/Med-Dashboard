"""Clean and flatten raw adverse event data into analysis-ready parquet."""

from __future__ import annotations

import json
import zipfile
from pathlib import Path

import pandas as pd

from src.config import DATA_CLEAN, DATA_RAW, OUTCOME_DEATH, OUTCOME_SERIOUS_INJURY

_DEFAULT_INPUT_DIR = DATA_RAW / "adverse_events" / "bulk"
_DEFAULT_OUTPUT = DATA_CLEAN / "clean_event_device_level.parquet"


def clean_adverse_events(
    input_dir: str | Path | None = None,
    output_path: str | Path | None = None,
) -> pd.DataFrame:
    """Clean adverse events from raw ZIP files into device-level parquet.

    Args:
        input_dir: Directory containing ZIP files. Defaults to standard location.
        output_path: Path to write parquet. Defaults to standard location.

    Returns:
        Cleaned DataFrame at device-event grain.
    """
    input_dir = Path(input_dir) if input_dir else _DEFAULT_INPUT_DIR
    output_path = Path(output_path) if output_path else _DEFAULT_OUTPUT

    # Read all ZIP files
    all_records = []
    for zip_path in sorted(input_dir.glob("*.zip")):
        all_records.extend(_read_zip_json(zip_path))

    if not all_records:
        # Return empty DataFrame with correct schema
        df = pd.DataFrame(columns=_OUTPUT_COLUMNS)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
        return df

    # Aggregate patient outcomes (report-level)
    outcomes = _aggregate_patient_outcomes(all_records)

    # Flatten devices (device-level rows)
    df = _flatten_devices(all_records)

    if df.empty:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame(columns=_OUTPUT_COLUMNS)
        df.to_parquet(output_path, index=False)
        return df

    # Merge outcomes onto device rows
    if not outcomes.empty:
        df = df.merge(outcomes, on="mdr_report_key", how="left")
    for col in ("has_death", "has_serious_injury"):
        if col not in df.columns:
            df[col] = False
        df[col] = df[col].fillna(False)

    # Normalize dates and flags
    df = _normalize_dates(df)
    df = _normalize_flags(df)

    # Dedup to latest version
    df = _dedup_reports(df)

    # Ensure column order
    for col in _OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[_OUTPUT_COLUMNS]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    return df


_OUTPUT_COLUMNS = [
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


def _read_zip_json(zip_path: Path) -> list[dict]:
    """Read all JSON files from a ZIP, returning concatenated results arrays."""
    records = []
    with zipfile.ZipFile(zip_path, "r") as zf:
        for name in zf.namelist():
            if name.endswith(".json"):
                with zf.open(name) as f:
                    data = json.loads(f.read())
                    if isinstance(data, dict):
                        records.extend(data.get("results", []))
                    elif isinstance(data, list):
                        records.extend(data)
    return records


def _flatten_devices(records: list[dict]) -> pd.DataFrame:
    """Flatten each device entry into its own row."""
    rows = []
    for rec in records:
        mdr_key = rec.get("mdr_report_key")
        if not mdr_key:
            continue

        devices = rec.get("device", [])
        if not devices:
            continue

        event_type = rec.get("event_type", "")
        source_type = rec.get("source_type", "")
        if isinstance(source_type, list):
            source_type = source_type[0] if source_type else ""

        remedial_action = rec.get("remedial_action", [])
        remedial_flag = bool(remedial_action)

        has_malfunction = event_type == "Malfunction"

        for i, dev in enumerate(devices):
            rows.append(
                {
                    "event_record_id": f"{mdr_key}_{i}",
                    "mdr_report_key": mdr_key,
                    "date_received": rec.get("date_received"),
                    "product_code": dev.get("device_report_product_code"),
                    "brand_name": dev.get("brand_name"),
                    "generic_name": dev.get("generic_name"),
                    "manufacturer_d_name": dev.get("manufacturer_d_name"),
                    "event_type": event_type,
                    "adverse_event_flag": rec.get("adverse_event_flag"),
                    "product_problem_flag": rec.get("product_problem_flag"),
                    "has_malfunction": has_malfunction,
                    "source_type": source_type,
                    "remedial_action_flag": remedial_flag,
                }
            )
    return pd.DataFrame(rows)


def _aggregate_patient_outcomes(records: list[dict]) -> pd.DataFrame:
    """Aggregate patient outcomes per report into death/serious-injury flags."""
    rows = []
    for rec in records:
        mdr_key = rec.get("mdr_report_key")
        if not mdr_key:
            continue

        has_death = False
        has_serious = False

        patients = rec.get("patient", [])
        if patients:
            for pat in patients:
                outcomes = pat.get("sequence_number_outcome", [])
                if isinstance(outcomes, str):
                    outcomes = [outcomes]
                for outcome in outcomes:
                    if outcome in OUTCOME_DEATH:
                        has_death = True
                    if outcome in OUTCOME_SERIOUS_INJURY:
                        has_serious = True

        rows.append(
            {
                "mdr_report_key": mdr_key,
                "has_death": has_death,
                "has_serious_injury": has_serious,
            }
        )

    if not rows:
        return pd.DataFrame(columns=["mdr_report_key", "has_death", "has_serious_injury"])

    df = pd.DataFrame(rows)
    # A report can appear multiple times in raw data; aggregate with OR
    df = df.groupby("mdr_report_key", as_index=False).agg({"has_death": "max", "has_serious_injury": "max"})
    return df


def _normalize_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Convert YYYYMMDD date strings to datetime and derive event_year."""
    df = df.copy()
    df["date_received"] = pd.to_datetime(df["date_received"], format="%Y%m%d", errors="coerce")
    df["event_year"] = df["date_received"].dt.year.astype("Int64")
    return df


def _normalize_flags(df: pd.DataFrame) -> pd.DataFrame:
    """Convert Y/N string flags to boolean."""
    df = df.copy()
    for col in ("adverse_event_flag", "product_problem_flag"):
        if col in df.columns:
            df[col] = df[col].map({"Y": True, "N": False})
    return df


def _dedup_reports(df: pd.DataFrame) -> pd.DataFrame:
    """Rank report versions by date_received desc, keep latest."""
    df = df.copy()
    df = df.sort_values("date_received", ascending=False, na_position="last")
    df["followup_rank"] = df.groupby("mdr_report_key").cumcount() + 1
    df["is_latest_version"] = df["followup_rank"] == 1
    return df
