"""Clean and flatten raw 510(k) clearance data into analysis-ready parquet."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from src.config import DATA_CLEAN, DATA_RAW, DATE_END, DATE_START
from src.mapping.manufacturer import _normalize_name

_DEFAULT_INPUT_DIR = DATA_RAW / "clearances"
_DEFAULT_OUTPUT = DATA_CLEAN / "clean_510k.parquet"

_OUTPUT_COLUMNS = [
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


def clean_clearances(
    input_dir: str | Path | None = None,
    output_path: str | Path | None = None,
) -> pd.DataFrame:
    """Clean 510(k) clearance data from raw JSON files into parquet.

    Args:
        input_dir: Directory containing year subdirs with clearance JSON files.
        output_path: Path to write parquet. Defaults to standard location.

    Returns:
        Cleaned DataFrame with one row per unique clearance.
    """
    input_dir = Path(input_dir) if input_dir else _DEFAULT_INPUT_DIR
    output_path = Path(output_path) if output_path else _DEFAULT_OUTPUT

    # Read all JSON files
    all_records = []
    for json_path in sorted(input_dir.glob("**/clearances_*.json")):
        all_records.extend(_read_year_json(json_path))

    if not all_records:
        df = pd.DataFrame(columns=_OUTPUT_COLUMNS)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
        return df

    # Extract fields
    df = _extract_fields(all_records)

    if df.empty:
        df = pd.DataFrame(columns=_OUTPUT_COLUMNS)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
        return df

    # Parse dates
    df = _parse_clearance_dates(df)

    # Filter to configured date window
    start_year = int(DATE_START[:4])
    end_year = int(DATE_END[:4])
    df = df[df["decision_year"].between(start_year, end_year)]

    # Standardize applicant names
    df = _standardize_applicant(df)

    # Dedup on k_number (keep first)
    df = df.drop_duplicates(subset=["k_number"], keep="first")
    df = df.reset_index(drop=True)

    # Enforce column order
    for col in _OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[_OUTPUT_COLUMNS]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    return df


def _read_year_json(json_path: Path) -> list[dict]:
    """Read a single year's JSON file, returning results array."""
    with open(json_path) as f:
        data = json.load(f)
    if isinstance(data, dict):
        return data.get("results", [])
    if isinstance(data, list):
        return data
    return []


def _extract_product_code(record: dict) -> str | None:
    """Extract product_code: prefer direct field, fallback to openfda."""
    # Direct field first
    code = record.get("product_code")
    if code:
        return code

    # Fallback to openfda
    openfda = record.get("openfda", {})
    if not openfda or not isinstance(openfda, dict):
        return None
    codes = openfda.get("product_code", [])
    if not codes or not isinstance(codes, list):
        return None
    return codes[0] if codes else None


def _extract_fields(records: list[dict]) -> pd.DataFrame:
    """Flatten clearance records into rows with target columns."""
    rows = []
    for rec in records:
        rows.append(
            {
                "k_number": rec.get("k_number"),
                "decision_date": rec.get("decision_date"),
                "product_code": _extract_product_code(rec),
                "applicant": rec.get("applicant"),
                "advisory_committee": rec.get("advisory_committee"),
                "clearance_type": rec.get("clearance_type"),
                "decision_code": rec.get("decision_code"),
            }
        )
    return pd.DataFrame(rows)


def _parse_clearance_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Parse decision_date and derive decision_year."""
    df = df.copy()
    df["decision_date"] = pd.to_datetime(df["decision_date"], format="mixed", errors="coerce")
    df["decision_year"] = df["decision_date"].dt.year.astype("Int64")
    return df


def _standardize_applicant(df: pd.DataFrame) -> pd.DataFrame:
    """Create applicant_std column via manufacturer name normalization."""
    df = df.copy()
    df["applicant_std"] = df["applicant"].apply(lambda x: _normalize_name(x) if isinstance(x, str) and x else None)
    return df
