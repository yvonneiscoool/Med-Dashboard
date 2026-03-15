"""Clean and flatten raw recall/enforcement data into analysis-ready parquet."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd

from src.config import DATA_CLEAN, DATA_RAW, DATE_END, DATE_START

_DEFAULT_INPUT_DIR = DATA_RAW / "recalls"
_DEFAULT_OUTPUT = DATA_CLEAN / "clean_recall.parquet"
_DEFAULT_RECALL_PC_DIR = DATA_RAW / "recall_product_codes"

_OUTPUT_COLUMNS = [
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
]

_RECALL_CLASS_RE = re.compile(r"Class\s+(I{1,3})")


def clean_recalls(
    input_dir: str | Path | None = None,
    output_path: str | Path | None = None,
    recall_pc_dir: str | Path | None = None,
) -> pd.DataFrame:
    """Clean recall/enforcement data from raw JSON files into parquet.

    Args:
        input_dir: Directory containing year subdirs with recall JSON files.
        output_path: Path to write parquet. Defaults to standard location.

    Returns:
        Cleaned DataFrame with one row per unique recall.
    """
    input_dir = Path(input_dir) if input_dir else _DEFAULT_INPUT_DIR
    output_path = Path(output_path) if output_path else _DEFAULT_OUTPUT

    # Read all JSON files
    all_records = []
    for json_path in sorted(input_dir.glob("**/recalls_*.json")):
        all_records.extend(_read_year_json(json_path))

    if not all_records:
        df = pd.DataFrame(columns=_OUTPUT_COLUMNS)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
        return df

    # Extract fields
    df = _extract_fields(all_records)

    # Enrich product_code from /device/recall.json lookup
    pc_dir = Path(recall_pc_dir) if recall_pc_dir else _DEFAULT_RECALL_PC_DIR
    df = _enrich_product_code(df, pc_dir)

    if df.empty:
        df = pd.DataFrame(columns=_OUTPUT_COLUMNS)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
        return df

    # Parse dates and recall class
    df = _parse_recall_dates(df)
    df = _parse_recall_class(df)

    # Filter to configured date window
    start_year = int(DATE_START[:4])
    end_year = int(DATE_END[:4])
    df = df[df["recall_year"].between(start_year, end_year)]

    # Dedup on recall_number (keep first)
    df = df.drop_duplicates(subset=["recall_number"], keep="first")
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


def _load_recall_pc_lookup(recall_pc_dir: Path) -> dict[str, str]:
    """Load product_res_number -> product_code mapping from recall API data."""
    lookup: dict[str, str] = {}
    for json_path in sorted(recall_pc_dir.glob("**/recall_product_codes_*.json")):
        with open(json_path) as f:
            records = json.load(f)
        if isinstance(records, dict):
            records = records.get("results", [])
        for rec in records:
            res_num = rec.get("product_res_number")
            pc = rec.get("product_code")
            if res_num and pc:
                lookup[res_num] = pc.rstrip("-")
    return lookup


def _enrich_product_code(df: pd.DataFrame, recall_pc_dir: Path) -> pd.DataFrame:
    """Fill missing product_code using /device/recall.json lookup.

    Only fills rows where product_code is null (preserves openfda values).
    """
    if not recall_pc_dir.exists():
        return df

    lookup = _load_recall_pc_lookup(recall_pc_dir)
    if not lookup:
        return df

    df = df.copy()
    missing_mask = df["product_code"].isna()
    df.loc[missing_mask, "product_code"] = df.loc[missing_mask, "recall_number"].map(lookup)
    return df


def _extract_product_code(record: dict) -> str | None:
    """Extract first product_code from record's openfda block."""
    openfda = record.get("openfda", {})
    if not openfda or not isinstance(openfda, dict):
        return None
    codes = openfda.get("product_code", [])
    if not codes or not isinstance(codes, list):
        return None
    return codes[0] if codes else None


def _extract_fields(records: list[dict]) -> pd.DataFrame:
    """Flatten recall records into rows with target columns."""
    rows = []
    for rec in records:
        rows.append(
            {
                "recall_number": rec.get("recall_number"),
                "recall_initiation_date": rec.get("recall_initiation_date"),
                "classification": rec.get("classification"),
                "product_code": _extract_product_code(rec),
                "product_description": rec.get("product_description"),
                "recalling_firm": rec.get("recalling_firm"),
                "reason_for_recall": rec.get("reason_for_recall"),
                "status": rec.get("status"),
                "voluntary_mandated": rec.get("voluntary_mandated"),
            }
        )
    return pd.DataFrame(rows)


def _parse_recall_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Parse recall_initiation_date and derive recall_year."""
    df = df.copy()
    df["recall_initiation_date"] = pd.to_datetime(df["recall_initiation_date"], format="mixed", errors="coerce")
    df["recall_year"] = df["recall_initiation_date"].dt.year.astype("Int64")
    return df


def _parse_recall_class(df: pd.DataFrame) -> pd.DataFrame:
    """Extract recall class (I/II/III) from classification text."""
    df = df.copy()

    def _extract_class(val):
        if not val or not isinstance(val, str):
            return None
        match = _RECALL_CLASS_RE.search(val)
        return match.group(1) if match else None

    df["recall_class"] = df["classification"].apply(_extract_class)
    df = df.drop(columns=["classification"])
    return df
