"""Build the dim_product_code dimension table from raw classification data."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from src.config import DATA_CLEAN, DATA_RAW

_DEFAULT_INPUT = DATA_RAW / "classification" / "classification_all.json"
_DEFAULT_OUTPUT = DATA_CLEAN / "dim_product_code.parquet"

_COLUMNS = [
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


def build_dim_product_code(
    input_path: str | Path | None = None,
    output_path: str | Path | None = None,
) -> pd.DataFrame:
    """Build product code dimension table from raw classification JSON.

    Args:
        input_path: Path to raw classification JSON. Defaults to standard location.
        output_path: Path to write parquet output. Defaults to standard location.

    Returns:
        Cleaned DataFrame with one row per unique product code.
    """
    input_path = Path(input_path) if input_path else _DEFAULT_INPUT
    output_path = Path(output_path) if output_path else _DEFAULT_OUTPUT

    with open(input_path) as f:
        data = json.load(f)

    records = data.get("results", data) if isinstance(data, dict) else data
    df = pd.DataFrame(records)

    # Select and rename columns — missing fields become null
    for col in _COLUMNS:
        if col not in df.columns:
            df[col] = None

    df = df[_COLUMNS].copy()

    # Convert Y/N flags to boolean
    for flag_col in ("implant_flag", "life_sustain_support_flag"):
        df[flag_col] = df[flag_col].map({"Y": True, "N": False})

    # Dedup on product_code (keep first)
    df = df.drop_duplicates(subset=["product_code"], keep="first")
    df = df.reset_index(drop=True)

    # Write output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)

    return df
