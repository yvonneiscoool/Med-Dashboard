"""Map recall records to device classification via product code and text matching."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from rapidfuzz import fuzz, process

from src.config import DATA_CLEAN

_DEFAULT_RECALL_PATH = DATA_CLEAN / "clean_recall.parquet"
_DEFAULT_DIM_PATH = DATA_CLEAN / "dim_product_code.parquet"
_DEFAULT_OUTPUT = DATA_CLEAN / "clean_recall.parquet"


def map_recall_to_classification(
    recall_df: pd.DataFrame | None = None,
    dim_product_code_path: str | Path | None = None,
    recall_path: str | Path | None = None,
    output_path: str | Path | None = None,
    high_threshold: int = 85,
    low_threshold: int = 60,
) -> pd.DataFrame:
    """Map recall records to classification dimension via product code and text.

    Args:
        recall_df: Recall DataFrame. If None, reads from recall_path.
        dim_product_code_path: Path to dim_product_code parquet.
        recall_path: Path to clean_recall parquet (used if recall_df is None).
        output_path: Path to write updated recall parquet.
        high_threshold: Min score for high-confidence text match (default 85).
        low_threshold: Min score for low-confidence text match (default 60).

    Returns:
        Recall DataFrame with mapping columns added.
    """
    if recall_df is None:
        recall_path = Path(recall_path) if recall_path else _DEFAULT_RECALL_PATH
        recall_df = pd.read_parquet(recall_path)

    dim_path = Path(dim_product_code_path) if dim_product_code_path else _DEFAULT_DIM_PATH
    output_path = Path(output_path) if output_path else _DEFAULT_OUTPUT

    dim_df = pd.read_parquet(dim_path)
    valid_codes = set(dim_df["product_code"].dropna().unique())

    # Build device name lookup: product_code -> device_name
    code_to_name = dict(zip(dim_df["product_code"].dropna(), dim_df["device_name"].dropna()))
    device_names = list(code_to_name.values())
    name_to_code = {v: k for k, v in code_to_name.items()}

    # Initialize mapping columns
    recall_df = recall_df.copy()
    recall_df["mapping_quality"] = "unmapped"
    recall_df["matched_product_code"] = None
    recall_df["match_score"] = None
    recall_df["include_in_core_dashboard"] = False

    if recall_df.empty:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        recall_df.to_parquet(output_path, index=False)
        return recall_df

    # Tier 1: exact product code match
    exact_mask = _exact_match(recall_df, valid_codes)
    recall_df.loc[exact_mask, "mapping_quality"] = "exact_product_code_match"
    recall_df.loc[exact_mask, "matched_product_code"] = recall_df.loc[exact_mask, "product_code"]
    recall_df.loc[exact_mask, "match_score"] = 100.0
    recall_df.loc[exact_mask, "include_in_core_dashboard"] = True

    # Tiers 2-3: text matching on unmatched records
    unmatched_mask = ~exact_mask
    if unmatched_mask.any() and device_names:
        text_results = _text_match(
            recall_df.loc[unmatched_mask],
            device_names,
            name_to_code,
            high_threshold,
            low_threshold,
        )

        for idx, result in text_results.items():
            recall_df.loc[idx, "mapping_quality"] = result["tier"]
            recall_df.loc[idx, "matched_product_code"] = result["matched_code"]
            recall_df.loc[idx, "match_score"] = result["score"]
            if result["tier"] == "high_confidence_text_match":
                recall_df.loc[idx, "include_in_core_dashboard"] = True

    output_path.parent.mkdir(parents=True, exist_ok=True)
    recall_df.to_parquet(output_path, index=False)
    return recall_df


def _exact_match(recall_df: pd.DataFrame, valid_codes: set) -> pd.Series:
    """Return boolean mask where recall's product_code exists in dim_product_code."""
    return recall_df["product_code"].notna() & recall_df["product_code"].isin(valid_codes)


def _text_match(
    unmatched_df: pd.DataFrame,
    device_names: list[str],
    name_to_code: dict[str, str],
    high_threshold: int,
    low_threshold: int,
) -> dict:
    """Match product_description against device_name list via rapidfuzz.

    Returns dict of {index: {tier, matched_code, score}}.
    """
    results = {}

    # Deduplicate descriptions for performance
    descriptions = unmatched_df["product_description"].dropna().unique().tolist()
    desc_results = {}

    for desc in descriptions:
        if not desc:
            continue
        match = process.extractOne(
            desc,
            device_names,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=low_threshold,
        )
        if match:
            matched_name, score, _ = match
            matched_code = name_to_code.get(matched_name)
            if score >= high_threshold:
                tier = "high_confidence_text_match"
            else:
                tier = "low_confidence_text_match"
            desc_results[desc] = {"tier": tier, "matched_code": matched_code, "score": float(score)}

    # Map back to DataFrame indices
    for idx, row in unmatched_df.iterrows():
        desc = row.get("product_description")
        if desc and desc in desc_results:
            results[idx] = desc_results[desc]

    return results
