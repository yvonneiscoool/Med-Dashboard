"""Manufacturer name standardization via normalization + fuzzy clustering."""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
from rapidfuzz import fuzz, process

from src.config import DATA_CLEAN

_DEFAULT_INPUT = DATA_CLEAN / "clean_event_device_level.parquet"
_DEFAULT_OUTPUT = DATA_CLEAN / "dim_manufacturer_alias.parquet"
_DEFAULT_REVIEW = DATA_CLEAN / "manufacturer_manual_review.csv"

_LEGAL_SUFFIXES = re.compile(
    r"\b("
    r"INC|LLC|LTD|CORP|CORPORATION|CO|COMPANY|LP|LLP|GMBH|AG|SA|BV|NV|PLC"
    r"|PTY|INTERNATIONAL|INTL|HOLDINGS|GROUP|ENTERPRISES|ENTERPRISE"
    r"|INDUSTRIES|INDUSTRY|LIMITED|INCORPORATED"
    r")\b",
    re.IGNORECASE,
)


def build_manufacturer_alias(
    input_path: str | Path | None = None,
    output_path: str | Path | None = None,
    review_csv_path: str | Path | None = None,
    fuzzy_threshold: int = 90,
    top_n_review: int = 200,
) -> pd.DataFrame:
    """Build manufacturer alias dimension from event-level manufacturer names.

    Args:
        input_path: Path to clean event parquet with manufacturer_d_name column.
        output_path: Path to write alias parquet.
        review_csv_path: Path to write manual review CSV.
        fuzzy_threshold: Minimum fuzzy score for clustering (0-100).
        top_n_review: Number of top clusters to flag for manual review.

    Returns:
        Alias DataFrame mapping raw_name -> standardized_name.
    """
    input_path = Path(input_path) if input_path else _DEFAULT_INPUT
    output_path = Path(output_path) if output_path else _DEFAULT_OUTPUT
    review_csv_path = Path(review_csv_path) if review_csv_path else _DEFAULT_REVIEW

    df = pd.read_parquet(input_path, columns=["manufacturer_d_name"])

    # Work with unique names + counts
    name_counts = df["manufacturer_d_name"].dropna().value_counts().reset_index()
    name_counts.columns = ["raw_name", "event_count"]

    if name_counts.empty:
        empty = pd.DataFrame(
            columns=["raw_name", "standardized_name", "normalization_rule", "confidence_level", "manual_review_flag"]
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        empty.to_parquet(output_path, index=False)
        empty.to_csv(review_csv_path, index=False)
        return empty

    names = name_counts["raw_name"].tolist()
    normalized = [_normalize_name(n) for n in names]

    clustered = _cluster_names(names, normalized, threshold=fuzzy_threshold)
    alias_df = _build_alias_table(clustered, name_counts, top_n_review=top_n_review)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    alias_df.to_parquet(output_path, index=False)

    # Write manual review subset
    review = alias_df[alias_df["manual_review_flag"]].copy()
    review.to_csv(review_csv_path, index=False)

    return alias_df


def _normalize_name(name: str) -> str:
    """Normalize a manufacturer name: uppercase, strip punctuation, remove legal suffixes."""
    if not name or not isinstance(name, str):
        return ""
    # Step 1: uppercase + strip
    result = name.upper().strip()
    # Step 2: remove non-alphanumeric (keep spaces)
    result = re.sub(r"[^A-Z0-9\s]", " ", result)
    result = re.sub(r"\s+", " ", result).strip()
    # Step 3: iteratively remove legal suffixes from the end
    prev = None
    while prev != result:
        prev = result
        result = _LEGAL_SUFFIXES.sub("", result).strip()
        result = re.sub(r"\s+", " ", result).strip()
    return result


def _cluster_names(
    names: list[str],
    normalized: list[str],
    threshold: int = 90,
) -> pd.DataFrame:
    """Greedy clustering of names by fuzzy similarity on normalized forms.

    Names are processed in frequency order (most frequent first).
    Each unassigned name becomes a cluster representative, then all remaining
    unassigned names within threshold are added to that cluster.
    """
    assigned = {}  # raw_name -> cluster_representative_raw_name

    for i in range(len(names)):
        raw = names[i]
        norm = normalized[i]

        if raw in assigned:
            continue

        # This name becomes the cluster representative
        assigned[raw] = raw

        # Find all unassigned names similar to this one
        remaining_norms = [n for j, n in enumerate(normalized) if names[j] not in assigned]
        remaining_raws = [names[j] for j, n in enumerate(normalized) if names[j] not in assigned]

        if remaining_norms:
            matches = process.extract(
                norm,
                remaining_norms,
                scorer=fuzz.token_sort_ratio,
                limit=None,
                score_cutoff=threshold,
            )
            for match_norm, score, idx in matches:
                matched_raw = remaining_raws[idx]
                if matched_raw not in assigned:
                    assigned[matched_raw] = raw

    result = pd.DataFrame([{"raw_name": raw, "cluster_representative": rep} for raw, rep in assigned.items()])
    return result


def _build_alias_table(
    clustered: pd.DataFrame,
    name_counts: pd.DataFrame,
    top_n_review: int = 200,
) -> pd.DataFrame:
    """Build final alias table with rules and confidence."""
    merged = clustered.merge(name_counts, on="raw_name", how="left")

    rows = []
    for _, row in merged.iterrows():
        raw = row["raw_name"]
        rep = row["cluster_representative"]

        if raw == rep:
            rule = "exact"
            confidence = 100.0
        else:
            norm_raw = _normalize_name(raw)
            norm_rep = _normalize_name(rep)
            if norm_raw == norm_rep:
                rule = "suffix_removal"
                confidence = 95.0
            else:
                confidence = fuzz.token_sort_ratio(norm_raw, norm_rep)
                rule = "fuzzy_match"

        rows.append(
            {
                "raw_name": raw,
                "standardized_name": rep,
                "normalization_rule": rule,
                "confidence_level": confidence,
                "manual_review_flag": False,
            }
        )

    alias_df = pd.DataFrame(rows)

    # Flag top_n clusters by event count for manual review (fuzzy matches only)
    fuzzy_mask = alias_df["normalization_rule"] == "fuzzy_match"
    if fuzzy_mask.any():
        fuzzy_subset = alias_df[fuzzy_mask].nlargest(top_n_review, "confidence_level", keep="all")
        alias_df.loc[fuzzy_subset.index, "manual_review_flag"] = True

    # Drop event_count (from merge) if present
    alias_df = alias_df[
        ["raw_name", "standardized_name", "normalization_rule", "confidence_level", "manual_review_flag"]
    ]

    return alias_df
