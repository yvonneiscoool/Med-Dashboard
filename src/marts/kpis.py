"""KPI helper functions for mart construction.

Pure functions with no I/O. Each accepts scalar int or pd.Series inputs
and returns None/pd.NA when denominators are below safety thresholds.
"""

from __future__ import annotations

import pandas as pd

# ── Threshold constants ───────────────────────────────────────────────────────
MIN_CLEARANCES_FOR_NORM = 10
MIN_RECALLS_FOR_SHARE = 3


def events_per_100_clearances(
    event_count: int | pd.Series,
    clearance_count: int | pd.Series,
) -> float | None | pd.Series:
    """Normalized event rate per 100 clearances. NULL if clearances < 10."""
    if isinstance(event_count, pd.Series):
        result = (event_count / clearance_count) * 100
        return result.where(clearance_count >= MIN_CLEARANCES_FOR_NORM)
    if clearance_count < MIN_CLEARANCES_FOR_NORM:
        return None
    return (event_count / clearance_count) * 100


def recalls_per_100_clearances(
    recall_count: int | pd.Series,
    clearance_count: int | pd.Series,
) -> float | None | pd.Series:
    """Normalized recall rate per 100 clearances. NULL if clearances < 10."""
    if isinstance(recall_count, pd.Series):
        result = (recall_count / clearance_count) * 100
        return result.where(clearance_count >= MIN_CLEARANCES_FOR_NORM)
    if clearance_count < MIN_CLEARANCES_FOR_NORM:
        return None
    return (recall_count / clearance_count) * 100


def recall_to_event_ratio(
    recall_count: int | pd.Series,
    event_count: int | pd.Series,
) -> float | None | pd.Series:
    """Ratio of recalls to events. NULL if events == 0."""
    if isinstance(recall_count, pd.Series):
        result = recall_count / event_count
        return result.where(event_count > 0)
    if event_count == 0:
        return None
    return recall_count / event_count


def severe_recall_share(
    class_i_count: int | pd.Series,
    total_recall_count: int | pd.Series,
) -> float | None | pd.Series:
    """Share of Class I (most severe) recalls. NULL if total < 3."""
    if isinstance(class_i_count, pd.Series):
        result = class_i_count / total_recall_count
        return result.where(total_recall_count >= MIN_RECALLS_FOR_SHARE)
    if total_recall_count < MIN_RECALLS_FOR_SHARE:
        return None
    return class_i_count / total_recall_count


def firm_share(
    firm_count: int | pd.Series,
    total_count: int | pd.Series,
) -> float | None | pd.Series:
    """Firm's share of total. NULL if total == 0."""
    if isinstance(firm_count, pd.Series):
        result = firm_count / total_count
        return result.where(total_count > 0)
    if total_count == 0:
        return None
    return firm_count / total_count
