"""Tests for KPI helper functions."""

import pandas as pd
import pytest

from src.marts.kpis import (
    MIN_CLEARANCES_FOR_NORM,
    MIN_RECALLS_FOR_SHARE,
    events_per_100_clearances,
    firm_share,
    recall_to_event_ratio,
    recalls_per_100_clearances,
    severe_recall_share,
)

# ── events_per_100_clearances ─────────────────────────────────────────────────


class TestEventsper100Clearances:
    def test_normal(self):
        assert events_per_100_clearances(50, 100) == 50.0

    def test_below_threshold(self):
        assert events_per_100_clearances(5, MIN_CLEARANCES_FOR_NORM - 1) is None

    def test_at_threshold(self):
        result = events_per_100_clearances(10, MIN_CLEARANCES_FOR_NORM)
        assert result == 100.0

    def test_series(self):
        events = pd.Series([10, 5, 20])
        clearances = pd.Series([100, 5, 50])
        result = events_per_100_clearances(events, clearances)
        assert result.iloc[0] == 10.0
        assert pd.isna(result.iloc[1])  # below threshold
        assert result.iloc[2] == 40.0

    def test_empty_series(self):
        result = events_per_100_clearances(pd.Series([], dtype="int64"), pd.Series([], dtype="int64"))
        assert len(result) == 0


# ── recalls_per_100_clearances ────────────────────────────────────────────────


class TestRecallsper100Clearances:
    def test_normal(self):
        assert recalls_per_100_clearances(5, 100) == 5.0

    def test_below_threshold(self):
        assert recalls_per_100_clearances(1, 9) is None

    def test_series(self):
        recalls = pd.Series([5, 2])
        clearances = pd.Series([100, 5])
        result = recalls_per_100_clearances(recalls, clearances)
        assert result.iloc[0] == 5.0
        assert pd.isna(result.iloc[1])


# ── recall_to_event_ratio ────────────────────────────────────────────────────


class TestRecallToEventRatio:
    def test_normal(self):
        assert recall_to_event_ratio(10, 100) == 0.1

    def test_zero_events(self):
        assert recall_to_event_ratio(5, 0) is None

    def test_series(self):
        recalls = pd.Series([10, 5])
        events = pd.Series([100, 0])
        result = recall_to_event_ratio(recalls, events)
        assert result.iloc[0] == 0.1
        assert pd.isna(result.iloc[1])


# ── severe_recall_share ──────────────────────────────────────────────────────


class TestSevereRecallShare:
    def test_normal(self):
        assert severe_recall_share(1, 10) == 0.1

    def test_below_threshold(self):
        assert severe_recall_share(1, MIN_RECALLS_FOR_SHARE - 1) is None

    def test_at_threshold(self):
        result = severe_recall_share(1, MIN_RECALLS_FOR_SHARE)
        assert result == pytest.approx(1 / 3)

    def test_series(self):
        class_i = pd.Series([1, 0, 3])
        total = pd.Series([10, 2, 5])
        result = severe_recall_share(class_i, total)
        assert result.iloc[0] == 0.1
        assert pd.isna(result.iloc[1])  # below threshold
        assert result.iloc[2] == 0.6


# ── firm_share ───────────────────────────────────────────────────────────────


class TestFirmShare:
    def test_normal(self):
        assert firm_share(25, 100) == 0.25

    def test_zero_total(self):
        assert firm_share(0, 0) is None

    def test_series(self):
        firm = pd.Series([25, 10])
        total = pd.Series([100, 0])
        result = firm_share(firm, total)
        assert result.iloc[0] == 0.25
        assert pd.isna(result.iloc[1])
