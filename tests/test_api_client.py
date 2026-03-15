"""Tests for the FDA API client."""

import json
from unittest.mock import MagicMock, patch

import pytest
import requests

from src.api.client import MAX_API_RESULTS, FDAClient
from src.api.exceptions import FDAApiError, PartitionTooLargeError, RateLimitExceeded


def _mock_response(status_code: int, json_data: dict) -> MagicMock:
    """Create a mock requests.Response with given status and JSON body."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.raise_for_status.return_value = None
    return resp


class TestRateLimiting:
    def test_rate_limit_allows_requests_under_limit(self, fda_client):
        """Requests under the rate limit should proceed without delay."""
        for _ in range(5):
            fda_client._wait_for_rate_limit()
        # Should not raise or sleep significantly

    def test_rate_limit_tracks_timestamps(self, fda_client):
        """Timestamps should be recorded for each request."""
        fda_client._wait_for_rate_limit()
        assert len(fda_client._request_timestamps) == 1


class TestRetryLogic:
    def test_retry_on_connection_error(self, fda_client):
        """Should retry on ConnectionError and eventually succeed."""
        call_count = 0

        def flaky_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise requests.exceptions.ConnectionError("Connection reset")
            return "success"

        with patch("src.api.client.time.sleep"):
            result = fda_client._retry_with_backoff(flaky_func)

        assert result == "success"
        assert call_count == 3

    def test_retry_exhausted_raises_fda_error(self, fda_client):
        """Should raise FDAApiError when retries are exhausted."""

        def always_fail():
            raise requests.exceptions.ConnectionError("Connection reset")

        with patch("src.api.client.time.sleep"):
            with pytest.raises(FDAApiError, match="Connection failed"):
                fda_client._retry_with_backoff(always_fail)

    def test_retry_on_429_raises_rate_limit_exceeded(self, fda_client):
        """Should raise RateLimitExceeded when 429 retries exhausted."""
        response = MagicMock()
        response.status_code = 429
        response.headers = {}
        error = requests.exceptions.HTTPError(response=response)

        def rate_limited():
            raise error

        with patch("src.api.client.time.sleep"):
            with pytest.raises(RateLimitExceeded):
                fda_client._retry_with_backoff(rate_limited)

    def test_retry_on_429_reads_retry_after(self, fda_client):
        """Should respect Retry-After header on 429."""
        call_count = 0
        response = MagicMock()
        response.status_code = 429
        response.headers = {"Retry-After": "5"}
        error = requests.exceptions.HTTPError(response=response)

        def rate_limited():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise error
            return "ok"

        with patch("src.api.client.time.sleep") as mock_sleep:
            result = fda_client._retry_with_backoff(rate_limited)

        assert result == "ok"
        mock_sleep.assert_called_with(5.0)

    def test_retry_on_500_retries(self, fda_client):
        """Should retry on 5xx server errors."""
        call_count = 0
        response = MagicMock()
        response.status_code = 500
        response.text = "Internal Server Error"
        error = requests.exceptions.HTTPError(response=response)

        def server_error():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise error
            return "recovered"

        with patch("src.api.client.time.sleep"):
            result = fda_client._retry_with_backoff(server_error)

        assert result == "recovered"

    def test_no_retry_on_400(self, fda_client):
        """Should NOT retry on 4xx errors (except 429)."""
        response = MagicMock()
        response.status_code = 400
        response.text = "Bad Request"
        error = requests.exceptions.HTTPError(response=response)

        def bad_request():
            raise error

        with pytest.raises(FDAApiError, match="HTTP 400"):
            fda_client._retry_with_backoff(bad_request)


class TestFetchPage:
    def test_fetch_page_basic(self, fda_client, sample_api_response):
        """Should fetch a single page and return parsed JSON."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = sample_api_response([{"id": 1}], total=1)
        mock_resp.raise_for_status.return_value = None
        fda_client._session.get.return_value = mock_resp

        result = fda_client.fetch_page("/device/event.json", search="foo", skip=0, limit=10)

        assert result["results"] == [{"id": 1}]
        assert result["meta"]["results"]["total"] == 1

    def test_fetch_page_includes_api_key(self, fda_client, sample_api_response):
        """Should include API key in request params."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = sample_api_response([], total=0)
        mock_resp.raise_for_status.return_value = None
        fda_client._session.get.return_value = mock_resp

        fda_client.fetch_page("/device/event.json")

        call_args = fda_client._session.get.call_args
        assert call_args[1]["params"]["api_key"] == "test_key"


class TestFetchCount:
    def test_fetch_count_returns_total(self, fda_client, sample_api_response):
        """Should return the total count from meta."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = sample_api_response([], total=5000)
        mock_resp.raise_for_status.return_value = None
        fda_client._session.get.return_value = mock_resp

        count = fda_client.fetch_count("/device/classification.json")
        assert count == 5000


class TestFetchAllPages:
    def test_pagination_collects_all_results(self, fda_client, sample_api_response, tmp_path):
        """Should paginate through all results and collect them."""
        # Page 1: 2 results, total 3
        page1_resp = MagicMock()
        page1_resp.json.return_value = sample_api_response([{"id": 1}, {"id": 2}], total=3)
        page1_resp.raise_for_status.return_value = None

        # Page 2: 1 result
        page2_resp = MagicMock()
        page2_resp.json.return_value = sample_api_response([{"id": 3}], total=3)
        page2_resp.raise_for_status.return_value = None

        fda_client._session.get.side_effect = [page1_resp, page2_resp]

        results = fda_client.fetch_all_pages(
            "/device/event.json",
            limit=2,
            cache_dir=tmp_path / "cache",
            progress_file=tmp_path / "progress.json",
        )

        assert len(results) == 3
        assert [r["id"] for r in results] == [1, 2, 3]

    def test_raises_on_too_many_results(self, fda_client, sample_api_response):
        """Should raise PartitionTooLargeError if total exceeds limit."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = sample_api_response([], total=MAX_API_RESULTS + 1)
        mock_resp.raise_for_status.return_value = None
        fda_client._session.get.return_value = mock_resp

        with pytest.raises(PartitionTooLargeError):
            fda_client.fetch_all_pages("/device/event.json")

    def test_caching_creates_page_files(self, fda_client, sample_api_response, tmp_path):
        """Should cache each page as a JSON file."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = sample_api_response([{"id": 1}], total=1)
        mock_resp.raise_for_status.return_value = None
        fda_client._session.get.return_value = mock_resp

        cache_dir = tmp_path / "cache"
        fda_client.fetch_all_pages("/test", limit=100, cache_dir=cache_dir)

        assert (cache_dir / "page_0.json").exists()
        cached = json.loads((cache_dir / "page_0.json").read_text())
        assert cached == [{"id": 1}]

    def test_resumability_skips_completed_pages(self, fda_client, sample_api_response, tmp_path):
        """Should skip already-fetched pages when resuming."""
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir(parents=True)
        progress_file = tmp_path / "progress.json"

        # Pre-populate cache and progress for page 0
        (cache_dir / "page_0.json").write_text(json.dumps([{"id": 1}, {"id": 2}]))
        progress_file.write_text(json.dumps({"completed_skips": [0]}))

        # Only page 2 needs fetching - first call is for page 0 (to get total)
        page0_resp = MagicMock()
        page0_resp.json.return_value = sample_api_response([{"id": 1}, {"id": 2}], total=3)
        page0_resp.raise_for_status.return_value = None

        page2_resp = MagicMock()
        page2_resp.json.return_value = sample_api_response([{"id": 3}], total=3)
        page2_resp.raise_for_status.return_value = None

        fda_client._session.get.side_effect = [page0_resp, page2_resp]

        results = fda_client.fetch_all_pages(
            "/test",
            limit=2,
            cache_dir=cache_dir,
            progress_file=progress_file,
        )

        assert len(results) == 3


class TestDateRangeSearch:
    def test_date_format_removes_hyphens(self):
        """Should convert YYYY-MM-DD to YYYYMMDD for openFDA."""
        result = FDAClient.date_range_search("date_received", "2019-01-01", "2025-12-31")
        assert result == "date_received:[20190101+TO+20251231]"


class TestCombineSearch:
    def test_join_with_and(self):
        """Should join clauses with +AND+."""
        result = FDAClient.combine_search("field1:value1", "field2:value2")
        assert result == "field1:value1+AND+field2:value2"

    def test_single_clause(self):
        """Single clause should be returned as-is."""
        result = FDAClient.combine_search("field1:value1")
        assert result == "field1:value1"


class TestDailyRateLimit:
    def test_daily_rate_limit_raises_when_exceeded(self, fda_client):
        """Client should raise RateLimitExceeded when daily limit is hit."""
        fda_client.daily_limit = 3
        fda_client._daily_request_count = 3
        with pytest.raises(RateLimitExceeded, match="Daily"):
            fda_client._check_daily_limit()

    def test_daily_rate_limit_resets_after_midnight(self, fda_client):
        """Daily counter should reset when the date changes."""
        fda_client.daily_limit = 5
        fda_client._daily_request_count = 5
        fda_client._daily_reset_date = "2026-03-13"
        fda_client._check_daily_limit()
        assert fda_client._daily_request_count == 0


class TestFetchCountBy:
    def test_fetch_count_by_returns_term_counts(self, fda_client, mock_session):
        """fetch_count_by should return list of {term, count} dicts."""
        mock_session.get.return_value = _mock_response(
            200,
            {
                "results": [
                    {"term": "SU", "count": 15000},
                    {"term": "CV", "count": 12000},
                    {"term": "OR", "count": 8000},
                ]
            },
        )
        result = fda_client.fetch_count_by("/device/510k.json", field="advisory_committee")
        assert len(result) == 3
        assert result[0] == {"term": "SU", "count": 15000}
        call_params = mock_session.get.call_args[1]["params"]
        assert call_params["count"] == "advisory_committee"
