"""FDA openFDA API client with rate limiting, retry, pagination, and caching."""

import json
import logging
import time
from collections import deque
from pathlib import Path

import requests

from src.api.exceptions import FDAApiError, PartitionTooLargeError, RateLimitExceeded
from src.config import API_RATE_LIMIT, API_RATE_LIMIT_NO_KEY, FDA_API_KEY, FDA_BASE_URL, MAX_RETRIES

logger = logging.getLogger(__name__)

MAX_API_RESULTS = 26000


class FDAClient:
    """Client for the openFDA API with rate limiting, retries, and pagination."""

    def __init__(self, api_key: str | None = None, rate_limit: int | None = None):
        self.api_key = api_key if api_key is not None else FDA_API_KEY
        if rate_limit is not None:
            self.rate_limit = rate_limit
        else:
            self.rate_limit = API_RATE_LIMIT if self.api_key else API_RATE_LIMIT_NO_KEY
        self.base_url = FDA_BASE_URL
        self._request_timestamps: deque[float] = deque()
        self._session = requests.Session()

    # ── Rate Limiting ─────────────────────────────────────────────────────────

    def _wait_for_rate_limit(self) -> None:
        """Token-bucket rate limiter using a deque of request timestamps."""
        now = time.monotonic()
        window = 60.0  # 1 minute window

        # Remove timestamps older than the window
        while self._request_timestamps and (now - self._request_timestamps[0]) > window:
            self._request_timestamps.popleft()

        if len(self._request_timestamps) >= self.rate_limit:
            sleep_time = window - (now - self._request_timestamps[0])
            if sleep_time > 0:
                logger.debug("Rate limit reached, sleeping %.2fs", sleep_time)
                time.sleep(sleep_time)

        self._request_timestamps.append(time.monotonic())

    # ── Retry Logic ───────────────────────────────────────────────────────────

    def _retry_with_backoff(self, func, max_retries: int = MAX_RETRIES):
        """Execute func with exponential backoff on retryable errors."""
        last_exception = None
        for attempt in range(max_retries + 1):
            try:
                return func()
            except requests.exceptions.ConnectionError as e:
                last_exception = e
                if attempt == max_retries:
                    raise FDAApiError(f"Connection failed after {max_retries + 1} attempts: {e}") from e
            except requests.exceptions.HTTPError as e:
                last_exception = e
                status = e.response.status_code if e.response is not None else None
                if status == 429:
                    if attempt == max_retries:
                        raise RateLimitExceeded(
                            f"Rate limit exceeded after {max_retries + 1} attempts",
                            status_code=429,
                        ) from e
                    retry_after = e.response.headers.get("Retry-After")
                    if retry_after:
                        wait = float(retry_after)
                    else:
                        wait = 2**attempt
                elif status is not None and status >= 500:
                    if attempt == max_retries:
                        raise FDAApiError(
                            f"Server error {status} after {max_retries + 1} attempts",
                            status_code=status,
                            response_body=e.response.text,
                        ) from e
                    wait = 2**attempt
                else:
                    raise FDAApiError(
                        f"HTTP {status}",
                        status_code=status,
                        response_body=e.response.text if e.response is not None else None,
                    ) from e

            wait_time = wait if "wait" in dir() else 2**attempt
            logger.warning("Attempt %d failed, retrying in %.1fs: %s", attempt + 1, wait_time, last_exception)
            time.sleep(wait_time)

        raise FDAApiError(f"Failed after {max_retries + 1} attempts")  # pragma: no cover

    # ── Core Request ──────────────────────────────────────────────────────────

    def _get(self, url: str, params: dict | None = None) -> requests.Response:
        """Make a single GET request with rate limiting and retry."""
        self._wait_for_rate_limit()

        def do_request():
            resp = self._session.get(url, params=params, timeout=30)
            resp.raise_for_status()
            return resp

        return self._retry_with_backoff(do_request)

    # ── Pagination ────────────────────────────────────────────────────────────

    def fetch_page(self, endpoint: str, search: str | None = None, skip: int = 0, limit: int = 100) -> dict:
        """Fetch a single page from an openFDA endpoint."""
        url = f"{self.base_url}{endpoint}"
        params = {"limit": limit, "skip": skip}
        if self.api_key:
            params["api_key"] = self.api_key
        if search:
            params["search"] = search

        resp = self._get(url, params)
        return resp.json()

    def fetch_count(self, endpoint: str, search: str | None = None) -> int:
        """Quick total count for a query (single request, limit=1)."""
        data = self.fetch_page(endpoint, search=search, skip=0, limit=1)
        return data.get("meta", {}).get("results", {}).get("total", 0)

    def fetch_all_pages(
        self,
        endpoint: str,
        search: str | None = None,
        limit: int = 1000,
        cache_dir: Path | None = None,
        progress_file: Path | None = None,
    ) -> list[dict]:
        """Paginate through all results for a query.

        Caches each page as JSON in cache_dir for resumability.
        Raises PartitionTooLargeError if total > 26,000.
        """
        # Load progress if resuming
        completed_skips: set[int] = set()
        if progress_file and progress_file.exists():
            progress = json.loads(progress_file.read_text())
            completed_skips = set(progress.get("completed_skips", []))
            logger.info("Resuming from progress: %d pages already fetched", len(completed_skips))

        # First request to get total
        first_page = self.fetch_page(endpoint, search=search, skip=0, limit=limit)
        total = first_page.get("meta", {}).get("results", {}).get("total", 0)

        if total > MAX_API_RESULTS:
            raise PartitionTooLargeError(
                f"Query returns {total} results, exceeding {MAX_API_RESULTS} limit. Partition further.",
                status_code=None,
            )

        all_results = []

        # Cache and collect first page
        if 0 not in completed_skips:
            results = first_page.get("results", [])
            all_results.extend(results)
            if cache_dir:
                cache_dir.mkdir(parents=True, exist_ok=True)
                (cache_dir / "page_0.json").write_text(json.dumps(results))
            completed_skips.add(0)
        else:
            # Load from cache
            if cache_dir and (cache_dir / "page_0.json").exists():
                all_results.extend(json.loads((cache_dir / "page_0.json").read_text()))

        # Remaining pages
        for skip in range(limit, total, limit):
            if skip in completed_skips:
                if cache_dir and (cache_dir / f"page_{skip}.json").exists():
                    all_results.extend(json.loads((cache_dir / f"page_{skip}.json").read_text()))
                continue

            data = self.fetch_page(endpoint, search=search, skip=skip, limit=limit)
            results = data.get("results", [])
            all_results.extend(results)

            if cache_dir:
                cache_dir.mkdir(parents=True, exist_ok=True)
                (cache_dir / f"page_{skip}.json").write_text(json.dumps(results))

            completed_skips.add(skip)

            # Save progress after each page
            if progress_file:
                progress_file.parent.mkdir(parents=True, exist_ok=True)
                progress_file.write_text(
                    json.dumps(
                        {
                            "total": total,
                            "completed_skips": sorted(completed_skips),
                            "endpoint": endpoint,
                            "search": search,
                        }
                    )
                )

            logger.info("Fetched %d / %d records", len(all_results), total)

        return all_results

    # ── File Download ─────────────────────────────────────────────────────────

    def download_file(self, url: str, dest_path: Path, chunk_size: int = 8192) -> Path:
        """Stream a large file to disk."""
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        self._wait_for_rate_limit()

        with self._session.get(url, stream=True, timeout=120) as resp:
            resp.raise_for_status()
            with open(dest_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=chunk_size):
                    f.write(chunk)

        logger.info("Downloaded %s (%.1f MB)", dest_path.name, dest_path.stat().st_size / 1_048_576)
        return dest_path

    # ── Search Helpers ────────────────────────────────────────────────────────

    @staticmethod
    def date_range_search(field: str, start: str, end: str) -> str:
        """Build an openFDA date range search clause.

        Dates should be YYYY-MM-DD; converted to YYYYMMDD for openFDA.
        """
        start_fmt = start.replace("-", "")
        end_fmt = end.replace("-", "")
        return f"{field}:[{start_fmt}+TO+{end_fmt}]"

    @staticmethod
    def combine_search(*clauses: str) -> str:
        """Join multiple search clauses with AND."""
        return "+AND+".join(clauses)
