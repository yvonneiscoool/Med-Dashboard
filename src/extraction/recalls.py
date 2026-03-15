"""Extractor for FDA device recall/enforcement data."""

import logging

from src.api.client import FDAClient
from src.config import DATA_RAW, ENDPOINT_RECALL
from src.extraction.base import BaseExtractor

logger = logging.getLogger(__name__)

MAX_SINGLE_QUERY = 26000


class RecallExtractor(BaseExtractor):
    """Extract device recall records from openFDA without date filtering.

    If the total record count exceeds the API pagination limit (26K),
    partitions by status to keep each query under the limit.
    Date filtering is deferred to the cleaning step.
    """

    def __init__(self, client: FDAClient | None = None):
        client = client or FDAClient()
        super().__init__(client, DATA_RAW / "recalls")

    def _fetch_partition(self, search: str, label: str) -> list[dict]:
        """Fetch all pages for a single partition (search filter + label)."""
        return self.client.fetch_all_pages(
            endpoint=ENDPOINT_RECALL,
            search=search,
            limit=1000,
            cache_dir=self.output_dir / label / "_cache",
            progress_file=self.output_dir / label / "_page_progress.json",
        )

    def _extract_by_status(self) -> list[dict]:
        """Partition by status when total exceeds 26K.

        Each partition uses only status as the search filter (avoiding
        compound queries that fail with URL encoding issues).
        """
        statuses = self.client.fetch_count_by(
            ENDPOINT_RECALL,
            field="status.exact",
            search='product_type:"Devices"',
        )
        logger.info("Found %d status values for partitioning", len(statuses))

        all_results = []
        for entry in statuses:
            term = entry["term"]
            count = entry["count"]
            logger.info("Status '%s': %d records", term, count)

            # Use only status filter per partition — avoids compound query
            # encoding issues. Non-device records are rare and filtered in cleaning.
            search = f'status:"{term}"'
            results = self._fetch_partition(search, f"status_{term.replace(' ', '_')}")
            all_results.extend(results)

        return all_results

    def extract(self) -> dict:
        """Fetch all device recall records without date filters.

        Uses product_type:"Devices" filter only.
        Automatically partitions by status if total > 26K.
        Date-window filtering is applied in the cleaning step.
        """
        progress = self._load_progress()

        if progress.get("status") == "complete":
            logger.info("Recall extraction already complete, skipping")
            return {"total_records": progress.get("total_records", 0)}

        search = 'product_type:"Devices"'
        count = self.client.fetch_count(ENDPOINT_RECALL, search=search)
        logger.info("Total device recalls (no date filter): %d records", count)

        if count > MAX_SINGLE_QUERY:
            logger.info("Total exceeds %d, partitioning by status", MAX_SINGLE_QUERY)
            results = self._extract_by_status()
        else:
            results = self._fetch_partition(search, "all")

        # Save combined output
        all_dir = self.output_dir / "all"
        all_dir.mkdir(parents=True, exist_ok=True)
        self._save_raw_json(results, "all/recalls_all.json")

        progress = {"status": "complete", "total_records": len(results)}
        self._save_progress(progress)
        logger.info("Recall extraction complete: %d total records", len(results))
        return {"total_records": len(results)}
