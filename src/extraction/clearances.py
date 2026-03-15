"""Extractor for FDA 510(k) clearance data."""

import logging

from src.api.client import FDAClient
from src.config import DATA_RAW, ENDPOINT_510K
from src.extraction.base import BaseExtractor

logger = logging.getLogger(__name__)

MAX_SINGLE_QUERY = 26000


class ClearanceExtractor(BaseExtractor):
    """Extract 510(k) clearance records from openFDA without date filtering.

    If the total record count exceeds the API pagination limit (26K),
    partitions by advisory_committee to keep each query under the limit.
    Date filtering is deferred to the cleaning step.
    """

    def __init__(self, client: FDAClient | None = None):
        client = client or FDAClient()
        super().__init__(client, DATA_RAW / "clearances")

    def _fetch_partition(self, search: str | None, label: str) -> list[dict]:
        """Fetch all pages for a single partition (search filter + label)."""
        return self.client.fetch_all_pages(
            endpoint=ENDPOINT_510K,
            search=search,
            limit=1000,
            cache_dir=self.output_dir / label / "_cache",
            progress_file=self.output_dir / label / "_page_progress.json",
        )

    def _extract_by_committee(self) -> list[dict]:
        """Partition by advisory_committee when total exceeds 26K."""
        committees = self.client.fetch_count_by(ENDPOINT_510K, field="advisory_committee")
        logger.info("Found %d advisory committees for partitioning", len(committees))

        all_results = []
        for entry in committees:
            term = entry["term"]
            count = entry["count"]
            logger.info("Committee %s: %d records", term, count)

            search = f'advisory_committee:"{term}"'
            results = self._fetch_partition(search, f"committee_{term}")
            all_results.extend(results)

        return all_results

    def extract(self) -> dict:
        """Fetch all 510(k) clearance records without date filters.

        Automatically partitions by advisory_committee if total > 26K.
        Date-window filtering is applied in the cleaning step.
        """
        progress = self._load_progress()

        if progress.get("status") == "complete":
            logger.info("Clearance extraction already complete, skipping")
            return {"total_records": progress.get("total_records", 0)}

        total = self.client.fetch_count(ENDPOINT_510K)
        logger.info("Total 510(k) clearances (no date filter): %d records", total)

        if total > MAX_SINGLE_QUERY:
            logger.info("Total exceeds %d, partitioning by advisory_committee", MAX_SINGLE_QUERY)
            results = self._extract_by_committee()
        else:
            results = self._fetch_partition(search=None, label="all")

        # Save combined output
        out_dir = self.output_dir / "all"
        out_dir.mkdir(parents=True, exist_ok=True)
        self._save_raw_json(results, "all/clearances_all.json")

        progress = {"status": "complete", "total_records": len(results)}
        self._save_progress(progress)
        logger.info("510(k) extraction complete: %d total records", len(results))
        return {"total_records": len(results)}
