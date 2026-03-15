"""Extractor for FDA device recall/enforcement data."""

import logging

from src.api.client import FDAClient
from src.config import DATA_RAW, ENDPOINT_RECALL
from src.extraction.base import BaseExtractor

logger = logging.getLogger(__name__)


class RecallExtractor(BaseExtractor):
    """Extract device recall records from openFDA without date filtering.

    Date filtering is deferred to the cleaning step to avoid openFDA
    server errors on date-range queries.
    """

    def __init__(self, client: FDAClient | None = None):
        client = client or FDAClient()
        super().__init__(client, DATA_RAW / "recalls")

    def extract(self) -> dict:
        """Fetch all device recall records without date filters.

        Uses product_type:"Devices" filter only.
        Date-window filtering is applied in the cleaning step.
        """
        progress = self._load_progress()

        if progress.get("status") == "complete":
            logger.info("Recall extraction already complete, skipping")
            return {"total_records": progress.get("total_records", 0)}

        search = 'product_type:"Devices"'
        count = self.client.fetch_count(ENDPOINT_RECALL, search=search)
        logger.info("Total device recalls (no date filter): %d records", count)

        results = self.client.fetch_all_pages(
            endpoint=ENDPOINT_RECALL,
            search=search,
            limit=1000,
            cache_dir=self.output_dir / "all" / "_cache",
            progress_file=self.output_dir / "all" / "_page_progress.json",
        )

        # Save as single file
        all_dir = self.output_dir / "all"
        all_dir.mkdir(parents=True, exist_ok=True)
        self._save_raw_json(results, "all/recalls_all.json")

        progress = {"status": "complete", "total_records": len(results)}
        self._save_progress(progress)
        logger.info("Recall extraction complete: %d total records", len(results))
        return {"total_records": len(results)}
