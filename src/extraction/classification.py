"""Extractor for FDA device classification data."""

import logging

from src.api.client import FDAClient
from src.config import DATA_RAW, ENDPOINT_CLASSIFICATION
from src.extraction.base import BaseExtractor

logger = logging.getLogger(__name__)


class ClassificationExtractor(BaseExtractor):
    """Extract all device classification records from openFDA."""

    def __init__(self, client: FDAClient | None = None):
        client = client or FDAClient()
        super().__init__(client, DATA_RAW / "classification")

    def extract(self) -> dict:
        """Fetch all classification records (no date filter needed)."""
        progress = self._load_progress()

        if progress.get("status") == "complete":
            logger.info("Classification extraction already complete, skipping")
            return progress

        logger.info("Starting classification extraction")
        total = self.client.fetch_count(ENDPOINT_CLASSIFICATION)
        logger.info("Total classification records: %d", total)

        results = self.client.fetch_all_pages(
            endpoint=ENDPOINT_CLASSIFICATION,
            limit=1000,
            cache_dir=self.output_dir / "_cache",
            progress_file=self.output_dir / "_page_progress.json",
        )

        self._save_raw_json(results, "classification_all.json")

        progress = {
            "status": "complete",
            "total_records": len(results),
            "api_total": total,
            "output_file": "classification_all.json",
        }
        self._save_progress(progress)
        logger.info("Classification extraction complete: %d records", len(results))
        return progress
