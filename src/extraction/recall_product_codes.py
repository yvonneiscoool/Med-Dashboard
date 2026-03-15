"""Extractor for product code lookup from FDA /device/recall.json endpoint."""

import logging

from src.api.client import FDAClient
from src.config import DATA_RAW, ENDPOINT_DEVICE_RECALL
from src.extraction.base import BaseExtractor

logger = logging.getLogger(__name__)

MAX_SINGLE_QUERY = 26000


class RecallProductCodeExtractor(BaseExtractor):
    """Extract product_res_number -> product_code lookup from /device/recall.json.

    This endpoint provides product_code as a top-level field, unlike
    /device/enforcement.json where openfda blocks are always empty.
    The extracted lookup is joined to enforcement data during cleaning.
    """

    def __init__(self, client: FDAClient | None = None):
        client = client or FDAClient()
        super().__init__(client, DATA_RAW / "recall_product_codes")

    def _fetch_partition(self, search: str, label: str) -> list[dict]:
        """Fetch all pages for a single partition."""
        return self.client.fetch_all_pages(
            endpoint=ENDPOINT_DEVICE_RECALL,
            search=search,
            limit=1000,
            cache_dir=self.output_dir / label / "_cache",
            progress_file=self.output_dir / label / "_page_progress.json",
        )

    def _extract_by_status(self) -> list[dict]:
        """Partition by recall_status when total exceeds 26K."""
        statuses = self.client.fetch_count_by(
            ENDPOINT_DEVICE_RECALL,
            field="recall_status.exact",
        )
        logger.info("Found %d recall_status values for partitioning", len(statuses))

        all_results = []
        for entry in statuses:
            term = entry["term"]
            count = entry["count"]
            logger.info("Status '%s': %d records", term, count)
            search = f'recall_status:"{term}"'
            results = self._fetch_partition(search, f"status_{term.replace(' ', '_')}")
            all_results.extend(results)
        return all_results

    def extract(self) -> dict:
        """Fetch all device recall records for product code lookup.

        Automatically partitions by recall_status if total > 26K.
        """
        progress = self._load_progress()

        if progress.get("status") == "complete":
            logger.info("Recall product code extraction already complete, skipping")
            return {"total_records": progress.get("total_records", 0)}

        count = self.client.fetch_count(ENDPOINT_DEVICE_RECALL)
        logger.info("Total device recalls (/device/recall.json): %d records", count)

        if count > MAX_SINGLE_QUERY:
            logger.info("Total exceeds %d, partitioning by recall_status", MAX_SINGLE_QUERY)
            results = self._extract_by_status()
        else:
            results = self._fetch_partition("", "all")

        # Save combined output
        all_dir = self.output_dir / "all"
        all_dir.mkdir(parents=True, exist_ok=True)
        self._save_raw_json(results, "all/recall_product_codes_all.json")

        progress = {"status": "complete", "total_records": len(results)}
        self._save_progress(progress)
        logger.info("Recall product code extraction complete: %d records", len(results))
        return {"total_records": len(results)}
