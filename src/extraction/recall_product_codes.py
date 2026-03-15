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

    def _extract_by_product_code(self) -> list[dict]:
        """Partition by product_code when total exceeds 26K.

        The /device/recall.json endpoint doesn't support compound AND queries,
        so we partition by product_code (max ~1K per bucket). The count-by API
        returns only the top 1000 product codes, which covers ~92% of records.
        Remaining records (with rare product codes) are in long-tail partitions
        that can't be fetched individually but are acceptable to miss since we
        only need the lookup for enrichment (not completeness).
        """
        product_codes = self.client.fetch_count_by(
            ENDPOINT_DEVICE_RECALL,
            field="product_code",
        )
        logger.info(
            "Found %d product_code values for partitioning (top 1000)",
            len(product_codes),
        )

        all_results = []
        for i, entry in enumerate(product_codes):
            pc = entry["term"]
            if i % 100 == 0:
                logger.info(
                    "Progress: %d/%d product codes, %d records so far",
                    i,
                    len(product_codes),
                    len(all_results),
                )
            search = f'product_code:"{pc}"'
            results = self._fetch_partition(search, f"pc_{pc}")
            all_results.extend(results)

        logger.info(
            "Fetched %d records across %d product code partitions",
            len(all_results),
            len(product_codes),
        )
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
            logger.info("Total exceeds %d, partitioning by product_code", MAX_SINGLE_QUERY)
            results = self._extract_by_product_code()
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
