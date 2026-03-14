"""Extractor for FDA device recall/enforcement data."""

import logging

from src.api.client import FDAClient
from src.config import DATA_RAW, DATE_END, DATE_START, ENDPOINT_RECALL
from src.extraction.base import BaseExtractor

logger = logging.getLogger(__name__)

PARTITION_THRESHOLD = 25000


class RecallExtractor(BaseExtractor):
    """Extract device recall records from openFDA, partitioned by year."""

    def __init__(self, client: FDAClient | None = None):
        client = client or FDAClient()
        super().__init__(client, DATA_RAW / "recalls")

    def _build_search(self, start: str, end: str) -> str:
        """Build search query with date range and device product type filter."""
        date_clause = self.client.date_range_search("report_date", start, end)
        type_clause = 'product_type:"Devices"'
        return self.client.combine_search(date_clause, type_clause)

    def _extract_partition(self, start: str, end: str, label: str) -> list[dict]:
        """Extract a single date partition, auto-splitting if too large."""
        search = self._build_search(start, end)
        count = self.client.fetch_count(ENDPOINT_RECALL, search=search)
        logger.info("Partition %s: %d records", label, count)

        if count > PARTITION_THRESHOLD:
            # Parse the year from start date and split into quarters
            year = int(start[:4])
            logger.warning("Partition %s has %d records, splitting by quarter", label, count)
            all_results = []
            for q_idx, (q_start, q_end) in enumerate(self._partition_by_quarter(year), 1):
                q_results = self._extract_partition(q_start, q_end, f"{label}-Q{q_idx}")
                all_results.extend(q_results)
            return all_results

        if count == 0:
            return []

        return self.client.fetch_all_pages(
            endpoint=ENDPOINT_RECALL,
            search=search,
            limit=1000,
            cache_dir=self.output_dir / label / "_cache",
            progress_file=self.output_dir / label / "_page_progress.json",
        )

    def extract(self) -> dict:
        """Extract recall data for each year in the time window."""
        progress = self._load_progress()
        start_year = int(DATE_START[:4])
        end_year = int(DATE_END[:4])

        summary = {"years": {}, "total_records": 0}

        for year_start, year_end in self._partition_by_year(start_year, end_year):
            year = year_start[:4]

            if progress.get("years", {}).get(year, {}).get("status") == "complete":
                logger.info("Year %s already complete, skipping", year)
                year_count = progress["years"][year]["records"]
                summary["years"][year] = {"records": year_count}
                summary["total_records"] += year_count
                continue

            logger.info("Extracting recalls for %s", year)
            results = self._extract_partition(year_start, year_end, year)

            year_dir = self.output_dir / year
            year_dir.mkdir(parents=True, exist_ok=True)
            self._save_raw_json(results, f"{year}/recalls_{year}.json")

            summary["years"][year] = {"records": len(results)}
            summary["total_records"] += len(results)

            # Update progress
            progress.setdefault("years", {})[year] = {"status": "complete", "records": len(results)}
            self._save_progress(progress)

        progress["status"] = "complete"
        progress["total_records"] = summary["total_records"]
        self._save_progress(progress)
        logger.info("Recall extraction complete: %d total records", summary["total_records"])
        return summary
