"""Extractor for FDA device adverse event (MAUDE) data."""

import logging

from src.api.client import FDAClient
from src.config import DATA_RAW, DATE_END, DATE_START, ENDPOINT_ADVERSE_EVENTS
from src.extraction.base import BaseExtractor

logger = logging.getLogger(__name__)

DOWNLOAD_INDEX_URL = "https://api.fda.gov/download.json"


class AdverseEventExtractor(BaseExtractor):
    """Extract device adverse event data via bulk download or API sample."""

    def __init__(self, client: FDAClient | None = None):
        client = client or FDAClient()
        super().__init__(client, DATA_RAW / "adverse_events")

    def extract(self, method: str = "bulk") -> dict:
        """Run extraction using the specified method."""
        if method == "bulk":
            return self.extract_bulk()
        elif method == "api_sample":
            return self.extract_api_sample()
        else:
            raise ValueError(f"Unknown extraction method: {method}")

    def extract_bulk(self) -> dict:
        """Download adverse event data via bulk ZIP files from openFDA."""
        progress = self._load_progress()

        if progress.get("status") == "complete":
            logger.info("Adverse event bulk extraction already complete, skipping")
            return progress

        # Fetch the download index
        logger.info("Fetching download index from %s", DOWNLOAD_INDEX_URL)
        resp = self.client._session.get(DOWNLOAD_INDEX_URL, timeout=30)
        resp.raise_for_status()
        download_index = resp.json()

        # Parse device event partitions
        partitions = download_index.get("results", {}).get("device", {}).get("event", {}).get("partitions", [])
        logger.info("Found %d total adverse event partitions", len(partitions))

        # Filter to our date window
        start_year = int(DATE_START[:4])
        end_year = int(DATE_END[:4])
        filtered = self._filter_partitions(partitions, start_year, end_year)
        logger.info("Filtered to %d partitions in %d-%d window", len(filtered), start_year, end_year)

        # Download each ZIP
        bulk_dir = self.output_dir / "bulk"
        bulk_dir.mkdir(parents=True, exist_ok=True)
        downloaded = progress.get("downloaded_files", [])
        downloaded_set = set(downloaded)

        for partition in filtered:
            file_url = partition["file"]
            # Use last two path segments for unique local filename (e.g., "2019q1_device-event-0001.json.zip")
            url_parts = file_url.rstrip("/").split("/")
            filename = "_".join(url_parts[-2:]) if len(url_parts) >= 2 else url_parts[-1]

            if filename in downloaded_set:
                logger.info("Already downloaded %s, skipping", filename)
                continue

            dest = bulk_dir / filename
            logger.info("Downloading %s", filename)
            self.client.download_file(file_url, dest)
            downloaded.append(filename)
            downloaded_set.add(filename)

            # Save progress after each file
            progress["downloaded_files"] = downloaded
            progress["total_partitions"] = len(filtered)
            self._save_progress(progress)

        progress["status"] = "complete"
        progress["total_files"] = len(downloaded)
        self._save_progress(progress)
        logger.info("Adverse event bulk download complete: %d files", len(downloaded))
        return progress

    def extract_api_sample(self, sample_size: int = 1000) -> dict:
        """Pull a small sample via API for cross-validation."""
        logger.info("Fetching API sample of %d adverse event records", sample_size)

        search = self.client.date_range_search("date_received", DATE_START, DATE_END)
        results = []
        fetched = 0
        limit = min(sample_size, 1000)

        while fetched < sample_size:
            page_limit = min(limit, sample_size - fetched)
            data = self.client.fetch_page(
                ENDPOINT_ADVERSE_EVENTS,
                search=search,
                skip=fetched,
                limit=page_limit,
            )
            page_results = data.get("results", [])
            if not page_results:
                break
            results.extend(page_results)
            fetched += len(page_results)

        self._save_raw_json(results, "adverse_events_sample.json")
        summary = {"method": "api_sample", "records": len(results)}
        logger.info("API sample complete: %d records", len(results))
        return summary

    @staticmethod
    def _filter_partitions(partitions: list[dict], start_year: int, end_year: int) -> list[dict]:
        """Filter download partitions to those within the date window.

        Partition filenames follow patterns like:
        - device-event-0001-of-0012.json.zip (undated, include them)
        - Or contain year info in display_name
        """
        filtered = []
        for partition in partitions:
            # Try to extract year from display_name or file path
            display = partition.get("display_name", "")
            file_path = partition.get("file", "")

            # Look for 4-digit year pattern
            year = None
            for token in display.split() + file_path.split("/"):
                token_clean = token.strip("-.()[]")
                if token_clean.isdigit() and len(token_clean) == 4:
                    candidate = int(token_clean)
                    if 2000 <= candidate <= 2030:
                        year = candidate
                        break

            # Include if year is in range, or if no year found (could be relevant)
            if year is None or (start_year <= year <= end_year):
                filtered.append(partition)

        return filtered
