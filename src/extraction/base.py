"""Base extractor with shared logic for progress tracking and partitioning."""

import json
import logging
from abc import ABC, abstractmethod
from pathlib import Path

from src.api.client import FDAClient

logger = logging.getLogger(__name__)


class BaseExtractor(ABC):
    """Base class for FDA data extractors."""

    def __init__(self, client: FDAClient, output_dir: Path):
        self.client = client
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._progress_file = self.output_dir / "_progress.json"

    def _load_progress(self) -> dict:
        """Load progress from disk."""
        if self._progress_file.exists():
            return json.loads(self._progress_file.read_text())
        return {}

    def _save_progress(self, progress: dict) -> None:
        """Save progress to disk."""
        self._progress_file.write_text(json.dumps(progress, indent=2))

    def _save_raw_json(self, data: list[dict], filename: str) -> Path:
        """Save records as a JSON file in the output directory."""
        filepath = self.output_dir / filename
        filepath.parent.mkdir(parents=True, exist_ok=True)
        filepath.write_text(json.dumps(data, indent=2))
        logger.info("Saved %d records to %s", len(data), filepath)
        return filepath

    @staticmethod
    def _partition_by_year(start_year: int, end_year: int) -> list[tuple[str, str]]:
        """Generate yearly date ranges as (start, end) tuples in YYYY-MM-DD format."""
        ranges = []
        for year in range(start_year, end_year + 1):
            ranges.append((f"{year}-01-01", f"{year}-12-31"))
        return ranges

    @staticmethod
    def _partition_by_quarter(year: int) -> list[tuple[str, str]]:
        """Generate quarterly date ranges for a given year."""
        return [
            (f"{year}-01-01", f"{year}-03-31"),
            (f"{year}-04-01", f"{year}-06-30"),
            (f"{year}-07-01", f"{year}-09-30"),
            (f"{year}-10-01", f"{year}-12-31"),
        ]

    @staticmethod
    def _partition_by_month(year: int, quarter: int) -> list[tuple[str, str]]:
        """Generate monthly date ranges for a given quarter of a year."""
        month_starts = {1: [1, 2, 3], 2: [4, 5, 6], 3: [7, 8, 9], 4: [10, 11, 12]}
        months = month_starts[quarter]
        ranges = []
        for month in months:
            import calendar

            last_day = calendar.monthrange(year, month)[1]
            ranges.append((f"{year}-{month:02d}-01", f"{year}-{month:02d}-{last_day:02d}"))
        return ranges

    @abstractmethod
    def extract(self) -> dict:
        """Run the extraction. Returns a summary dict."""
        ...
