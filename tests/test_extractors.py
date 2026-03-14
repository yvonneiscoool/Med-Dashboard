"""Tests for the data extractors."""

import json
from unittest.mock import MagicMock, patch

import pytest

from src.api.client import FDAClient
from src.extraction.adverse_events import AdverseEventExtractor
from src.extraction.classification import ClassificationExtractor
from src.extraction.clearances import ClearanceExtractor
from src.extraction.recalls import RecallExtractor


@pytest.fixture
def mock_client():
    """Create a mock FDAClient."""
    client = MagicMock(spec=FDAClient)
    client.date_range_search = FDAClient.date_range_search
    client.combine_search = FDAClient.combine_search
    return client


class TestClassificationExtractor:
    def test_full_extraction(self, mock_client, tmp_path):
        """Should fetch all classification records and save to JSON."""
        mock_client.fetch_count.return_value = 3
        mock_client.fetch_all_pages.return_value = [
            {"product_code": "DXN", "device_name": "Test Device"},
            {"product_code": "DYB", "device_name": "Test Device 2"},
            {"product_code": "HQF", "device_name": "Test Device 3"},
        ]

        output_dir = tmp_path / "classification"
        extractor = ClassificationExtractor.__new__(ClassificationExtractor)
        extractor.client = mock_client
        extractor.output_dir = output_dir
        extractor.output_dir.mkdir(parents=True, exist_ok=True)
        extractor._progress_file = output_dir / "_progress.json"

        result = extractor.extract()

        assert result["status"] == "complete"
        assert result["total_records"] == 3
        assert (output_dir / "classification_all.json").exists()
        saved = json.loads((output_dir / "classification_all.json").read_text())
        assert len(saved) == 3

    def test_resume_skips_completed(self, mock_client, tmp_path):
        """Should skip extraction if already complete."""
        output_dir = tmp_path / "classification"
        output_dir.mkdir(parents=True)
        (output_dir / "_progress.json").write_text(
            json.dumps(
                {
                    "status": "complete",
                    "total_records": 5,
                }
            )
        )

        extractor = ClassificationExtractor.__new__(ClassificationExtractor)
        extractor.client = mock_client
        extractor.output_dir = output_dir
        extractor._progress_file = output_dir / "_progress.json"

        result = extractor.extract()

        assert result["status"] == "complete"
        mock_client.fetch_count.assert_not_called()


class TestRecallExtractor:
    def test_year_partitioning(self, mock_client, tmp_path):
        """Should extract recalls for each year."""
        mock_client.fetch_count.return_value = 100
        mock_client.fetch_all_pages.return_value = [{"recall_number": "Z-0001-2019"}]

        output_dir = tmp_path / "recalls"

        with patch("src.extraction.recalls.DATA_RAW", tmp_path):
            with patch("src.extraction.recalls.DATE_START", "2019-01-01"):
                with patch("src.extraction.recalls.DATE_END", "2019-12-31"):
                    extractor = RecallExtractor.__new__(RecallExtractor)
                    extractor.client = mock_client
                    extractor.output_dir = output_dir
                    extractor.output_dir.mkdir(parents=True, exist_ok=True)
                    extractor._progress_file = output_dir / "_progress.json"

                    result = extractor.extract()

        assert result["total_records"] == 1
        assert "2019" in result["years"]
        assert (output_dir / "2019" / "recalls_2019.json").exists()

    def test_auto_quarter_split_on_large_year(self, mock_client, tmp_path):
        """Should split into quarters when a year exceeds threshold."""
        # First call (year-level) returns too many
        # Subsequent calls (quarter-level) return manageable counts
        mock_client.fetch_count.side_effect = [26000, 6000, 7000, 6500, 6500]
        mock_client.fetch_all_pages.side_effect = [
            [{"id": i} for i in range(6000)],
            [{"id": i} for i in range(7000)],
            [{"id": i} for i in range(6500)],
            [{"id": i} for i in range(6500)],
        ]

        output_dir = tmp_path / "recalls"

        with patch("src.extraction.recalls.DATE_START", "2019-01-01"):
            with patch("src.extraction.recalls.DATE_END", "2019-12-31"):
                extractor = RecallExtractor.__new__(RecallExtractor)
                extractor.client = mock_client
                extractor.output_dir = output_dir
                extractor.output_dir.mkdir(parents=True, exist_ok=True)
                extractor._progress_file = output_dir / "_progress.json"

                result = extractor.extract()

        assert result["total_records"] == 26000


class TestClearanceExtractor:
    def test_year_partitioning(self, mock_client, tmp_path):
        """Should extract clearances for each year."""
        mock_client.fetch_count.return_value = 200
        mock_client.fetch_all_pages.return_value = [{"k_number": "K190001"}]

        output_dir = tmp_path / "clearances"

        with patch("src.extraction.clearances.DATE_START", "2019-01-01"):
            with patch("src.extraction.clearances.DATE_END", "2019-12-31"):
                extractor = ClearanceExtractor.__new__(ClearanceExtractor)
                extractor.client = mock_client
                extractor.output_dir = output_dir
                extractor.output_dir.mkdir(parents=True, exist_ok=True)
                extractor._progress_file = output_dir / "_progress.json"

                result = extractor.extract()

        assert result["total_records"] == 1
        assert (output_dir / "2019" / "clearances_2019.json").exists()


class TestAdverseEventExtractor:
    def test_download_index_parsing(self, mock_client, sample_download_index, tmp_path):
        """Should parse download.json and filter partitions."""
        partitions = sample_download_index["results"]["device"]["event"]["partitions"]
        filtered = AdverseEventExtractor._filter_partitions(partitions, 2019, 2025)

        # Should include 2019 and 2020, exclude 2017
        assert len(filtered) == 2
        assert any("2019" in p["display_name"] for p in filtered)
        assert any("2020" in p["display_name"] for p in filtered)

    def test_skip_already_downloaded(self, sample_download_index, tmp_path):
        """Should skip files that are already downloaded."""
        mock_client = MagicMock()
        mock_client.date_range_search = FDAClient.date_range_search
        mock_client.combine_search = FDAClient.combine_search

        output_dir = tmp_path / "adverse_events"
        output_dir.mkdir(parents=True)

        # Pre-populate progress with one file already done (uses subdir_filename format)
        (output_dir / "_progress.json").write_text(
            json.dumps(
                {
                    "downloaded_files": ["2019q1_device-event-0001-of-0004.json.zip"],
                    "total_partitions": 2,
                }
            )
        )

        # Mock the download index response
        mock_resp = MagicMock()
        mock_resp.json.return_value = sample_download_index
        mock_resp.raise_for_status.return_value = None
        mock_client._session.get.return_value = mock_resp

        extractor = AdverseEventExtractor.__new__(AdverseEventExtractor)
        extractor.client = mock_client
        extractor.output_dir = output_dir
        extractor._progress_file = output_dir / "_progress.json"

        with patch("src.extraction.adverse_events.DATE_START", "2019-01-01"):
            with patch("src.extraction.adverse_events.DATE_END", "2025-12-31"):
                extractor.extract_bulk()

        # Should only download the one file it hasn't seen yet (2020 Q1)
        # 2017 Q1 is filtered out, 2019 Q1 already downloaded
        assert mock_client.download_file.call_count == 1

    def test_extract_method_dispatch(self, mock_client, tmp_path):
        """Should dispatch to correct method based on argument."""
        extractor = AdverseEventExtractor.__new__(AdverseEventExtractor)
        extractor.client = mock_client
        extractor.output_dir = tmp_path / "ae"
        extractor.output_dir.mkdir(parents=True)
        extractor._progress_file = extractor.output_dir / "_progress.json"

        with pytest.raises(ValueError, match="Unknown extraction method"):
            extractor.extract(method="invalid")
