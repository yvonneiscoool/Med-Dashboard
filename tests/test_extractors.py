"""Tests for the data extractors."""

import json
from unittest.mock import MagicMock, patch

import pytest
import requests

from src.api.client import FDAClient
from src.extraction.adverse_events import AdverseEventExtractor
from src.extraction.classification import ClassificationExtractor
from src.extraction.clearances import ClearanceExtractor
from src.extraction.recalls import RecallExtractor


def _mock_response(status_code, json_data):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        resp.raise_for_status.side_effect = requests.HTTPError(response=resp)
    return resp


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
    def test_recall_extractor_fetches_without_date_filter(self, tmp_path, fda_client, sample_api_response):
        """RecallExtractor should fetch all device recalls without date filters."""
        fda_client._session.get.return_value = _mock_response(
            200,
            sample_api_response(
                total=2,
                results=[
                    {"recall_number": "Z-0001-2020", "product_type": "Devices"},
                    {"recall_number": "Z-0002-2021", "product_type": "Devices"},
                ],
            ),
        )

        extractor = RecallExtractor(client=fda_client)
        extractor.output_dir = tmp_path / "recalls"
        extractor.output_dir.mkdir()
        extractor._progress_file = extractor.output_dir / "_progress.json"

        result = extractor.extract()

        assert result["total_records"] == 2
        # Verify no date range in search params
        call_params = fda_client._session.get.call_args[1]["params"]
        assert "report_date" not in call_params.get("search", "")


class TestClearanceExtractor:
    def test_clearance_extractor_small_dataset_fetches_all(self, tmp_path, fda_client, sample_api_response):
        """ClearanceExtractor should fetch all at once if total <= 26K."""
        fda_client._session.get.return_value = _mock_response(
            200,
            sample_api_response(
                total=500,
                results=[{"k_number": "K201234", "decision_date": "2020-06-15"}],
            ),
        )

        extractor = ClearanceExtractor(client=fda_client)
        extractor.output_dir = tmp_path / "clearances"
        extractor.output_dir.mkdir()
        extractor._progress_file = extractor.output_dir / "_progress.json"

        result = extractor.extract()

        assert result["total_records"] >= 1
        call_params = fda_client._session.get.call_args[1]["params"]
        assert "decision_date" not in call_params.get("search", "")

    def test_clearance_extractor_large_dataset_partitions_by_committee(self, tmp_path, fda_client, sample_api_response):
        """ClearanceExtractor should partition by advisory_committee if > 26K."""
        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            params = kwargs.get("params", {})

            # Count-by query
            if "count" in params:
                return _mock_response(
                    200,
                    {"results": [{"term": "SU", "count": 5000}, {"term": "CV", "count": 3000}]},
                )
            # Initial count query returns > 26K
            if params.get("limit") == 1:
                return _mock_response(200, sample_api_response(total=30000, results=[]))

            # Partition fetches
            return _mock_response(
                200,
                sample_api_response(
                    total=5000,
                    results=[{"k_number": f"K20{call_count}", "advisory_committee": "SU"}],
                ),
            )

        fda_client._session.get.side_effect = side_effect

        extractor = ClearanceExtractor(client=fda_client)
        extractor.output_dir = tmp_path / "clearances"
        extractor.output_dir.mkdir()
        extractor._progress_file = extractor.output_dir / "_progress.json"

        result = extractor.extract()

        assert result["total_records"] >= 1


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
