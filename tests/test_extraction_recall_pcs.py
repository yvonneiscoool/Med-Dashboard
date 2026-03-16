"""Tests for RecallProductCodeExtractor."""

import json
from unittest.mock import MagicMock

from src.extraction.recall_product_codes import RecallProductCodeExtractor


def _mock_response(status_code, json_data):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.raise_for_status = MagicMock()
    return resp


class TestRecallProductCodeExtractor:
    def test_extracts_and_saves_lookup(self, tmp_path, fda_client, sample_api_response):
        """Should fetch recall records and save product code lookup JSON."""
        fda_client._session.get.return_value = _mock_response(
            200,
            sample_api_response(
                total=2,
                results=[
                    {"product_res_number": "Z-0001-2020", "product_code": "DXN"},
                    {"product_res_number": "Z-0002-2021", "product_code": "FRN"},
                ],
            ),
        )

        extractor = RecallProductCodeExtractor(client=fda_client)
        extractor.output_dir = tmp_path / "recall_product_codes"
        extractor.output_dir.mkdir()
        extractor._progress_file = extractor.output_dir / "_progress.json"

        result = extractor.extract()

        assert result["total_records"] == 2
        lookup_file = extractor.output_dir / "all" / "recall_product_codes_all.json"
        assert lookup_file.exists()
        saved = json.loads(lookup_file.read_text())
        assert len(saved) == 2
        assert saved[0]["product_res_number"] == "Z-0001-2020"

    def test_skips_when_already_complete(self, tmp_path, fda_client):
        """Should skip extraction if progress shows complete."""
        output_dir = tmp_path / "recall_product_codes"
        output_dir.mkdir(parents=True)
        (output_dir / "_progress.json").write_text(json.dumps({"status": "complete", "total_records": 100}))

        extractor = RecallProductCodeExtractor.__new__(RecallProductCodeExtractor)
        extractor.client = fda_client
        extractor.output_dir = output_dir
        extractor._progress_file = output_dir / "_progress.json"

        result = extractor.extract()

        assert result["total_records"] == 100
        # fetch_count should not have been called since progress is complete;
        # verified by the fact that the mock session.get was never invoked
        fda_client._session.get.assert_not_called()

    def test_partitions_by_product_code_when_large(self, tmp_path, fda_client, sample_api_response):
        """Should partition by product_code if total exceeds 26K."""
        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            params = kwargs.get("params", {})

            # Count-by query for product_code
            if "count" in params:
                return _mock_response(
                    200,
                    {
                        "results": [
                            {"term": "DXN", "count": 500},
                            {"term": "FRN", "count": 300},
                        ]
                    },
                )
            # Initial count query returns > 26K
            if params.get("limit") == 1:
                return _mock_response(200, sample_api_response(total=27000, results=[]))

            # Partition fetches
            return _mock_response(
                200,
                sample_api_response(
                    total=500,
                    results=[
                        {
                            "product_res_number": f"Z-{call_count:04d}-2023",
                            "product_code": "DXN",
                        }
                    ],
                ),
            )

        fda_client._session.get.side_effect = side_effect

        extractor = RecallProductCodeExtractor(client=fda_client)
        extractor.output_dir = tmp_path / "recall_product_codes"
        extractor.output_dir.mkdir()
        extractor._progress_file = extractor.output_dir / "_progress.json"

        result = extractor.extract()

        assert result["total_records"] == 2
