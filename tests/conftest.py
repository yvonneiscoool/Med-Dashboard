"""Shared test fixtures for FDA pipeline tests."""

from unittest.mock import MagicMock, patch

import pytest

from src.api.client import FDAClient


@pytest.fixture
def mock_session():
    """Mock requests.Session for testing without network calls."""
    with patch("src.api.client.requests.Session") as mock:
        session = MagicMock()
        mock.return_value = session
        yield session


@pytest.fixture
def fda_client(mock_session):
    """FDAClient with mocked session and no rate limit delays."""
    client = FDAClient(api_key="test_key", rate_limit=10000)
    client._session = mock_session
    return client


@pytest.fixture
def sample_api_response():
    """Factory for creating mock API responses."""

    def _make(results: list[dict], total: int = 100):
        return {
            "meta": {
                "results": {"skip": 0, "limit": 100, "total": total},
            },
            "results": results,
        }

    return _make


@pytest.fixture
def sample_download_index():
    """Sample download.json response for adverse event bulk download."""
    return {
        "results": {
            "device": {
                "event": {
                    "partitions": [
                        {
                            "file": "https://download.open.fda.gov/device/event/2019q1/device-event-0001-of-0004.json.zip",
                            "display_name": "2019 Q1",
                            "size_mb": "120",
                        },
                        {
                            "file": "https://download.open.fda.gov/device/event/2020q1/device-event-0001-of-0004.json.zip",
                            "display_name": "2020 Q1",
                            "size_mb": "130",
                        },
                        {
                            "file": "https://download.open.fda.gov/device/event/2017q1/device-event-0001-of-0002.json.zip",
                            "display_name": "2017 Q1",
                            "size_mb": "80",
                        },
                    ]
                }
            }
        }
    }


@pytest.fixture
def tmp_data_dir(tmp_path):
    """Temporary data directory for extraction tests."""
    return tmp_path / "data" / "raw"
