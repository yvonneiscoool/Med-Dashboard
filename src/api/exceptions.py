"""Custom exceptions for FDA API interactions."""


class FDAApiError(Exception):
    """Base exception for FDA API errors."""

    def __init__(self, message: str, status_code: int | None = None, response_body: str | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body


class PartitionTooLargeError(FDAApiError):
    """Raised when a query returns more results than the API pagination limit (26,000)."""

    pass


class RateLimitExceeded(FDAApiError):
    """Raised when retries are exhausted on HTTP 429 responses."""

    pass
