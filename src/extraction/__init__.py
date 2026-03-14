"""Data extraction modules for FDA data sources."""

from src.extraction.adverse_events import AdverseEventExtractor
from src.extraction.classification import ClassificationExtractor
from src.extraction.clearances import ClearanceExtractor
from src.extraction.recalls import RecallExtractor

__all__ = [
    "AdverseEventExtractor",
    "ClassificationExtractor",
    "ClearanceExtractor",
    "RecallExtractor",
]
