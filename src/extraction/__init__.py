"""Data extraction modules for FDA data sources."""

from src.extraction.adverse_events import AdverseEventExtractor
from src.extraction.classification import ClassificationExtractor
from src.extraction.clearances import ClearanceExtractor
from src.extraction.recall_product_codes import RecallProductCodeExtractor
from src.extraction.recalls import RecallExtractor

__all__ = [
    "AdverseEventExtractor",
    "ClassificationExtractor",
    "ClearanceExtractor",
    "RecallExtractor",
    "RecallProductCodeExtractor",
]
