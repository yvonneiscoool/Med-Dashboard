"""Mapping module for entity standardization."""

from src.mapping.manufacturer import build_manufacturer_alias
from src.mapping.recall_product_code import map_recall_to_classification

__all__ = ["build_manufacturer_alias", "map_recall_to_classification"]
