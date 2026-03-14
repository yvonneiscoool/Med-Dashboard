"""Central configuration for the FDA Medical Device Dashboard pipeline."""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ── Paths ──────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_CLEAN = PROJECT_ROOT / "data" / "clean"
DATA_MART = PROJECT_ROOT / "data" / "mart"
DATA_APP = PROJECT_ROOT / "data" / "app"

# ── openFDA API ────────────────────────────────────────────────────────────────
FDA_BASE_URL = "https://api.fda.gov"
FDA_API_KEY = os.getenv("FDA_API_KEY")

ENDPOINT_ADVERSE_EVENTS = "/device/event.json"
ENDPOINT_CLASSIFICATION = "/device/classification.json"
ENDPOINT_RECALL = "/device/enforcement.json"
ENDPOINT_510K = "/device/510k.json"

# ── Time window ────────────────────────────────────────────────────────────────
DATE_START = "2019-01-01"
DATE_END = "2025-12-31"

# ── Rate limiting & retries ────────────────────────────────────────────────────
API_RATE_LIMIT = 240  # requests/min with API key
API_RATE_LIMIT_NO_KEY = 120  # requests/min without key
MAX_RETRIES = 3

# ── Patient outcome codes ─────────────────────────────────────────────────────
OUTCOME_DEATH = {"D"}
OUTCOME_SERIOUS_INJURY = {"L", "H", "S"}
