# Task Tracking

## Phase 0: Project Scaffolding
- [x] Directory structure
- [x] requirements.txt
- [x] .env.example and src/config.py
- [x] pyproject.toml
- [x] .gitignore updates
- [x] Virtual environment setup and verification

## Phase 1: Source Validation & Extraction
- [x] API exceptions module (`src/api/exceptions.py`)
- [x] API client with rate limiting, retry, pagination (`src/api/client.py`)
- [x] API module re-exports (`src/api/__init__.py`)
- [x] Base extractor with progress tracking (`src/extraction/base.py`)
- [x] Classification extractor (`src/extraction/classification.py`)
- [x] Recall extractor with year partitioning (`src/extraction/recalls.py`)
- [x] 510(k) clearance extractor (`src/extraction/clearances.py`)
- [x] Adverse event extractor — bulk + API sample (`src/extraction/adverse_events.py`)
- [x] Extraction module re-exports (`src/extraction/__init__.py`)
- [x] Test fixtures (`tests/conftest.py`)
- [x] API client tests — 18 tests (`tests/test_api_client.py`)
- [x] Extractor tests — 8 tests (`tests/test_extractors.py`)
- [x] Source validation notebook (`notebooks/01_source_validation.ipynb`)
- [x] Full extraction notebook (`notebooks/02_data_extraction.ipynb`)
- [x] All 27 tests passing, lint clean
