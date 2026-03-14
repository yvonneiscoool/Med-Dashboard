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

## Phase 2: Cleaning & Core Dimensions
- [x] Outcome code constants in `src/config.py`
- [x] QA module — `src/qa/checks.py` with QAResult, 7 check functions, run_checks
- [x] QA module tests — 22 tests (`tests/test_qa_checks.py`)
- [x] QA module re-exports (`src/qa/__init__.py`)
- [x] Classification dimension builder — `src/cleaning/classification.py`
- [x] Classification tests — 7 tests (`tests/test_cleaning_classification.py`)
- [x] Adverse event cleaner — `src/cleaning/adverse_events.py` (ZIP read, flatten, outcomes, dedup, normalize)
- [x] Adverse event tests — 20 tests (`tests/test_cleaning_adverse_events.py`)
- [x] Cleaning module re-exports (`src/cleaning/__init__.py`)
- [x] Manufacturer name standardizer — `src/mapping/manufacturer.py` (normalize, cluster, alias)
- [x] Manufacturer tests — 12 tests (`tests/test_mapping_manufacturer.py`)
- [x] Mapping module re-exports (`src/mapping/__init__.py`)
- [x] Event cleaning notebook (`notebooks/03_event_cleaning.ipynb`)
- [x] All 90 tests passing, lint clean
