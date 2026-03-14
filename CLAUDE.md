# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

FDA Medical Device Regulatory Risk Intelligence Dashboard — a Python data analytics project that integrates openFDA adverse event, recall, classification, and 510(k) clearance data into an interactive Tableau Public dashboard. This is a BI/data product, not a web application.

Full specification: `docs/project_design.md`

## Architecture

**Data pipeline (lakehouse pattern):**
- **Raw** (`data/raw/`): JSON/CSV from openFDA APIs
- **Clean** (`data/clean/`): Standardized parquet files (deduplicated, typed)
- **Mart** (`data/mart/`): Business-ready aggregated tables with KPIs
- **App** (`data/app/`): Dashboard-facing CSV extracts for Tableau

**Pipeline code** lives in `src/` with modules: `api/`, `extraction/`, `cleaning/`, `mapping/`, `marts/`, `qa/`

**Notebooks** (`notebooks/`) are numbered sequentially (01–06) for source validation through dashboard export.

**Four openFDA data sources:**
1. Device Adverse Events (MAUDE)
2. Device Classification (product code hierarchy)
3. Device Recall/Enforcement
4. Device 510(k) clearances

**Key mart tables:** `mart_panel_year`, `mart_product_code_year`, `mart_firm_product_year` — grain is documented in design spec section 7.3.

## Planned Tech Stack

Python with: pandas, duckdb, pyarrow, requests, rapidfuzz (manufacturer name matching), python-dotenv, tqdm. Storage is file-based (parquet), no database server.

## Development Environment

Always use the virtual environment (`.venv`) for all Python operations. Never use the system Python.

```bash
# Activate the virtual environment before any work
source .venv/bin/activate

# All python/pip commands must use the venv python
.venv/bin/python <script>
.venv/bin/pip install <package>

# Or after activating:
python <script>
pip install <package>
```

If the virtual environment does not exist, create it first:
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Commands

```bash
# Run tests
.venv/bin/python -m pytest

# Lint
.venv/bin/python -m ruff check .
.venv/bin/python -m ruff format --check .
```

## Workflow Orchestration

### 1. Plan Node Default
- Enter plan mode for ANY non-trivial task (3+ steps or architectural decisions)
- If something goes sideways, STOP and re-plan immediately — don't keep pushing
- Write detailed specs upfront to reduce ambiguity

### 2. Subagent Strategy
- Use subagents liberally to keep main context window clean
- One task per subagent for focused execution

### 3. Self-Improvement Loop
- After ANY correction from the user: update `tasks/lessons.md` with the pattern
- Review lessons at session start for relevant project

### 4. Verification Before Done
- Never mark a task complete without proving it works
- Ask yourself: "Would a staff engineer approve this?"
- Run tests, check logs, demonstrate correctness

### 5. Autonomous Bug Fixing
- When given a bug report: just fix it. Don't ask for hand-holding
- Point at logs, errors, failing tests — then resolve them

## Task Management

1. **Plan First**: Write plan to `tasks/todo.md` with checkable items
2. **Verify Plan**: Check in before starting implementation
3. **Track Progress**: Mark items complete as you go
4. **Document Results**: Add review section to `tasks/todo.md`
5. **Capture Lessons**: Update `tasks/lessons.md` after corrections

## Core Principles

- **Simplicity First**: Make every change as simple as possible. Impact minimal code.
- **No Laziness**: Find root causes. No temporary fixes. Senior developer standards.
- **Minimal Impact**: Changes should only touch what's necessary. Avoid introducing bugs.
