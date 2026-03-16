"""Run the full FDA Medical Device pipeline: extraction → cleaning → mapping → marts → app CSVs."""

import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("pipeline")


def main():
    t0 = time.time()

    # ── Step 1: Extraction ────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("STEP 1: EXTRACTION")
    logger.info("=" * 60)

    from src.extraction import (
        AdverseEventExtractor,
        ClassificationExtractor,
        ClearanceExtractor,
        RecallExtractor,
        RecallProductCodeExtractor,
    )

    ClassificationExtractor().extract()
    RecallExtractor().extract()
    RecallProductCodeExtractor().extract()
    ClearanceExtractor().extract()
    AdverseEventExtractor().extract(method="bulk")

    # ── Step 2: Cleaning ──────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("STEP 2: CLEANING")
    logger.info("=" * 60)

    from src.cleaning import (
        build_dim_product_code,
        clean_adverse_events,
        clean_clearances,
        clean_recalls,
    )

    dim_pc = build_dim_product_code()
    logger.info("dim_product_code: %d rows", len(dim_pc))

    events = clean_adverse_events()
    logger.info("clean_event_device_level: %d rows", len(events))

    recalls = clean_recalls()
    logger.info("clean_recall: %d rows", len(recalls))

    clearances = clean_clearances()
    logger.info("clean_510k: %d rows", len(clearances))

    # ── Step 3: Mapping ───────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("STEP 3: MAPPING")
    logger.info("=" * 60)

    from src.mapping import build_manufacturer_alias, map_recall_to_classification

    mfr_alias = build_manufacturer_alias()
    logger.info("dim_manufacturer_alias: %d rows", len(mfr_alias))

    recalls_mapped = map_recall_to_classification()
    logger.info("recalls mapped to classification: %d rows", len(recalls_mapped))

    # ── Step 4: Marts ─────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("STEP 4: MARTS")
    logger.info("=" * 60)

    from src.marts import build_all_marts

    marts = build_all_marts()
    for name, df in marts.items():
        logger.info("  %s: %d rows", name, len(df))

    # ── Step 5: App CSV Export ────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("STEP 5: APP CSV EXPORT")
    logger.info("=" * 60)

    from src.marts import export_all

    exports = export_all()
    for name, df in exports.items():
        logger.info("  %s: %d rows", name, len(df))

    elapsed = time.time() - t0
    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETE in %.1f seconds", elapsed)
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
