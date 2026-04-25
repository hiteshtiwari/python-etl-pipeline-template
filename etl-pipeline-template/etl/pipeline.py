"""
Pipeline Runner
---------------
The main entry point. Loads config, runs extract → transform → quality check → load.
Handles all errors and sends Slack alerts automatically.

Usage:
  python -m etl.pipeline --config config/pipeline.yaml

Or import and run programmatically:
  from etl.pipeline import Pipeline
  pipeline = Pipeline("config/pipeline.yaml")
  pipeline.run()
"""

import time
import logging
import argparse
import yaml
from pathlib import Path

from etl.extractors.api_extractor import APIExtractor
from etl.extractors.s3_extractor import S3Extractor
from etl.extractors.postgres_extractor import PostgresExtractor
from etl.transformers.transformer import Transformer
from etl.transformers.quality_checker import DataQualityChecker
from etl.loaders.s3_loader import S3Loader
from etl.loaders.postgres_loader import PostgresLoader
from etl.alerting import SlackAlerter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

EXTRACTOR_MAP = {
    "api": APIExtractor,
    "s3": S3Extractor,
    "postgres": PostgresExtractor,
}

LOADER_MAP = {
    "s3": S3Loader,
    "postgres": PostgresLoader,
}


class Pipeline:
    """Orchestrates the full ETL pipeline from config."""

    def __init__(self, config_path: str):
        with open(config_path) as f:
            self.config = yaml.safe_load(f)
        self.alerter = SlackAlerter(self.config.get("alerting", {}))

    def run(self) -> None:
        start_time = time.time()
        pipeline_name = self.config.get("name", "ETL Pipeline")
        logger.info(f"{'='*50}")
        logger.info(f"Starting pipeline: {pipeline_name}")
        logger.info(f"{'='*50}")

        try:
            # ── EXTRACT ──────────────────────────────────
            extractor_config = self.config["extractor"]
            extractor_type = extractor_config["type"]
            if extractor_type not in EXTRACTOR_MAP:
                raise ValueError(f"Unknown extractor type: '{extractor_type}'. Options: {list(EXTRACTOR_MAP.keys())}")

            extractor = EXTRACTOR_MAP[extractor_type](extractor_config)
            df = extractor.extract()
            logger.info(f"Extracted {len(df)} rows, {len(df.columns)} columns.")

            # ── TRANSFORM ────────────────────────────────
            if "transformer" in self.config:
                transformer = Transformer(self.config["transformer"])
                df = transformer.transform(df)

            # ── QUALITY CHECKS ───────────────────────────
            if "quality_checks" in self.config:
                checker = DataQualityChecker(self.config["quality_checks"])
                checker.run(df)

            # ── LOAD ─────────────────────────────────────
            loader_config = self.config["loader"]
            loader_type = loader_config["type"]
            if loader_type not in LOADER_MAP:
                raise ValueError(f"Unknown loader type: '{loader_type}'. Options: {list(LOADER_MAP.keys())}")

            loader = LOADER_MAP[loader_type](loader_config)
            loader.load(df)

            # ── DONE ─────────────────────────────────────
            duration = time.time() - start_time
            logger.info(f"{'='*50}")
            logger.info(f"Pipeline '{pipeline_name}' COMPLETED in {duration:.1f}s")
            logger.info(f"{'='*50}")
            self.alerter.send_success(rows_processed=len(df), duration_seconds=duration)

        except Exception as e:
            logger.error(f"Pipeline FAILED: {e}", exc_info=True)
            self.alerter.send_failure(str(e))
            raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run ETL Pipeline")
    parser.add_argument("--config", required=True, help="Path to pipeline YAML config")
    args = parser.parse_args()
    pipeline = Pipeline(args.config)
    pipeline.run()
