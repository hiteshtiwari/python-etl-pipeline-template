"""
Data Quality Checker
---------------------
Validates data before loading. Stops bad data from reaching your destination.

Config example:
  quality_checks:
    min_row_count: 100
    max_null_pct:
      customer_id: 0.0    # Zero nulls allowed
      amount: 0.05        # Max 5% nulls
    expected_columns: [customer_id, amount, created_at]
    value_ranges:
      amount:
        min: 0
        max: 1000000
"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)


class DataQualityChecker:
    """Run data quality checks and raise errors on failure."""

    def __init__(self, config: dict):
        self.checks = config  # full quality_checks config block

    def run(self, df: pd.DataFrame) -> None:
        """Run all configured checks. Raises ValueError on failure."""
        logger.info("Running data quality checks...")
        errors = []

        errors += self._check_row_count(df)
        errors += self._check_expected_columns(df)
        errors += self._check_null_percentages(df)
        errors += self._check_value_ranges(df)

        if errors:
            msg = "\n".join(f"  ❌ {e}" for e in errors)
            raise ValueError(f"Data quality checks FAILED:\n{msg}")

        logger.info(f"✅ All data quality checks passed. Rows: {len(df)}")

    def _check_row_count(self, df: pd.DataFrame) -> list:
        errors = []
        min_rows = self.checks.get("min_row_count")
        if min_rows and len(df) < min_rows:
            errors.append(f"Row count {len(df)} is below minimum {min_rows}")
        return errors

    def _check_expected_columns(self, df: pd.DataFrame) -> list:
        errors = []
        expected = self.checks.get("expected_columns", [])
        missing = [c for c in expected if c not in df.columns]
        if missing:
            errors.append(f"Missing expected columns: {missing}")
        return errors

    def _check_null_percentages(self, df: pd.DataFrame) -> list:
        errors = []
        null_config = self.checks.get("max_null_pct", {})
        for col, max_pct in null_config.items():
            if col not in df.columns:
                continue
            actual_pct = df[col].isnull().mean()
            if actual_pct > max_pct:
                errors.append(
                    f"Column '{col}' has {actual_pct:.1%} nulls (max allowed: {max_pct:.1%})"
                )
        return errors

    def _check_value_ranges(self, df: pd.DataFrame) -> list:
        errors = []
        ranges = self.checks.get("value_ranges", {})
        for col, bounds in ranges.items():
            if col not in df.columns:
                continue
            if "min" in bounds and (df[col] < bounds["min"]).any():
                errors.append(f"Column '{col}' has values below minimum {bounds['min']}")
            if "max" in bounds and (df[col] > bounds["max"]).any():
                errors.append(f"Column '{col}' has values above maximum {bounds['max']}")
        return errors
