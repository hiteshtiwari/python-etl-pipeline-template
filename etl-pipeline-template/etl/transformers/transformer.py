"""
Transformer
-----------
Applies a chain of transformations to a DataFrame.
All transforms are driven by config — no code changes needed.

Config example:
  transformer:
    steps:
      - type: rename_columns
        mapping:
          cust_id: customer_id
          amt: amount

      - type: cast_types
        columns:
          amount: float
          created_at: datetime
          is_active: bool

      - type: drop_duplicates
        subset: [customer_id, created_at]

      - type: drop_nulls
        columns: [customer_id, amount]

      - type: filter_rows
        column: amount
        operator: ">"
        value: 0

      - type: add_column
        name: ingested_at
        value: now
"""

import pandas as pd
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class Transformer:
    """Applies a sequence of transformation steps to a DataFrame."""

    SUPPORTED_STEPS = [
        "rename_columns", "cast_types", "drop_duplicates",
        "drop_nulls", "filter_rows", "add_column", "select_columns"
    ]

    def __init__(self, config: dict):
        self.steps = config.get("steps", [])

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info(f"Starting transformation. Input shape: {df.shape}")

        for i, step in enumerate(self.steps):
            step_type = step.get("type")
            if step_type not in self.SUPPORTED_STEPS:
                raise ValueError(f"Unknown transform step: '{step_type}'. Supported: {self.SUPPORTED_STEPS}")
            try:
                df = self._apply_step(df, step)
                logger.info(f"Step {i+1} [{step_type}] done. Shape: {df.shape}")
            except Exception as e:
                logger.error(f"Step {i+1} [{step_type}] failed: {e}")
                raise

        logger.info(f"Transformation complete. Output shape: {df.shape}")
        return df

    def _apply_step(self, df: pd.DataFrame, step: dict) -> pd.DataFrame:
        t = step["type"]

        if t == "rename_columns":
            return df.rename(columns=step["mapping"])

        elif t == "cast_types":
            for col, dtype in step["columns"].items():
                if col not in df.columns:
                    logger.warning(f"Column '{col}' not found, skipping cast.")
                    continue
                if dtype == "datetime":
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                elif dtype == "bool":
                    df[col] = df[col].astype(bool)
                else:
                    df[col] = df[col].astype(dtype, errors="ignore")
            return df

        elif t == "drop_duplicates":
            subset = step.get("subset")
            return df.drop_duplicates(subset=subset)

        elif t == "drop_nulls":
            cols = step.get("columns")
            return df.dropna(subset=cols)

        elif t == "filter_rows":
            col = step["column"]
            op = step["operator"]
            val = step["value"]
            ops = {">": "__gt__", ">=": "__ge__", "<": "__lt__",
                   "<=": "__le__", "==": "__eq__", "!=": "__ne__"}
            if op not in ops:
                raise ValueError(f"Unsupported filter operator: {op}")
            return df[getattr(df[col], ops[op])(val)]

        elif t == "add_column":
            name = step["name"]
            value = step["value"]
            if value == "now":
                df[name] = datetime.now(timezone.utc)
            else:
                df[name] = value
            return df

        elif t == "select_columns":
            cols = [c for c in step["columns"] if c in df.columns]
            return df[cols]

        return df
