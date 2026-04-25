"""
Unit Tests
----------
Run with: pytest tests/ -v
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from etl.transformers.transformer import Transformer
from etl.transformers.quality_checker import DataQualityChecker


# ── TRANSFORMER TESTS ────────────────────────────────────────

class TestTransformer:

    def _make_df(self):
        return pd.DataFrame({
            "cust_id": [1, 2, 2, None],
            "amt": ["10.5", "20.0", "20.0", "5.0"],
            "created_at": ["2024-01-01", "2024-01-02", "2024-01-02", "2024-01-03"],
        })

    def test_rename_columns(self):
        df = self._make_df()
        t = Transformer({"steps": [{"type": "rename_columns", "mapping": {"cust_id": "customer_id", "amt": "amount"}}]})
        result = t.transform(df)
        assert "customer_id" in result.columns
        assert "amount" in result.columns
        assert "cust_id" not in result.columns

    def test_cast_types(self):
        df = self._make_df()
        t = Transformer({"steps": [{"type": "cast_types", "columns": {"amt": "float", "created_at": "datetime"}}]})
        result = t.transform(df)
        assert result["amt"].dtype == float
        assert pd.api.types.is_datetime64_any_dtype(result["created_at"])

    def test_drop_duplicates(self):
        df = self._make_df()
        t = Transformer({"steps": [{"type": "drop_duplicates", "subset": ["cust_id"]}]})
        result = t.transform(df)
        assert len(result) < len(df)

    def test_drop_nulls(self):
        df = self._make_df()
        t = Transformer({"steps": [{"type": "drop_nulls", "columns": ["cust_id"]}]})
        result = t.transform(df)
        assert result["cust_id"].isnull().sum() == 0

    def test_filter_rows(self):
        df = pd.DataFrame({"amount": [10.0, -5.0, 0.0, 100.0]})
        t = Transformer({"steps": [{"type": "filter_rows", "column": "amount", "operator": ">", "value": 0}]})
        result = t.transform(df)
        assert (result["amount"] > 0).all()

    def test_add_column_now(self):
        df = pd.DataFrame({"a": [1, 2, 3]})
        t = Transformer({"steps": [{"type": "add_column", "name": "ingested_at", "value": "now"}]})
        result = t.transform(df)
        assert "ingested_at" in result.columns
        assert result["ingested_at"].notnull().all()

    def test_unknown_step_raises(self):
        df = pd.DataFrame({"a": [1]})
        t = Transformer({"steps": [{"type": "unknown_step"}]})
        with pytest.raises(ValueError, match="Unknown transform step"):
            t.transform(df)


# ── DATA QUALITY TESTS ───────────────────────────────────────

class TestDataQualityChecker:

    def _make_df(self):
        return pd.DataFrame({
            "customer_id": [1, 2, 3],
            "amount": [10.0, 20.0, 30.0],
            "created_at": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
        })

    def test_passes_all_checks(self):
        df = self._make_df()
        checker = DataQualityChecker({
            "min_row_count": 2,
            "expected_columns": ["customer_id", "amount"],
            "max_null_pct": {"amount": 0.0},
            "value_ranges": {"amount": {"min": 0, "max": 1000}},
        })
        checker.run(df)  # Should not raise

    def test_fails_min_row_count(self):
        df = self._make_df()
        checker = DataQualityChecker({"min_row_count": 100})
        with pytest.raises(ValueError, match="Row count"):
            checker.run(df)

    def test_fails_missing_column(self):
        df = self._make_df()
        checker = DataQualityChecker({"expected_columns": ["customer_id", "non_existent_col"]})
        with pytest.raises(ValueError, match="Missing expected columns"):
            checker.run(df)

    def test_fails_null_check(self):
        df = self._make_df()
        df.loc[0, "customer_id"] = None
        checker = DataQualityChecker({"max_null_pct": {"customer_id": 0.0}})
        with pytest.raises(ValueError, match="nulls"):
            checker.run(df)

    def test_fails_value_range(self):
        df = self._make_df()
        checker = DataQualityChecker({"value_ranges": {"amount": {"min": 0, "max": 15}}})
        with pytest.raises(ValueError, match="above maximum"):
            checker.run(df)
