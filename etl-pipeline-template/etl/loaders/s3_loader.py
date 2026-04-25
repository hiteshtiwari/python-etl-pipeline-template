"""
S3 Loader
---------
Writes a DataFrame to AWS S3 as Parquet or CSV.
Supports partitioning by date columns for efficient querying.

Config example:
  loader:
    type: s3
    bucket: my-processed-bucket
    prefix: processed/orders/
    file_format: parquet     # or csv
    partition_by: created_at # Optional: partitions as year=.../month=...
    mode: overwrite          # or append
"""

import boto3
import pandas as pd
import io
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class S3Loader:
    """Write a DataFrame to S3 as Parquet or CSV."""

    def __init__(self, config: dict):
        self.bucket = config["bucket"]
        self.prefix = config.get("prefix", "")
        self.file_format = config.get("file_format", "parquet")
        self.partition_by = config.get("partition_by")
        self.mode = config.get("mode", "overwrite")
        self.s3 = boto3.client("s3")

    def load(self, df: pd.DataFrame) -> None:
        logger.info(f"Loading {len(df)} rows to s3://{self.bucket}/{self.prefix}")

        if self.partition_by and self.partition_by in df.columns:
            self._write_partitioned(df)
        else:
            self._write_single(df)

        logger.info("S3 load complete.")

    def _write_single(self, df: pd.DataFrame) -> None:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        key = f"{self.prefix}data_{timestamp}.{self.file_format}"
        self._upload(df, key)

    def _write_partitioned(self, df: pd.DataFrame) -> None:
        col = pd.to_datetime(df[self.partition_by])
        df = df.copy()
        df["_year"] = col.dt.year
        df["_month"] = col.dt.month

        for (year, month), group in df.groupby(["_year", "_month"]):
            group = group.drop(columns=["_year", "_month"])
            key = f"{self.prefix}year={year}/month={month:02d}/data.{self.file_format}"
            self._upload(group, key)
            logger.info(f"  Written partition year={year}/month={month:02d} ({len(group)} rows)")

    def _upload(self, df: pd.DataFrame, key: str) -> None:
        buffer = io.BytesIO()
        if self.file_format == "parquet":
            df.to_parquet(buffer, index=False)
        elif self.file_format == "csv":
            df.to_csv(buffer, index=False)
        else:
            raise ValueError(f"Unsupported format: {self.file_format}")

        buffer.seek(0)
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=buffer.getvalue())
        logger.info(f"  Uploaded: s3://{self.bucket}/{key}")
