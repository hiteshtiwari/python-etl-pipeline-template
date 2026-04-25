"""
S3 Extractor
------------
Reads CSV or Parquet files from AWS S3.
Supports single files and entire prefixes (folders).

Config example:
  extractor:
    type: s3
    name: s3_sales_data
    bucket: my-data-bucket
    prefix: raw/sales/2024/
    file_format: parquet   # or csv
    csv_options:
      delimiter: ","
      encoding: utf-8
"""

import boto3
import pandas as pd
import io
from .base import BaseExtractor


class S3Extractor(BaseExtractor):
    """Extract CSV or Parquet files from an S3 bucket."""

    def __init__(self, config: dict):
        super().__init__(config)
        self.bucket = config["bucket"]
        self.prefix = config.get("prefix", "")
        self.key = config.get("key")           # Single file
        self.file_format = config.get("file_format", "csv").lower()
        self.csv_options = config.get("csv_options", {})
        self.s3 = boto3.client("s3")

    def _extract(self) -> pd.DataFrame:
        if self.key:
            return self._read_file(self.key)
        else:
            return self._read_prefix(self.prefix)

    def _read_prefix(self, prefix: str) -> pd.DataFrame:
        """Read all matching files under a given S3 prefix."""
        paginator = self.s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self.bucket, Prefix=prefix)

        frames = []
        for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if self._is_target_file(key):
                    df = self._read_file(key)
                    frames.append(df)

        if not frames:
            raise ValueError(f"No {self.file_format} files found at s3://{self.bucket}/{prefix}")

        return pd.concat(frames, ignore_index=True)

    def _read_file(self, key: str) -> pd.DataFrame:
        response = self.s3.get_object(Bucket=self.bucket, Key=key)
        body = response["Body"].read()

        if self.file_format == "parquet":
            return pd.read_parquet(io.BytesIO(body))
        elif self.file_format == "csv":
            return pd.read_csv(io.BytesIO(body), **self.csv_options)
        else:
            raise ValueError(f"Unsupported file format: {self.file_format}")

    def _is_target_file(self, key: str) -> bool:
        return key.endswith(f".{self.file_format}")
