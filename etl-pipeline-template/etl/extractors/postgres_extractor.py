"""
PostgreSQL Extractor
--------------------
Pulls data from a PostgreSQL table using a SQL query.
Supports incremental extraction via a watermark column.

Config example:
  extractor:
    type: postgres
    name: pg_orders
    connection_string: "postgresql://user:pass@host:5432/dbname"
    query: "SELECT * FROM orders WHERE updated_at > '{watermark}'"
    watermark_column: updated_at
    initial_watermark: "2024-01-01"
"""

import pandas as pd
import sqlalchemy
from .base import BaseExtractor


class PostgresExtractor(BaseExtractor):
    """Extract data from PostgreSQL with optional incremental loading."""

    def __init__(self, config: dict):
        super().__init__(config)
        self.connection_string = config["connection_string"]
        self.query = config["query"]
        self.watermark_column = config.get("watermark_column")
        self.watermark_value = config.get("initial_watermark", "1970-01-01")
        self.chunksize = config.get("chunksize", 10000)

    def _extract(self) -> pd.DataFrame:
        engine = sqlalchemy.create_engine(self.connection_string)

        # Replace watermark placeholder if provided
        query = self.query
        if "{watermark}" in query and self.watermark_value:
            query = query.format(watermark=self.watermark_value)

        frames = []
        with engine.connect() as conn:
            for chunk in pd.read_sql(query, conn, chunksize=self.chunksize):
                frames.append(chunk)

        if not frames:
            return pd.DataFrame()

        df = pd.concat(frames, ignore_index=True)

        # Update watermark for next run
        if self.watermark_column and self.watermark_column in df.columns:
            self.watermark_value = str(df[self.watermark_column].max())

        return df
