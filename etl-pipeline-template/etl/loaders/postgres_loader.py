"""
PostgreSQL Loader
-----------------
Writes a DataFrame to PostgreSQL.
Supports insert, replace, and upsert (insert on conflict update) modes.

Config example:
  loader:
    type: postgres
    connection_string: "postgresql://user:pass@host:5432/dbname"
    table: processed_orders
    mode: upsert          # insert | replace | upsert
    upsert_keys: [order_id]
    chunksize: 1000
"""

import pandas as pd
import sqlalchemy
import logging
from sqlalchemy.dialects.postgresql import insert as pg_insert

logger = logging.getLogger(__name__)


class PostgresLoader:
    """Write DataFrame to PostgreSQL with insert, replace, or upsert support."""

    def __init__(self, config: dict):
        self.connection_string = config["connection_string"]
        self.table = config["table"]
        self.mode = config.get("mode", "insert")
        self.upsert_keys = config.get("upsert_keys", [])
        self.chunksize = config.get("chunksize", 1000)
        self.engine = sqlalchemy.create_engine(self.connection_string)

    def load(self, df: pd.DataFrame) -> None:
        logger.info(f"Loading {len(df)} rows to table '{self.table}' (mode: {self.mode})")

        if self.mode == "replace":
            df.to_sql(self.table, self.engine, if_exists="replace", index=False,
                      chunksize=self.chunksize, method="multi")

        elif self.mode == "insert":
            df.to_sql(self.table, self.engine, if_exists="append", index=False,
                      chunksize=self.chunksize, method="multi")

        elif self.mode == "upsert":
            self._upsert(df)

        else:
            raise ValueError(f"Unsupported mode: {self.mode}. Use: insert | replace | upsert")

        logger.info(f"PostgreSQL load complete. Table: {self.table}")

    def _upsert(self, df: pd.DataFrame) -> None:
        """INSERT ... ON CONFLICT (keys) DO UPDATE SET ..."""
        if not self.upsert_keys:
            raise ValueError("upsert_keys must be set when mode is 'upsert'")

        meta = sqlalchemy.MetaData()
        table = sqlalchemy.Table(self.table, meta, autoload_with=self.engine)

        records = df.to_dict(orient="records")

        with self.engine.begin() as conn:
            for i in range(0, len(records), self.chunksize):
                batch = records[i:i + self.chunksize]
                stmt = pg_insert(table).values(batch)
                update_cols = {
                    c.name: stmt.excluded[c.name]
                    for c in table.columns
                    if c.name not in self.upsert_keys
                }
                stmt = stmt.on_conflict_do_update(
                    index_elements=self.upsert_keys,
                    set_=update_cols
                )
                conn.execute(stmt)

        logger.info(f"Upserted {len(records)} rows into '{self.table}'")
