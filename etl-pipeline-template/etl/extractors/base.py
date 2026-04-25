"""
Base Extractor
--------------
All extractors inherit from this class.
Provides logging, error handling, and a common interface.
"""

import logging
import pandas as pd
from abc import ABC, abstractmethod
from typing import Optional

logger = logging.getLogger(__name__)


class BaseExtractor(ABC):
    """Abstract base class for all data extractors."""

    def __init__(self, config: dict):
        self.config = config
        self.source_name = config.get("name", "unknown")

    def extract(self) -> pd.DataFrame:
        """Extract data and return as a DataFrame."""
        logger.info(f"[{self.source_name}] Starting extraction...")
        try:
            df = self._extract()
            logger.info(f"[{self.source_name}] Extracted {len(df)} rows successfully.")
            return df
        except Exception as e:
            logger.error(f"[{self.source_name}] Extraction failed: {e}")
            raise

    @abstractmethod
    def _extract(self) -> pd.DataFrame:
        """Implement extraction logic in subclasses."""
        pass
