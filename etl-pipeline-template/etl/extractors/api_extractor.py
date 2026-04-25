"""
API Extractor
-------------
Pulls data from any REST API endpoint.
Supports pagination, auth headers, and retry logic.

Config example (config/pipeline.yaml):
  extractor:
    type: api
    name: my_api
    url: https://api.example.com/data
    headers:
      Authorization: "Bearer YOUR_TOKEN"
    params:
      page_size: 100
    pagination:
      enabled: true
      page_param: page
      max_pages: 10
"""

import time
import requests
import pandas as pd
from typing import Optional
from .base import BaseExtractor


class APIExtractor(BaseExtractor):
    """Extract data from a REST API with pagination and retry support."""

    def __init__(self, config: dict):
        super().__init__(config)
        self.url = config["url"]
        self.headers = config.get("headers", {})
        self.params = config.get("params", {})
        self.pagination = config.get("pagination", {})
        self.max_retries = config.get("max_retries", 3)
        self.retry_delay = config.get("retry_delay_seconds", 2)

    def _extract(self) -> pd.DataFrame:
        all_records = []

        if self.pagination.get("enabled", False):
            all_records = self._extract_paginated()
        else:
            data = self._fetch_with_retry(self.url, self.params)
            all_records = self._parse_response(data)

        return pd.DataFrame(all_records)

    def _extract_paginated(self) -> list:
        records = []
        page = 1
        max_pages = self.pagination.get("max_pages", 100)
        page_param = self.pagination.get("page_param", "page")

        while page <= max_pages:
            params = {**self.params, page_param: page}
            data = self._fetch_with_retry(self.url, params)
            batch = self._parse_response(data)

            if not batch:
                break  # No more data

            records.extend(batch)
            page += 1

        return records

    def _fetch_with_retry(self, url: str, params: dict) -> dict:
        for attempt in range(1, self.max_retries + 1):
            try:
                response = requests.get(url, headers=self.headers, params=params, timeout=30)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                if attempt == self.max_retries:
                    raise
                wait = self.retry_delay * (2 ** (attempt - 1))  # Exponential backoff
                print(f"Attempt {attempt} failed. Retrying in {wait}s... Error: {e}")
                time.sleep(wait)

    def _parse_response(self, data) -> list:
        """Handle both list and dict responses."""
        if isinstance(data, list):
            return data
        # Try common keys where data is nested
        for key in ["data", "results", "items", "records"]:
            if key in data:
                return data[key]
        return [data]  # Wrap single object as list
