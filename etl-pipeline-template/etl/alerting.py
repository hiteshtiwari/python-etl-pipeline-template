"""
Slack Alerter
-------------
Sends pipeline status notifications to a Slack channel.
Clients LOVE this — they can monitor pipelines without logging in anywhere.

Setup:
  1. Create a Slack App at https://api.slack.com/apps
  2. Add 'Incoming Webhooks' and enable it
  3. Copy the Webhook URL into your config

Config example:
  alerting:
    slack_webhook_url: "https://hooks.slack.com/services/XXX/YYY/ZZZ"
    channel: "#data-pipelines"
    pipeline_name: "Orders ETL"
"""

import requests
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class SlackAlerter:
    """Send pipeline success/failure alerts to Slack."""

    def __init__(self, config: dict):
        self.webhook_url = config.get("slack_webhook_url")
        self.pipeline_name = config.get("pipeline_name", "ETL Pipeline")
        self.enabled = bool(self.webhook_url)

    def send_success(self, rows_processed: int, duration_seconds: float) -> None:
        if not self.enabled:
            return
        message = {
            "text": f"✅ *{self.pipeline_name}* completed successfully",
            "attachments": [{
                "color": "#36a64f",
                "fields": [
                    {"title": "Rows Processed", "value": f"{rows_processed:,}", "short": True},
                    {"title": "Duration", "value": f"{duration_seconds:.1f}s", "short": True},
                    {"title": "Finished At", "value": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"), "short": True},
                    {"title": "Status", "value": "SUCCESS 🎉", "short": True},
                ]
            }]
        }
        self._send(message)

    def send_failure(self, error: str) -> None:
        if not self.enabled:
            return
        message = {
            "text": f"❌ *{self.pipeline_name}* FAILED",
            "attachments": [{
                "color": "#ff0000",
                "fields": [
                    {"title": "Error", "value": str(error)[:500], "short": False},
                    {"title": "Failed At", "value": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"), "short": True},
                    {"title": "Action Required", "value": "Check logs immediately ⚠️", "short": True},
                ]
            }]
        }
        self._send(message)

    def send_quality_warning(self, warning: str) -> None:
        if not self.enabled:
            return
        message = {
            "text": f"⚠️ *{self.pipeline_name}* — Data Quality Warning",
            "attachments": [{
                "color": "#ffa500",
                "fields": [
                    {"title": "Warning", "value": warning[:500], "short": False},
                    {"title": "Time", "value": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"), "short": True},
                ]
            }]
        }
        self._send(message)

    def _send(self, payload: dict) -> None:
        try:
            resp = requests.post(self.webhook_url, json=payload, timeout=10)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Slack alert failed (non-critical): {e}")
