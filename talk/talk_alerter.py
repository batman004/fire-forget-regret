import json
import time
from pathlib import Path

from loguru import logger
from stitch import Alert, Alerter

ALERTS_FILE = Path(__file__).parent / "output" / "alerts.jsonl"
CHANNELS = {"email": "oncall@example.com", "slack": "#data-alerts"}


class SimulatedAlerter(Alerter):
    def send(self, alert: Alert) -> None:
        for channel, destination in CHANNELS.items():
            self.deliver(channel, destination, alert)

    def deliver(self, channel: str, destination: str, alert: Alert) -> None:
        message = f"ALERT via {channel} to {destination}: {alert.title}"
        if alert.severity == "critical":
            logger.critical(message)
        else:
            logger.success(message)
        self.record(channel, destination, alert)

    def record(self, channel: str, destination: str, alert: Alert) -> None:
        ALERTS_FILE.parent.mkdir(exist_ok=True)
        entry = {"time": time.time(), "channel": channel, "to": destination, "severity": alert.severity,
                 "title": alert.title, "step": alert.step_name, **alert.detail}
        with ALERTS_FILE.open("a") as alerts_file:
            alerts_file.write(json.dumps(entry) + "\n")
