from datetime import datetime, timezone
from confluent_kafka import Message
import random
import time

# Clock helper
def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def event_time(payload: dict, message: Message) -> datetime:
    raw = payload.get("produced_at")
    if raw:
        return datetime.fromisoformat(raw)
    ts_type, ts_ms = message.timestamp()
    if ts_ms and ts_ms > 0:
        return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return datetime.now(timezone.utc)

def sleep_with_variation(interval: float, variation: float) -> None:
    delay = interval + random.uniform(-variation, variation)
    delay = max(0, delay)
    time.sleep(delay)