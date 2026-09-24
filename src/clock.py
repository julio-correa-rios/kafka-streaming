from datetime import datetime, timezone
from confluent_kafka import Message

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