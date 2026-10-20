import json
import os
import time
from src.clock import utc_now
from src.event_producer import EventProducer


def poison_event(n: int) -> dict:
    return {
        "type": "refund",  # no consumer knows this type
        "produced_at": utc_now(),
        "user-id": f"user-{n}",
        "reason": "contract change test",
    }


def publish_poison_events() -> None:
    interval = float(os.getenv("POISON_PRODUCER_INTERVAL", "5"))
    producer = EventProducer()
    n = 1
    print("--- Producing poison events ---")
    while True:
        key = f"poison-{n}"
        value = poison_event(n)
        producer.publish(key, json.dumps(value))
        print(f"☠ Poison: {key} -> {value}")
        n += 1
        time.sleep(interval)