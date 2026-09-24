import random
import json
import os
import time
from src.clock import utc_now
from src.event_producer import EventProducer


def random_inference(event_id: str) -> dict:
    duration = random.randint(5, 30)
    distance = round(random.uniform(1.5, 20.0), 1)
    n = event_id.split("-")[1]
    return {
        "type": "inference",
        "produced_at": utc_now(),
        "user-id": f"user-{n}",
        "duration": f"{duration} minutes",
        "distance": f"{distance} km",
    }


def publish_inference_events() -> None:
    interval = float(os.getenv("PRODUCER_INTERVAL", "2.0"))
    producer = EventProducer()
    next_id = 1
    print("--- Producing inference events (Ctrl+C to stop when run using python main.py -i) ---")
    while True:
        event_key = f"infer-{next_id}"
        event_value = random_inference(event_key)
        producer.publish(event_key, json.dumps(event_value))
        print(f"✓ Inference request: {event_key} -> {event_value}")
        next_id += 1
        time.sleep(interval)