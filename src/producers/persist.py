import random
import json
import os
import time
from src.clock import utc_now, sleep_with_variation
from src.event_producer import EventProducer

# Interval and variation
import dotenv
dotenv.load_dotenv()
INTERVAL = float(os.getenv("PRODUCER_INTERVAL"))
VARIATION = float(os.getenv("PRODUCER_INTERVAL_VARIATION"))

## Random events generator
def random_event(event_id: str) -> dict:
    duration = random.randint(5, 30)
    distance = round(random.uniform(1.5, 20.0), 1)
    amount = round(distance * 2.5, 2)
    n = event_id.split("-")[1]
    return {
        "type": "persist",
        "produced_at": utc_now(),
        "vehicle-id": f"vehicle-{n}",
        "driver-id": f"driver-{n}",
        "user-id": f"user-{n}",
        "duration": f"{duration} minutes",
        "distance": f"{distance} km",
        "amount": f"${amount:.2f}",
    }

def publish_random_events() -> None:
    interval = float(os.getenv("PRODUCER_INTERVAL", "2.0"))
    replay_chance = float(os.getenv("PRODUCER_REPLAY_CHANCE", "0.3"))

    producer = EventProducer()
    seen: dict[str, dict] = {}
    next_id = 4  # 1–3 already exist from the static producer
    print("--- Producing random events (Ctrl+C to stop when run using python main.py -p) ---")
    while True:
        if seen and random.random() < replay_chance:
            event_key = random.choice(list(seen))
            event_value = seen[event_key]
            kind = "replay"
        else:
            event_key = f"event-{next_id}"
            event_value = random_event(event_key)
            seen[event_key] = event_value
            next_id += 1
            kind = "new"
        producer.publish(event_key, json.dumps(event_value))
        print(f"✓ Produced ({kind}): {event_key} -> {event_value}")
        sleep_with_variation(INTERVAL, VARIATION)