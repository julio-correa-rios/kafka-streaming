import argparse
import json
import os
import random
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from confluent_kafka import Message
from dotenv import load_dotenv
from sqlalchemy.dialects.postgresql import insert

from src.event_consumer import EventConsumer
from src.event_producer import EventProducer
from src.models.event import Event, InferenceResult
from src.models.utils import create_tables, get_session

load_dotenv()

# CLOCK HELPERS
# utc_now() → written into Kafka.
# event_time() → read back (JSON first, Kafka timestamp if old messages have no field).

# Price per km
PRICE_PER_KM = Decimal("2.5")

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


def persist_event(message: Message) -> None:
    event_id = message.key().decode()
    raw = message.value().decode()
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        print(f"↷ Skipped old message: {event_id} -> {raw}")
        return

    if not isinstance(payload, dict) or "vehicle-id" not in payload:
        print(f"↷ Skipped incomplete message: {event_id} -> {raw}")
        return

    stmt = (
        insert(Event)
        .values(
            id=event_id,
            vehicle_id=payload["vehicle-id"],
            driver_id=payload["driver-id"],
            user_id=payload["user-id"],
            duration=payload["duration"],
            distance_km=Decimal(payload["distance"].replace(" km", "")),
            amount=Decimal(payload["amount"].replace("$", "")),
            produced_at=event_time(payload, message),
        )
        .on_conflict_do_nothing(index_elements=["id"])
    )
    with get_session() as session:
        session.execute(stmt)
        session.commit()
    print(f"✓ Persisted: {event_id} -> {payload}")


def infer_event(message: Message, payload: dict) -> None:
    event_id = message.key().decode()
    if "distance" not in payload or "user-id" not in payload:
        print(f"↷ Skipped incomplete inference: {event_id}")
        return

    distance_km = Decimal(payload["distance"].replace(" km", ""))
    price = (distance_km * PRICE_PER_KM).quantize(Decimal("0.01"))

    stmt = (
        insert(InferenceResult)
        .values(
            id=event_id,
            user_id=payload["user-id"],
            duration=payload.get("duration", ""),
            distance_km=distance_km,
            recommended_price=price,
            produced_at=event_time(payload, message),
        )
        .on_conflict_do_nothing(index_elements=["id"])
    )
    with get_session() as session:
        session.execute(stmt)
        session.commit()
    print(f"✓ Inferred: {event_id} -> {price} ({payload})")


def handle_event(message: Message) -> None:
    event_id = message.key().decode()
    raw = message.value().decode()
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        print(f"↷ Skipped old message: {event_id} -> {raw}")
        return

    if not isinstance(payload, dict):
        print(f"↷ Skipped incomplete message: {event_id}")
        return

    event_type = payload.get("type", "persist")
    if event_type == "persist":
        persist_event(message)
    elif event_type == "inference":
        infer_event(message, payload)
    else:
        print(f"↷ Unknown type: {event_id} -> {event_type}")



def consume_events(limit: Optional[int] = None) -> None:
    create_tables()
    consumer = EventConsumer()

    print("\n--- Consuming events ---")
    try:
        consumer.consume(handle_event, limit=limit)
    except KeyboardInterrupt:
        print("\n✓ Stopped consuming")
    finally:
        consumer.close()

def main() -> None:
        
    parser = argparse.ArgumentParser()
    parser.add_argument(
    "-r", "--random",
    action="store_true",
    help="Publish random events in a loop (sometimes repeats ids)",
    )
    parser.add_argument(
    "--inference",
    action="store_true",
    help="Publish inference (quote) requests in a loop",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("-p", "--publish", action="store_true",
                      help="Only publish events")
    mode.add_argument("-c", "--consume", action="store_true",
                      help="Only consume events")
    args = parser.parse_args()

    if args.inference:
        try:
            publish_inference_events()
        except KeyboardInterrupt:
            print("\n✓ Stopped producing inference events")
        return


    if not args.consume:
        publish_events()
    if args.random:
        try:
            publish_random_events()
        except KeyboardInterrupt:
            print("\n✓ Stopped producing")
            return
    
    if not args.publish:
        consume_events(limit=None if args.consume else len(EVENTS))


if __name__ == "__main__":
    main()