import json
import os
from typing import Optional

from confluent_kafka import Message

from src.clock import utc_now
from src.consumers.infer import infer_event
from src.consumers.persist import persist_event
from src.event_consumer import EventConsumer
from src.event_producer import EventProducer
from src.models.utils import create_tables

_producers: dict[str, EventProducer] = {}


def publish_to(topic: str, key: str, value: str) -> None:
    if topic not in _producers:
        _producers[topic] = EventProducer(topic)
    _producers[topic].publish(key, value)


def dlq_topic() -> str:
    # Never fall back to KAFKA_TOPIC: poison events would loop forever.
    return os.getenv("KAFKA_DLQ_TOPIC") or f"{os.getenv('KAFKA_TOPIC')}-dlq"


def send_to_dlq(message: Message, reason: str) -> None:
    key = message.key().decode()
    value = {
        "reason": reason,
        "failed_at": utc_now(),
        "original": message.value().decode(),
    }
    publish_to(dlq_topic(), key, json.dumps(value))
    print(f"☠ DLQ: {key} ({reason})")


def handle_event(message: Message) -> None:
    event_id = message.key().decode()
    raw = message.value().decode()
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        send_to_dlq(message, "invalid json")
        return

    if not isinstance(payload, dict):
        send_to_dlq(message, "payload is not an object")
        return

    event_type = payload.get("type", "persist")
    if event_type == "persist":
        persist_event(message)
    elif event_type == "inference":
        infer_event(message, payload)
    else:
        send_to_dlq(message, f"unknown type: {event_type}")


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