import json
from confluent_kafka import Message
from src.consumers.persist import persist_event
from src.consumers.infer import infer_event
from src.models.utils import create_tables
from src.event_consumer import EventConsumer
from typing import Optional



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