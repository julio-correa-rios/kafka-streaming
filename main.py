import argparse
import json
from decimal import Decimal
from typing import Optional

from confluent_kafka import Message
from dotenv import load_dotenv
from sqlalchemy.dialects.postgresql import insert

from src.event_consumer import EventConsumer
from src.event_producer import EventProducer
from src.models.event import Event
from src.models.utils import create_tables, get_session

load_dotenv()

EVENTS = {
    "event-1": {
        "vehicle-id": "vehicle-1",
        "driver-id": "driver-1",
        "user-id": "user-1",
        "duration": "15 minutes",
        "distance": "10 km",
        "amount": "$25.00"
    },
    "event-2": {
        "vehicle-id": "vehicle-2",
        "driver-id": "driver-2",
        "user-id": "user-2",
        "duration": "10 minutes",
        "distance": "4.5 km",
        "amount": "$12.00"
    },
    "event-3": {
        "vehicle-id": "vehicle-3",
        "driver-id": "driver-3",
        "user-id": "user-3",
        "duration": "18 minutes",
        "distance": "15 km",
        "amount": "$30.00"
    }
}

#     # - ID del vehículo.
#     # - ID del conductor.
#     # - ID del usuario.
#     # - Duración.
#     # - KM recorridos.
#     # - Importe.
#     # Vehículo
#     [("vehicle-id", "vehicle-1"),
#     # Conductor
#     ("driver-id", "driver-1"),
#     # Usuario
#     ("user-id", "user-1"),
#     # Duración
#     ("duration", "15 minutes"),
#     # KM recorridos
#     ("distance", "10 km"),
#     # Importe
#     ("amount", "$25.00"),
#     ],
#     [("vehicle-id", "vehicle-2"),
#     # Conductor
#     ("driver-id", "driver-2"),
#     # Usuario
#     ("user-id", "user-2"),
#     # Duración
#     ("duration", "10 minutes"),
#     # KM recorridos
#     ("distance", "4.5 km"),
#     # Importe
#     ("amount", "$12.00"),]
# ]


def publish_events() -> None:
    producer = EventProducer()

    print("--- Producing events ---")
    
    for event_key, event_value in EVENTS.items():
        producer.publish(event_key, json.dumps(event_value))
        print(f"✓ Produced: {event_key} -> {event_value}")


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
        )
        .on_conflict_do_nothing(index_elements=["id"])
    )
    with get_session() as session:
        session.execute(stmt)
        session.commit()
    print(f"✓ Persisted: {event_id} -> {payload}")


def consume_events(limit: Optional[int] = None) -> None:
    create_tables()
    consumer = EventConsumer()

    print("\n--- Consuming events ---")
    try:
        consumer.consume(persist_event, limit=limit)
    except KeyboardInterrupt:
        print("\n✓ Stopped consuming")
    finally:
        consumer.close()


def print_event(message: Message) -> None:
    key = message.key().decode()
    value = message.value().decode()
    print(f"✓ Consumed: {key} -> {value}")


def main() -> None:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("-p", "--publish", action="store_true",
                      help="Only publish events")
    mode.add_argument("-c", "--consume", action="store_true",
                      help="Only consume events")
    args = parser.parse_args()

    if not args.consume:
        publish_events()

    if not args.publish:
        consume_events(limit=None if args.consume else len(EVENTS))


if __name__ == "__main__":
    main()