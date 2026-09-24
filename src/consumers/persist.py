import json
from decimal import Decimal
from confluent_kafka import Message
from src.clock import event_time
from sqlalchemy.dialects.postgresql import insert
from src.models.event import Event
from src.models.utils import get_session

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