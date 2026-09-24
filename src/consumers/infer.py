import json
from decimal import Decimal
from confluent_kafka import Message
from sqlalchemy.dialects.postgresql import insert
from src.models.event import InferenceResult    
from src.models.utils import get_session
from src.clock import event_time
# from src.config import PRICE_PER_KM

# Price per km
PRICE_PER_KM = Decimal("2.5")


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