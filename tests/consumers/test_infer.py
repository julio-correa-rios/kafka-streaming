
import json
from decimal import Decimal

from src.consumers.router import handle_event
from src.event_consumer import EventConsumer
from src.event_producer import EventProducer
from src.models.event import Event, InferenceResult
from src.models.utils import get_session


def test_infer_event_writes_recommended_price(topic, db):
    payload = {
        "type": "inference",
        "produced_at": "2026-09-25T00:00:00+00:00",
        "user-id": "user-5",
        "duration": "15 minutes",
        "distance": "10 km",
    }
    producer = EventProducer()
    producer.publish("infer-test-1", json.dumps(payload))

    consumer = EventConsumer()
    consumer.consume(handle_event, limit=1)
    consumer.close()

    with get_session() as session:
        row = session.get(InferenceResult, "infer-test-1")
        trip = session.get(Event, "infer-test-1")

    assert row is not None
    assert row.recommended_price == Decimal("25.00")  # 10 * 2.5
    assert trip is None  # must NOT land in events