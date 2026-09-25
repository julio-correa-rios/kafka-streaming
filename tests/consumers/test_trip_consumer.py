import json

from src.consumers.persist import persist_event
from src.event_consumer import EventConsumer
from src.event_producer import EventProducer
from src.models.event import Event
from src.models.utils import get_session


def test_persist_event_writes_a_trip(topic, db):
    payload = {
        "type": "persist",
        "produced_at": "2026-09-25T00:00:00+00:00",
        "vehicle-id": "vehicle-99",
        "driver-id": "driver-99",
        "user-id": "user-99",
        "duration": "10 minutes",
        "distance": "4 km",
        "amount": "$10.00",
    }
    producer = EventProducer()
    producer.publish("trip-test-1", json.dumps(payload))

    consumer = EventConsumer()
    consumer.consume(persist_event, limit=1)
    consumer.close()

    with get_session() as session:
        row = session.get(Event, "trip-test-1")

    assert row is not None
    assert row.user_id == "user-99"
    assert float(row.amount) == 10.00


def test_persist_event_skips_bad_json(topic, db):
    producer = EventProducer()
    producer.publish("trip-test-bad", "not-json")

    consumer = EventConsumer()
    consumer.consume(persist_event, limit=1)
    consumer.close()

    with get_session() as session:
        row = session.get(Event, "trip-test-bad")

    assert row is None