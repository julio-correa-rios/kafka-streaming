import json

from sqlalchemy.exc import OperationalError

from src.consumers import router
from src.consumers.router import handle_event
from src.event_consumer import EventConsumer
from src.event_producer import EventProducer


def read_one(topic: str, monkeypatch):
    monkeypatch.setenv("KAFKA_TOPIC", topic)
    consumer = EventConsumer()
    received = []
    consumer.consume(received.append, limit=1)
    consumer.close()
    return received[0]


def test_unknown_type_goes_to_dlq(topic, dlq_topic, monkeypatch):
    EventProducer().publish("poison-test-1", json.dumps({"type": "refund"}))

    consumer = EventConsumer()
    consumer.consume(handle_event, limit=1)
    consumer.close()

    dead = read_one(dlq_topic, monkeypatch)
    body = json.loads(dead.value().decode())
    assert dead.key().decode() == "poison-test-1"
    assert body["reason"] == "unknown type: refund"
    assert json.loads(body["original"]) == {"type": "refund"}


def test_bad_json_goes_to_dlq(topic, dlq_topic, monkeypatch):
    EventProducer().publish("bad-test-1", "not json")

    consumer = EventConsumer()
    consumer.consume(handle_event, limit=1)
    consumer.close()

    dead = read_one(dlq_topic, monkeypatch)
    assert json.loads(dead.value().decode())["reason"] == "invalid json"


def db_down(*args):
    raise OperationalError("INSERT", {}, Exception("connection refused"))


def test_db_error_requeues_with_one_less_retry(topic, monkeypatch):
    monkeypatch.setenv("RETRY_DELAY_SECONDS", "0")
    monkeypatch.setattr(router, "persist_event", db_down)
    EventProducer().publish("retry-test-1", json.dumps({"type": "persist", "retry": 2}))

    consumer = EventConsumer()
    consumer.consume(handle_event, limit=1)
    received = []
    consumer.consume(received.append, limit=1)
    consumer.close()

    assert received[0].key().decode() == "retry-test-1"
    assert json.loads(received[0].value().decode())["retry"] == 1


def test_db_error_with_no_retries_goes_to_dlq(topic, dlq_topic, monkeypatch):
    monkeypatch.setattr(router, "persist_event", db_down)
    EventProducer().publish("retry-test-2", json.dumps({"type": "persist", "retry": 0}))

    consumer = EventConsumer()
    consumer.consume(handle_event, limit=1)
    consumer.close()

    dead = read_one(dlq_topic, monkeypatch)
    assert dead.key().decode() == "retry-test-2"
    assert json.loads(dead.value().decode())["reason"].startswith("retries exhausted")