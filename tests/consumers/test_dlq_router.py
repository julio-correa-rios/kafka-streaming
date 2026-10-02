import json

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