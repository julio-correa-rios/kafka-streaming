from confluent_kafka import Message

from src.event_consumer import EventConsumer
from src.event_producer import EventProducer

EVENTS = [("first", "a"), ("second", "b")]


def test_event_consumer(topic):
    producer = EventProducer()
    for key, value in EVENTS:
        producer.publish(key, value)

    received = []
    consumer = EventConsumer()
    count = consumer.consume(received.append, limit=len(EVENTS))
    consumer.close()

    assert count == len(EVENTS)
    assert [m.key().decode() for m in received] == ["first", "second"]


def test_event_consumer_stops_on_timeout(topic):
    consumer = EventConsumer()
    count = consumer.consume(fail_on_message, limit=1, timeout=5.0)
    consumer.close()

    assert count == 0


def fail_on_message(message: Message) -> None:
    raise AssertionError(f"Unexpected message: {message.key()}")