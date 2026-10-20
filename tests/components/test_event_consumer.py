import signal

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


def test_sigint_before_loop_stops_without_polling(monkeypatch):
    consumer = EventConsumer.__new__(EventConsumer)

    class MustNotPoll:
        def poll(self, timeout):
            raise AssertionError("poll() should not run after SIGINT")

    consumer._consumer = MustNotPoll()
    current_handler = signal.default_int_handler

    def install_handler(sig, handler):
        nonlocal current_handler
        previous_handler = current_handler
        current_handler = handler
        if handler == consumer._stop:
            handler(sig, None)
        return previous_handler

    monkeypatch.setattr(signal, "signal", install_handler)

    count = consumer.consume(lambda message: None)

    assert count == 0


def test_sigint_inside_loop_finishes_current_message(monkeypatch):
    consumer = EventConsumer.__new__(EventConsumer)

    class FakeMessage:
        def error(self):
            return None

    message = FakeMessage()

    class OneMessage:
        def __init__(self):
            self.poll_count = 0

        def poll(self, timeout):
            self.poll_count += 1
            return message

    fake_consumer = OneMessage()
    consumer._consumer = fake_consumer
    current_handler = signal.default_int_handler

    def install_handler(sig, handler):
        nonlocal current_handler
        previous_handler = current_handler
        current_handler = handler
        return previous_handler

    monkeypatch.setattr(signal, "signal", install_handler)
    handled = []

    def handle(received):
        handled.append(received)
        current_handler(signal.SIGINT, None)

    count = consumer.consume(handle)

    assert handled == [message]
    assert count == 1
    assert fake_consumer.poll_count == 1