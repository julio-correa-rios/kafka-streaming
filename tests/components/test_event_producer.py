from src.event_consumer import EventConsumer
from src.event_producer import EventProducer

def test_event_producer(topic):
    producer = EventProducer()
    producer.publish("test-event", "hello")
    
    received = []
    consumer = EventConsumer()
    consumer.consume(received.append, limit=1)
    consumer.close()

    assert len(received) == 1
    assert received[0].key().decode() == "test-event"
    assert received[0].value().decode() == "hello"