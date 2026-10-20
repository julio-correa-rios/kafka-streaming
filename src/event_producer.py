import os

from confluent_kafka import Producer
from dotenv import load_dotenv


class EventProducer:
    _topic: str = None
    _producer: Producer = None

    def __init__(self, topic: str | None = None) -> None:
        load_dotenv()
        self._topic = topic or os.getenv("KAFKA_TOPIC")
        if not self._topic:
            raise ValueError("KAFKA_TOPIC is not set")
        self._producer = Producer(
            {"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS")}
        )

    def publish(self, key: str, value: str) -> None:
        self._producer.produce(self._topic, key=key, value=value)
        self._producer.flush()