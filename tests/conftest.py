import os

import pytest
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

from src.models.utils import create_tables

load_dotenv()
load_dotenv(".env.test", override=True)


def delete_topic(admin: AdminClient, name: str) -> None:
    for future in admin.delete_topics([name]).values():
        try:
            future.result()
        except KafkaException as error:
            if error.args[0].code() != KafkaError.UNKNOWN_TOPIC_OR_PART:
                raise


@pytest.fixture
def topic() -> str:
    admin = AdminClient(
        {"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS")}
    )
    name = os.getenv("KAFKA_TOPIC")

    delete_topic(admin, name)
    for future in admin.create_topics([NewTopic(name, 1, 1)]).values():
        future.result()

    yield name

    delete_topic(admin, name)


@pytest.fixture
def db():
    create_tables()
    yield