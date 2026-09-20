# kafka

Minimal viable example for Apache Kafka with Docker and Python. A producer publishes trip events, a consumer reads them from a topic and writes them to Postgres.

## What you get

| Service | What it's for | Where |
| --- | --- | --- |
| Kafka | Broker (KRaft, 3 partitions by default) | `localhost:9092` |
| Kafka UI | Look at topics and messages | http://localhost:8080 |
| Postgres | Where events land | `localhost:5432` |
| Metabase | Query the table without SQL if you want | http://localhost:3000 |
| Producer / consumer | The actual app | Docker services, or `main.py` |

Each event is a small trip record: vehicle, driver, user, duration, distance, amount. Topic name is `user-events`.

The consumer inserts into an `events` table. If the same id shows up twice, it is ignored (`ON CONFLICT DO NOTHING`). That matters because the random producer sometimes replays ids on purpose.

## Run everything

```bash
docker compose up --build
```

This starts Kafka, Postgres, Metabase, Kafka UI, a producer and a consumer.

The producer sends 3 static events, then keeps going with random ones (every 2s by default). Roughly 30% of the time it reuses an id it already sent.

Stop with Ctrl+C.

## Run the app yourself

If you only want Kafka and Postgres in Docker, and the Python process on the host:

```bash
docker compose up kafka postgres
```

Python 3.12+. Then either:

```bash
uv sync
python main.py
```

or:

```bash
pip install -r requirements.txt
python main.py
```

With no flags, that publishes the 3 static events and consumes them.

```bash
python main.py -p        # publish the 3 static events and exit
python main.py -c        # consume forever, persist to Postgres
python main.py -p -r     # publish the 3, then loop random events
```

`-r` does not return until you Ctrl+C. That's the same mode the Docker producer uses.

## Config

Values live in `.env`. The ones you actually care about:

```
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
KAFKA_TOPIC=user-events
KAFKA_GROUP_ID=user-events-consumer

POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=admin
POSTGRES_PASSWORD=admin123
POSTGRES_DB=testdb

PRODUCER_INTERVAL=2.0
PRODUCER_REPLAY_CHANCE=0.3
```

Inside Compose, the producer/consumer override the Kafka and Postgres hosts so they talk to the containers (`kafka:9093`, `postgres`), not to localhost.

## Layout

```
main.py                 # CLI, random generator, persist logic
src/event_producer.py   # thin wrapper around confluent-kafka Producer
src/event_consumer.py   # poll loop, hands each message to a handler
src/models/event.py     # SQLAlchemy model
src/models/utils.py     # engine / session / create_tables
docker-compose.yml
Dockerfile
```

## Metabase

Open http://localhost:3000 after Compose is up. Point it at Postgres (`postgres` as host from inside the network, or `localhost` from your machine), database `testdb`. The table is `events`.
