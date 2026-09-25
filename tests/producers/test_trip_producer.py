from decimal import Decimal

from src.producers.persist import random_event


def test_random_event_is_a_persist_trip():
    payload = random_event("event-7")

    assert payload["type"] == "persist"
    assert payload["vehicle-id"] == "vehicle-7"
    assert payload["driver-id"] == "driver-7"
    assert payload["user-id"] == "user-7"
    assert "produced_at" in payload
    assert payload["distance"].endswith(" km")
    assert payload["amount"].startswith("$")

    km = Decimal(payload["distance"].replace(" km", ""))
    amount = Decimal(payload["amount"].replace("$", ""))
    assert amount == (km * Decimal("2.5")).quantize(Decimal("0.01"))