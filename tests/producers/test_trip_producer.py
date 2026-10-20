from decimal import Decimal

from src.producers.persist import CITY_WEIGHTS, random_event


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


def test_random_event_uses_city_population_weights(monkeypatch):
    selected = {}

    def choose(cities, weights, k):
        selected["cities"] = cities
        selected["weights"] = weights
        selected["k"] = k
        return ["Madrid"]

    monkeypatch.setattr("src.producers.persist.random.choices", choose)

    payload = random_event("event-7")

    assert payload["city"] == "Madrid"
    assert selected["cities"] == list(CITY_WEIGHTS)
    assert selected["weights"] == list(CITY_WEIGHTS.values())
    assert selected["k"] == 1
    assert CITY_WEIGHTS["Madrid"] > CITY_WEIGHTS["Bilbao"]