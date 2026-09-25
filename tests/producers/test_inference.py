from src.producers.inference import random_inference

def test_random_inference_is_a_quote_request():
    payload = random_inference("infer-3")

    assert payload["type"] == "inference"
    assert payload["user-id"] == "user-3"
    assert "produced_at" in payload
    assert "distance" in payload
    assert "duration" in payload
    assert "amount" not in payload
    assert "vehicle-id" not in payload