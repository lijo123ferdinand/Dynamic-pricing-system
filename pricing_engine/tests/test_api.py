import json
import pytest

from app.api import create_app

@pytest.fixture
def client():
    app = create_app()
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c

def test_health(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["status"] == "ok"

def test_price_feedback_validation(client):
    payload = {
        "vendor_id": "v1",
        "sku": "s1",
        "suggested_price": 100.0,
        "action": "custom_price",
        "timestamp": "2025-01-01T10:00:00Z",
    }
    resp = client.post("/price-feedback", json=payload)
    assert resp.status_code == 400
    data = resp.get_json()
    assert "custom_price required" in data["error"]
