from fastapi.testclient import TestClient

from app.api.main import build_app
from app.common.publisher import InMemoryPublisher


def test_ingestion_accepts_event_and_publishes_with_partition_key() -> None:
    publisher = InMemoryPublisher()
    client = TestClient(build_app(publisher=publisher))

    response = client.post(
        "/v1/events",
        json={
            "tenant_id": "tenant-a",
            "event_type": "order.created",
            "aggregate_id": "order-123",
            "idempotency_key": "request-12345",
            "payload": {"amount": 42},
        },
    )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "accepted"
    assert body["partition_key"] == "tenant-a:order-123"
    assert len(publisher.published) == 1


def test_duplicate_idempotency_key_returns_original_event_without_republishing() -> None:
    publisher = InMemoryPublisher()
    client = TestClient(build_app(publisher=publisher))
    payload = {
        "tenant_id": "tenant-a",
        "event_type": "order.created",
        "aggregate_id": "order-123",
        "idempotency_key": "request-duplicate",
        "payload": {"amount": 42},
    }

    first = client.post("/v1/events", json=payload)
    second = client.post("/v1/events", json=payload)

    assert first.status_code == 202
    assert second.status_code == 202
    assert second.json()["duplicate"] is True
    assert second.json()["event_id"] == first.json()["event_id"]
    assert len(publisher.published) == 1

