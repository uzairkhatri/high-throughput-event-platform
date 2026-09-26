from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator


class EventRequest(BaseModel):
    tenant_id: str = Field(min_length=2, max_length=64)
    event_type: str = Field(pattern=r"^[a-z][a-z0-9_.-]+$")
    aggregate_id: str = Field(min_length=1, max_length=128)
    idempotency_key: str = Field(min_length=8, max_length=128)
    payload: dict[str, Any]

    @field_validator("payload")
    @classmethod
    def reject_empty_payload(cls, value: dict[str, Any]) -> dict[str, Any]:
        if not value:
            raise ValueError("payload must not be empty")
        return value


class EventEnvelope(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    trace_id: str
    tenant_id: str
    event_type: str
    aggregate_id: str
    idempotency_key: str
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    schema_version: int = 1
    payload: dict[str, Any]

class AcceptedEvent(BaseModel):
    status: str
    event_id: UUID
    partition_key: str
    duplicate: bool = False


class DeadLetterRecord(BaseModel):
    envelope: EventEnvelope
    error_type: str
    error_message: str
    attempts: int
    failed_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

