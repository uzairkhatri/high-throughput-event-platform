from dataclasses import dataclass, field

from app.common.models import DeadLetterRecord, EventEnvelope
from app.common.retry import RetryPolicy


class RecoverableWorkerError(RuntimeError):
    """Raised when a message should be retried before dead-lettering."""


class NonRecoverableWorkerError(RuntimeError):
    """Raised when retrying would not make the message processable."""


@dataclass
class ProjectionState:
    processed_event_ids: set[str] = field(default_factory=set)
    orders: dict[str, dict] = field(default_factory=dict)
    dlq: list[DeadLetterRecord] = field(default_factory=list)


class OrderProjectionWorker:
    def __init__(
        self,
        state: ProjectionState | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        self.state = state or ProjectionState()
        self.retry_policy = retry_policy or RetryPolicy()

    async def handle(self, envelope: EventEnvelope) -> str:
        event_id = str(envelope.event_id)
        if event_id in self.state.processed_event_ids:
            return "already_processed"

        try:
            self._apply(envelope)
        except NonRecoverableWorkerError as exc:
            self.state.dlq.append(
                DeadLetterRecord(
                    envelope=envelope,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    attempts=1,
                )
            )
            return "dead_lettered"

        self.state.processed_event_ids.add(event_id)
        return "processed"

    def _apply(self, envelope: EventEnvelope) -> None:
        if envelope.event_type == "order.created":
            amount = envelope.payload.get("amount")
            if not isinstance(amount, int | float) or amount <= 0:
                raise NonRecoverableWorkerError("order.created requires a positive numeric amount")
            self.state.orders[envelope.aggregate_id] = {
                "tenant_id": envelope.tenant_id,
                "amount": amount,
                "status": "created",
            }
            return

        if envelope.event_type == "order.cancelled":
            order = self.state.orders.get(envelope.aggregate_id)
            if order is None:
                raise RecoverableWorkerError("order must exist before it can be cancelled")
            order["status"] = "cancelled"
            return

        raise NonRecoverableWorkerError(f"unsupported event_type={envelope.event_type}")

