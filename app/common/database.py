from decimal import Decimal
from typing import Protocol

from app.common.models import EventEnvelope
from app.workers.order_projection import NonRecoverableWorkerError, RecoverableWorkerError


class OrderRepository(Protocol):
    async def apply(self, envelope: EventEnvelope) -> str:
        """Apply an event transactionally and return its processing status."""


class PostgresOrderRepository:
    def __init__(self, pool: object, consumer_group: str) -> None:
        self._pool = pool
        self._consumer_group = consumer_group

    async def apply(self, envelope: EventEnvelope) -> str:
        async with self._pool.acquire() as connection, connection.transaction():
            inserted = await connection.fetchval(
                """
                INSERT INTO processed_events (event_id, consumer_group)
                VALUES ($1, $2)
                ON CONFLICT (event_id) DO NOTHING
                RETURNING event_id
                """,
                envelope.event_id,
                self._consumer_group,
            )
            if inserted is None:
                return "already_processed"

            if envelope.event_type == "order.created":
                amount = envelope.payload.get("amount")
                if isinstance(amount, bool) or not isinstance(amount, int | float) or amount <= 0:
                    raise NonRecoverableWorkerError(
                        "order.created requires a positive numeric amount"
                    )
                await connection.execute(
                    """
                    INSERT INTO order_projection (tenant_id, order_id, amount, status)
                    VALUES ($1, $2, $3, 'created')
                    ON CONFLICT (tenant_id, order_id) DO UPDATE
                    SET amount = EXCLUDED.amount,
                        status = EXCLUDED.status,
                        updated_at = now()
                    """,
                    envelope.tenant_id,
                    envelope.aggregate_id,
                    Decimal(str(amount)),
                )
                return "processed"

            if envelope.event_type == "order.cancelled":
                result = await connection.execute(
                    """
                    UPDATE order_projection
                    SET status = 'cancelled', updated_at = now()
                    WHERE tenant_id = $1 AND order_id = $2
                    """,
                    envelope.tenant_id,
                    envelope.aggregate_id,
                )
                if result == "UPDATE 0":
                    raise RecoverableWorkerError(
                        "order must exist before it can be cancelled"
                    )
                return "processed"

            raise NonRecoverableWorkerError(
                f"unsupported event_type={envelope.event_type}"
            )

