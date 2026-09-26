from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    service_name: str = "event-ingestion-api"
    environment: str = "local"
    runtime_mode: str = "memory"
    kafka_bootstrap_servers: str = "localhost:9092"
    events_topic: str = "events.orders.v1"
    dlq_topic: str = "events.orders.dlq.v1"
    consumer_group: str = "order-projection-v1"
    event_partitions: int = 12
    redis_url: str = "redis://localhost:6379/0"
    postgres_dsn: str = "postgresql://events:events@localhost:5432/events"
    idempotency_ttl_seconds: int = 86_400
    max_payload_bytes: int = 262_144
    worker_max_attempts: int = 5
    otlp_endpoint: str | None = None

    model_config = SettingsConfigDict(env_prefix="EVENT_PLATFORM_", env_file=".env")


settings = Settings()

