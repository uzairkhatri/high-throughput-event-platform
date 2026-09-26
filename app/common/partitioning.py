import hashlib


def partition_key(tenant_id: str, aggregate_id: str) -> str:
    """Keep all events for the same tenant aggregate ordered on one stream partition."""
    return f"{tenant_id}:{aggregate_id}"


def partition_for_key(key: str, partition_count: int) -> int:
    if partition_count < 1:
        raise ValueError("partition_count must be positive")
    digest = hashlib.sha256(key.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % partition_count

