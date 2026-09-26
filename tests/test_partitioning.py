from app.common.partitioning import partition_for_key, partition_key


def test_partition_key_keeps_tenant_aggregate_ordering_boundary() -> None:
    assert partition_key("tenant-a", "order-123") == "tenant-a:order-123"


def test_partition_assignment_is_stable() -> None:
    key = partition_key("tenant-a", "order-123")
    assert partition_for_key(key, 12) == partition_for_key(key, 12)
    assert 0 <= partition_for_key(key, 12) < 12

