# Security Considerations

## Controls Included

- explicit payload size limit at ingestion;
- structured event validation;
- trace IDs without logging secrets;
- Kubernetes secrets placeholders for Kafka connection values;
- dependency and test workflow hooks through GitHub Actions.

## Controls Required Before Production

- authentication and authorization at the API gateway;
- tenant isolation rules for every command and query path;
- Kafka ACLs and TLS/mTLS;
- Redis and PostgreSQL TLS plus credential rotation;
- schema registry compatibility checks;
- PII classification, field-level encryption where required, and retention rules;
- signed container images and vulnerability scanning;
- incident runbooks for DLQ replay, stream lag, and partition hotspots.

