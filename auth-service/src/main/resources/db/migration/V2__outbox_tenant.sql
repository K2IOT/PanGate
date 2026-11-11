-- Kafka header tenant-id for outbox records (spec §5): the record key alone does not carry the tenant (revoke:j:).
ALTER TABLE outbox ADD COLUMN tenant_id text NOT NULL DEFAULT '';
