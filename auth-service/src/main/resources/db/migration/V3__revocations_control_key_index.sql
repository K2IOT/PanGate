-- Revocation upserts and per-request jti lookups go by control_key (not unique: see JdbcRevocationStore.upsert).
CREATE INDEX revocations_control_key ON revocations (control_key);
