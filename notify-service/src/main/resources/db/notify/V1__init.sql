-- Offline notifications spec §5.2: one push token per device; a token belongs to one device only (invariant 3).
CREATE TABLE push_tokens (
  tenant_id    text NOT NULL,
  principal_id text NOT NULL,
  device_id    text NOT NULL,
  platform     text NOT NULL,          -- apns | fcm
  environment  text NOT NULL,          -- production | sandbox (fcm: production)
  token        text NOT NULL,
  updated_at   timestamptz NOT NULL,
  PRIMARY KEY (tenant_id, principal_id, device_id)
);
CREATE UNIQUE INDEX push_tokens_token ON push_tokens (platform, token);
-- revocations name a principal and device without a tenant (spec §5.3)
CREATE INDEX push_tokens_principal ON push_tokens (principal_id, device_id);
CREATE INDEX push_tokens_updated ON push_tokens (updated_at);
