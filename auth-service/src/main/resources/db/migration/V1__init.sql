-- auth schema V1 (spec §2). Flyway runs it with search_path = auth.
CREATE TABLE tenants (
  id          text PRIMARY KEY CHECK (id ~ '^[a-z0-9][a-z0-9_-]{0,63}$' AND id <> '__warmup'),
  status      text NOT NULL CHECK (status IN ('active','disabled')),
  created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE clients (
  id                text PRIMARY KEY,
  tenant_id         text NOT NULL REFERENCES tenants(id),
  secret_hash       text NOT NULL,          -- Argon2id, encoded (tham số nằm trong chuỗi)
  secret_hash_next  text,                   -- secret mới trong lúc xoay; cả hai đều hợp lệ
  scopes            text[] NOT NULL,
  status            text NOT NULL CHECK (status IN ('active','disabled')),
  created_at        timestamptz NOT NULL DEFAULT now(),
  secret_rotated_at timestamptz
);

CREATE TABLE signing_keys (
  kid              text PRIMARY KEY,         -- base64url(SHA-256 thumbprint) theo RFC 7638
  private_key_enc  bytea NOT NULL,           -- AES-256-GCM(KEK): 12 B nonce || ciphertext || tag
  kek_id           text NOT NULL,            -- tên KEK đã dùng, để xoay KEK sau này
  public_jwk       jsonb NOT NULL,
  state            text NOT NULL CHECK (state IN ('pending','active','retired')),
  created_at       timestamptz NOT NULL DEFAULT now(),
  activate_at      timestamptz NOT NULL,
  retire_at        timestamptz,              -- lúc chuyển sang retired
  remove_at        timestamptz               -- retire_at + 7 ngày: gỡ khỏi JWKS và xoá
);
CREATE UNIQUE INDEX one_active_key ON signing_keys ((state)) WHERE state = 'active';

CREATE TABLE revocations (
  id          bigserial PRIMARY KEY,
  tenant_id   text NOT NULL,
  kind        text NOT NULL CHECK (kind IN ('principal','device','jti','client')),
  control_key text NOT NULL,                 -- khoá trên gw.control, ví dụ revoke:p:t1:u1
  expires_at  timestamptz NOT NULL,
  created_by  text NOT NULL,                 -- client_id của người gọi
  created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX revocations_expiry ON revocations (expires_at);

CREATE TABLE outbox (
  id          bigserial PRIMARY KEY,
  topic       text NOT NULL,
  record_key  text NOT NULL,
  payload     bytea,                         -- NULL = tombstone
  created_at  timestamptz NOT NULL DEFAULT now(),
  sent_at     timestamptz
);
CREATE INDEX outbox_unsent ON outbox (id) WHERE sent_at IS NULL;
