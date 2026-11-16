-- account spec §3. Schema `account` (spring.flyway.schemas).
CREATE EXTENSION IF NOT EXISTS citext;

CREATE TABLE accounts (
  user_id              text PRIMARY KEY,               -- ULID, never handed out again
  username             citext UNIQUE,                  -- NULL once deleted
  display_name         text,                           -- NULL once deleted
  bio                  text,
  password_hash        text,                           -- Argon2id encoded; NULL once deleted
  must_change_password boolean NOT NULL DEFAULT false,
  status               text NOT NULL CHECK (status IN ('active','locked','deleted')),
  created_at           timestamptz NOT NULL DEFAULT now(),
  updated_at           timestamptz NOT NULL DEFAULT now(),
  password_changed_at  timestamptz NOT NULL DEFAULT now(),
  deleted_at           timestamptz
);

CREATE TABLE identifiers (
  kind       text NOT NULL CHECK (kind IN ('email','phone')),
  value      text NOT NULL,                            -- normalised (spec §4.1)
  user_id    text NOT NULL REFERENCES accounts(user_id),
  verified   boolean NOT NULL DEFAULT false,           -- always false in CH2; OTP turns it on later
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (kind, value),
  UNIQUE (user_id, kind)
);

CREATE TABLE sessions (
  device_id     text PRIMARY KEY,                      -- ULID
  user_id       text NOT NULL REFERENCES accounts(user_id),
  refresh_hash  bytea NOT NULL UNIQUE,                 -- SHA-256 of the refresh token
  device_name   text NOT NULL,
  platform      text NOT NULL CHECK (platform IN ('ios','android','web','desktop','other')),
  created_at    timestamptz NOT NULL DEFAULT now(),
  last_used_at  timestamptz NOT NULL DEFAULT now(),
  revoked_at    timestamptz
);
CREATE INDEX sessions_by_user ON sessions (user_id) WHERE revoked_at IS NULL;

CREATE TABLE admin_actions (
  id         bigserial PRIMARY KEY,
  actor      text NOT NULL,
  action     text NOT NULL,
  user_id    text NOT NULL,
  reason     text NOT NULL,
  at         timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX admin_actions_by_user ON admin_actions (user_id);

CREATE TABLE outbox (
  id          bigserial PRIMARY KEY,
  kind        text NOT NULL CHECK (kind IN ('event','revocation')),
  record_key  text NOT NULL,
  payload     bytea NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now(),
  sent_at     timestamptz
);
CREATE INDEX outbox_pending ON outbox (kind, id) WHERE sent_at IS NULL;
