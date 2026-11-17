# account-service

Accounts of the chat app (sub-project CH2). Spec: [docs/superpowers/specs/2026-11-16-account-service-design.md](../docs/superpowers/specs/2026-11-16-account-service-design.md); ADR 0033–0036.

It is the "backend of tenant `chat`": it keeps users, passwords and device sessions in PostgreSQL (schema `account`) and asks auth-service for connect tokens of `chat:<userId>`.

## Ports

| Port | Content |
|---|---|
| `8080` | Public API `/v1/**` (devenv host port 8088) |
| `8081` | Admin API `/admin/v1/**`, `/ready`, `/live`, `/actuator/prometheus` (devenv host port 8089; never behind the ingress) |

## Public API

| Endpoint | Auth | |
|---|---|---|
| `POST /v1/accounts` | — | `{username, password, email?, phone?, displayName?, deviceName, platform}` → `201 {userId, deviceId, refreshToken}` |
| `POST /v1/sessions` | — | `{login, password, deviceName, platform, newPassword?}`; `login` = email, `+phone` or username |
| `POST /v1/sessions/token` | `Bearer rt_…` | `{connectToken, expiresAt}` (10 minutes) |
| `GET /v1/sessions`, `DELETE /v1/sessions/{deviceId}`, `DELETE /v1/sessions?others=true` | connect token | devices |
| `GET /v1/me`, `PATCH /v1/me`, `PUT /v1/me/username`, `PUT /v1/me/email`, `PUT /v1/me/phone`, `POST /v1/me/password`, `DELETE /v1/me` | connect token | my account |
| `GET /v1/users/{id}`, `GET /v1/users?username=`, `POST /v1/users:batch` | connect token | public profiles |

Errors are `application/problem+json` with a stable `code` (`invalid_*`, `weak_password`, `*_taken`, `invalid_credentials`, `account_locked`, `password_change_required`, `too_many_attempts`, `rate_limited`, `invalid_session`, `invalid_token`, `not_found`, `unavailable`).

## Admin API

Bearer API token of auth-service with scope `accounts:admin` and tenant `chat` (devenv client `chat-admin`). `GET /admin/v1/accounts/{id}`, `GET /admin/v1/accounts?username=|email=|phone=`, `POST …/{id}:lock`, `:unlock`, `:reset-password` (returns `temporaryPassword` once), `DELETE …/{id}`; every change needs `{reason}` and is logged in `admin_actions`.

## Limits (spec §7)

Wrong logins: 10 per login + IP in 15 min → that pair blocked 15 min; 50 per IP → each further try from it waits 2 s (shared carrier-grade NAT addresses are not blocked); 1000 per IP → IP blocked 15 min; 100 per login from every IP → each further try waits 2 s. Registration from one IP: 20 attempts per hour (taken and invalid ones too, bounding what `*_taken` reveals) and 5 accounts created per hour (a slot reserved before the insert and given back if it fails); token exchange 60 per session per hour; lookups 600 per user per minute; admin 20/s per client. Counters live in Redis and fall back to per-pod memory. Password hashing (Argon2id m=19 MiB, t=2) runs at most 2 × cores at a time per pod; more → `503`.

## Configuration (`account.*`)

`account.auth.base-url`, `issuer`, `jwks-uri`, `client-id` (`chat-backend`), the secret from `client-file` (`clients.json` of auth's seed: local, dev) or the `SecretSource` name `account/chat-backend-secret`; `account.kafka.bootstrap`, `users-topic` (`account.users`), `control-topic` (`gw.control`); `account.redis.uri`; `account.session-idle-ttl` (180 d); `account.limits.*`; `account.outbox.*`.

## Events

The outbox sends `AccountState` (current status, never a tombstone) to the compacted `account.users`, and revocations to auth-service — `device` for logouts, password changes and locks, `principal` only on deletion (ADR 0036). The two relays are independent.

## Run

`make images up` starts it in devenv; `make smoke` runs the `ACCOUNT …` steps. `gateway-devtools account register|login|token --url http://localhost:8088` for manual calls.
