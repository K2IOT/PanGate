# auth-service

Auth & Token service (spec: [docs/superpowers/specs/2026-11-05-auth-service-design.md](../docs/superpowers/specs/2026-11-05-auth-service-design.md)).
It issues API tokens to tenant backends (OAuth2 client credentials), mints gateway connect tokens, publishes the
JWKS with automatic key rotation, and sends revocations to `gw.control` through a transactional outbox.

## Run

| Mode | How | Notes |
|---|---|---|
| local | `make infra-up`, `make bootstrap`, then `make auth-local` (or run `AuthApplication` from the IDE with profile `local`) from the repo root | PostgreSQL `localhost:5432`, Kafka `localhost:9092`; public `:8090`, management `:8082` (the gateway uses `:8081` locally); KEK and seed secrets created in `.dev/auth/` |
| dev | `make images up` | Container `auth` in compose; management `:8081` inside the container; seed in the `dev-secrets` volume at `/dev-secrets/auth/` |
| prod | Image `pangate/auth-service` (profile `prod` by default) | Database password `AUTH_DB_PASSWORD`; KEK `auth.kek.<id>` from the secret source; seed off |

`make auth-local-check` starts the jar, waits for `/ready`, mints a connect token through it with devtools and stops.

## Endpoints

Public, `:8090`:

| Endpoint | Who | Result |
|---|---|---|
| `POST /oauth2/token` | Tenant backend: HTTP Basic or form `client_id`/`client_secret`, `grant_type=client_credentials`, optional `scope` | `{access_token, token_type, expires_in: 900, scope}`; OAuth2 errors |
| `POST /v1/tokens` | Bearer API token with `tokens:mint`: `{principal, device, topics?, ttlSec?}` | `{token, expiresAt}` — a connect token for the gateway (`aud=pangate-gw`) |
| `POST /v1/revocations` | Bearer API token with `sessions:revoke`: `{kind: principal\|device\|jti, ...}` | `202 {id, controlKey, expiresAt}`; reaches `gw.control` |
| `GET /.well-known/jwks.json` | Gateway, Push API | Pending, active and retired public keys; served from memory |

Management, `:8081` (`:8082` in profile `local`): `/ready`, `/live`, `/actuator/prometheus`, and the Admin API
`/admin/v1/**` (Bearer API token with scope `admin`, only for clients of tenant `pangate`):

| Method | Path | Effect |
|---|---|---|
| `POST` | `/tenants` `{id}` | Create tenant |
| `GET` | `/tenants`, `/tenants/{t}` | List, get |
| `POST` | `/tenants/{t}:disable`, `/tenants/{t}:enable` | Disabled: no API tokens, no connect tokens |
| `POST` | `/tenants/{t}/clients` `{name, scopes[]}` | Create client `<t>-<name>`; the secret is returned once |
| `GET` | `/tenants/{t}/clients` | List (never secrets or hashes) |
| `POST` | `/clients/{id}:rotate-secret`, `/clients/{id}:finish-rotation` | Second secret valid until finished |
| `POST` | `/clients/{id}:disable` | Disable and publish `revoke:c:<id>` |
| `POST` | `/keys:rotate-now` | Publish a new pending key now |

Errors other than OAuth2 are `application/problem+json` with a stable `code`.

## Seed (local and dev)

On start Auth creates tenant `pangate` with client `pangate-admin` (`admin`), and tenant `t1` with client `t1-backend`
(`tokens:mint sessions:revoke push:send push:broadcast`). The secrets are written once to `<seed-dir>/clients.json`
(mode 600). If the file already exists, its secrets win. devtools reads the file:

```bash
java -jar gateway-devtools/target/gateway-devtools-0.1.0-exec.jar token --auth-url http://localhost:8090 \
  --client-file .dev/auth/clients.json --principal t1:me --device laptop
```

## Behaviour under failure

- **PostgreSQL down:**
  - JWKS stays 200.
  - Token endpoints answer `503` with `Retry-After: 5`.
  - `/ready` goes 503 and `/live` stays 200.
- **Kafka down:** revocations still answer `202` and the outbox drains in order later. Readiness does not depend on Kafka.
- **Missing or short KEK:** startup fails with a message naming the secret and its location.

## Metrics

- `auth.tokens.issued{type,result}`
- `auth.token.latency{type}`
- `auth.revocations{kind}`
- `auth.keys{state}`
- `auth.keys.undecryptable`
- `auth.outbox.pending`
- `auth.outbox.oldest_age_seconds`
- `auth.ratelimited{endpoint}`
- `auth.admin.actions{action,result}`

## Tests

```bash
./mvnw -f auth-service verify
```

Unit tests need no Spring. The ITs use Testcontainers (PostgreSQL 16.4, Kafka 3.9.1). `GatewayContractTest` checks
tokens against the gateway's real `JwtVerifier` from the `verifier` jar, so build `gateway-service` first:
`make bootstrap`.
