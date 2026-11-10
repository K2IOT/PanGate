# PanGate

Connection gateway (Netty on Spring Boot) for 10M long-lived client connections.

## Requirements

- JDK 21
- Docker with Compose: Kafka, Redis and PostgreSQL for `local` (`make infra-up`), everything for `dev`, Testcontainers for
  `make verify`

## Build

```bash
make bootstrap
```

Modules build in order: `platform-parent` → `platform-contracts` → `platform-spring-starter` → `platform-contracts-testkit` → `gateway-devtools` → `gateway-service` → `push-router-service` → `push-api-service` → `auth-service`.

- Design spec: [docs/superpowers/specs/2026-11-03-gateway-service-design.md](docs/superpowers/specs/2026-11-03-gateway-service-design.md)
- Auth service: [auth-service/README.md](auth-service/README.md) (spec [docs/superpowers/specs/2026-11-05-auth-service-design.md](docs/superpowers/specs/2026-11-05-auth-service-design.md))
- Push API: [push-api-service/README.md](push-api-service/README.md) (spec [docs/superpowers/specs/2026-11-10-push-api-design.md](docs/superpowers/specs/2026-11-10-push-api-design.md))
- Push router: [push-router-service/README.md](push-router-service/README.md) (spec [docs/superpowers/specs/2026-11-10-push-router-design.md](docs/superpowers/specs/2026-11-10-push-router-design.md))
- Push-path contracts: [docs/superpowers/specs/2026-11-10-push-contracts-design.md](docs/superpowers/specs/2026-11-10-push-contracts-design.md) (testkit in `platform-contracts-testkit/`)
- Dev environment: [platform-devenv/README.md](platform-devenv/README.md)

## Run

- **Local (IDE):** `make infra-up` (Kafka `localhost:9092`, Redis `localhost:6379`, PostgreSQL `localhost:5432`), then see
  [gateway-service/README.md](gateway-service/README.md) — profile `local`, secrets auto-generated in `~/.pangate-dev`.
- **Dev (Docker):** `make bootstrap images up smoke`, then `make down`.
- Checks: `make doctor`, `make local-check`, `make auth-local-check`, `make memory-check` (< 5 KB/session), `make load-check N=18000` (TODO: `N=50000` on a machine with `ulimit -n` ≥ 120000).
