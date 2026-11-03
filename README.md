# PanGate

Connection gateway (Netty on Spring Boot) for 10M long-lived client connections.

## Requirements

- JDK 21
- Docker with Compose (only for the `dev` environment)

## Build

```bash
make bootstrap
```

Modules build in order: `platform-parent` → `platform-contracts` → `platform-spring-starter` → `gateway-devtools` → `gateway-service`.

- Design spec: [docs/superpowers/specs/2026-11-03-gateway-service-design.md](docs/superpowers/specs/2026-11-03-gateway-service-design.md)
- Dev environment: [platform-devenv/README.md](platform-devenv/README.md)

## Run

- **Local (IDE):** see [gateway-service/README.md](gateway-service/README.md) — profile `local`, secrets auto-generated in `~/.pangate-dev`.
- **Dev (Docker):** `make bootstrap images up smoke`, then `make down`.
- Checks: `make doctor`, `make local-check`, `make memory-check` (< 5 KB/session), `make load-check N=18000` (TODO: `N=50000` on a machine with `ulimit -n` ≥ 120000).
