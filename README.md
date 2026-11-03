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
