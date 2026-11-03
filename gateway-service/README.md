# gateway-service

Netty data plane on Spring Boot (spec: `docs/superpowers/specs/2026-11-03-gateway-service-design.md`).

| Port | What |
|---|---|
| 8443 | WSS `/v1/ws`, subprotocol `pangate.v1` |
| 8883 | TCP + TLS, ALPN `pangate.v1`, 4-byte length prefix |
| 8081 | Management: `/ready`, `/live`, `/actuator/prometheus`, `POST /admin/drain` (loopback) |

## Run from the IDE (profile `local`)

1. `make bootstrap` once (installs contracts, starter and devtools into `~/.m2`).
2. **IntelliJ IDEA:** Run → Edit Configurations → Spring Boot → main class `com.pangate.gateway.GatewayApplication`,
   *Active profiles* `local`.
   **VS Code:** Spring Boot Dashboard, or a `launch.json` entry with `"mainClass": "com.pangate.gateway.GatewayApplication"`
   and `"args": "--spring.profiles.active=local"`.
3. First start generates dev secrets into `~/.pangate-dev` (CA, TLS cert, ES256 key + JWKS, ticket keys).

From the shell: `make run-local`.

## Tests

- `./mvnw -f gateway-service test` — unit tests (EmbeddedChannel), BlockHound enforces non-blocking `gw-io-*` threads.
- `./mvnw -f gateway-service verify` — plus `GatewayLocalIT` (boots the gateway, real TLS clients).
