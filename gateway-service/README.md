# gateway-service

Netty data plane on Spring Boot (spec: `docs/superpowers/specs/2026-11-03-gateway-service-design.md`).

| Port | What |
|---|---|
| 8443 | WSS `/v1/ws`, subprotocol `pangate.v1` |
| 8883 | TCP + TLS, ALPN `pangate.v1`, 4-byte length prefix |
| 9090 | gRPC `PushStream` for the push router (mTLS + SPIFFE in prod; plaintext in `local`/`dev`), header `pangate-push-version: 1` |
| 8081 | Management: `/ready` (reasons in `details.reasons`), `/live`, `/actuator/prometheus`, `POST /admin/drain` (loopback) |

Dependencies (M2): Redis (lease, registry `reg:{p}`, `seq:{p}`, `seqepochs`) and Kafka (upstream, lifecycle,
`gw.control`). Startup waits up to `gateway.startup.dependency-timeout` for them, then exits with a one-line hint.
Single-session KICK for connections on other nodes goes to the Push API (`gateway.push-api.*`; disabled in `local`,
where every connection is on the one node; `fake-pushapi` in `dev`).

Push by hand while a client is connected (`client` prints PUSH and ACKs reliable ones):

```bash
DT=gateway-devtools/target/gateway-devtools-0.1.0-SNAPSHOT-exec.jar
java -jar $DT client --url wss://localhost:8443/v1/ws --ca ~/.pangate-dev/ca.crt \
  --key ~/.pangate-dev/es256.pem --tenant t1 --principal t1:me --subscribe t1:news
java -jar $DT fake-router --target localhost:9090 unicast --conn-id <conn_id from CONNECTED> --class reliable
java -jar $DT fake-router --target localhost:9090 broadcast --tenant t1 --topic t1:news
```

## Run from the IDE (profile `local`)

1. `make bootstrap` once (installs contracts, starter and devtools into `~/.m2`), and `make infra-up`
   (Kafka + Redis on `localhost`; or native installs on the same ports).
2. **IntelliJ IDEA:** Run → Edit Configurations → Spring Boot → main class `com.pangate.gateway.GatewayApplication`,
   *Active profiles* `local`.
   **VS Code:** Spring Boot Dashboard, or a `launch.json` entry with `"mainClass": "com.pangate.gateway.GatewayApplication"`
   and `"args": "--spring.profiles.active=local"`.
3. First start generates dev secrets into `~/.pangate-dev` (CA, TLS cert, ES256 key + JWKS, ticket keys).

From the shell: `make run-local`.

## Tests

- `./mvnw -f gateway-service test` — unit tests (EmbeddedChannel), BlockHound enforces non-blocking `gw-io-*` threads.
- `./mvnw -f gateway-service verify` — plus the ITs on Testcontainers Redis 7.4.2 and Kafka 3.9.1 (Docker needed):
  `GatewayLocalIT` (M1 behaviour), `GatewayClusterIT` (registry, seq, lifecycle, upstream, control, lease drills),
  `GatewayPushIT` (push/ack, timeouts, broadcast across loops, control push, single-session kick across two
  gateways through `fake-pushapi`) and component ITs.
