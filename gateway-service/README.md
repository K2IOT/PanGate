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

Protection (M3): load-shedding ladder L1–L5 (`gw_shed_level`), outbound budget, slow-consumer close (4008),
connection recycling, warm-up before `/ready`, and a staged drain on SIGTERM or `POST /admin/drain` (local/dev:
at most 5 s). Kernel listen-queue metrics `gw_kernel_*` on Linux.

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
3. First start generates dev secrets into `~/.pangate-dev` (CA, TLS cert, ES256 key + JWKS, resume ticket keys `ticket-keys/`, TLS session ticket keys `tls-ticket-keys/`, `principal-hash.key`). Missing files are added on the next start, so an older dev directory keeps working.

From the shell: `make run-local`.

## Tests

- `./mvnw -f gateway-service test` — unit tests (EmbeddedChannel), BlockHound enforces non-blocking `gw-io-*` threads.
- `./mvnw -f gateway-service verify` — plus the ITs on Testcontainers Redis 7.4.2 and Kafka 3.9.1 (Docker needed):
  `GatewayLocalIT` (M1 behaviour), `GatewayClusterIT` (registry, seq, lifecycle, upstream, control, lease drills),
  `GatewayPushIT` (push/ack, timeouts, broadcast across loops, control push, single-session kick across two
  gateways through `fake-pushapi`), `GatewayProtectionIT` (warm-up, drain, slow consumer, shed, recycling, admission
  kill switch) and component ITs.

## Production image

`make images` builds `pangate/gateway-service:dev` and `:<version>` with Jib (distroless Java 21). The image sets the
JVM flags that do not depend on the environment (spec §9.1, §10):

| Flags | Why |
|---|---|
| `-XX:+UseZGC -XX:+ZGenerational` | Sub-millisecond pauses; GC does not add event-loop lag |
| `-XX:InitialRAMPercentage=50 -XX:MaxRAMPercentage=50 -XX:+AlwaysPreTouch` | Fixed heap at half the container memory, touched at start so it never grows under load |
| `-XX:+ExitOnOutOfMemoryError -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp` | The pod restarts instead of limping on |
| `--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true` | Netty's direct-memory fast paths |
| `-Djava.security.egd=file:/dev/urandom -Dfile.encoding=UTF-8` | SecureRandom never blocks at start |

The deployment sets the rest in `JAVA_TOOL_OPTIONS`:
- **prod:** `-XX:MaxDirectMemorySize=<25% of container memory>`. Without it the gateway refuses to start with
  profile `prod` (the JVM default equals the max heap, so heap plus direct memory would fill the container). The
  outbound budget is 35% of this value.
- **dev compose:** `-Xmx1g -XX:MaxDirectMemorySize=1g` (an explicit `-Xmx` wins over `MaxRAMPercentage`).

The container must have a memory limit: `AlwaysPreTouch` touches half of whatever memory the JVM sees. Netty leak
detection follows `gateway.netty.leak-detection` (prod `disabled`, local/dev `simple`; tests run `paranoid`). The
start-up log prints the result, e.g. `jvm gc=ZGC Minor Cycles,... heapMax=1024MiB directMax=1024MiB leak=simple`.

## Fuzz

Two Jazzer fuzz tests (spec §11): `FrameCodecFuzzTest` (random bytes into `FrameCodec.decode`, before and after
auth) and `HelloAuthFuzzTest` (random bytes, random JWTs, a valid JWT with one byte changed, random resume tickets
through `FrameCodec → HelloAuthHandler → SessionHandler`). Both fail on any unexpected exception or on a `ByteBuf`
leak at leak level `paranoid`.

- Every `make verify` runs them in regression mode: only the inputs under
  `src/test/resources/**/<Class>Inputs/` (seeds plus any past findings).
- `make fuzz FUZZ=FrameCodecFuzzTest` (or `HelloAuthFuzzTest`) fuzzes for 10 minutes. A finding is saved as a new
  input next to the seeds and fails the build; keep it as a regression input once fixed. The working corpus goes to
  `gateway-service/.cifuzz-corpus/` (git-ignored).
- `jazzer.instrument` in `src/test/resources/junit-platform.properties` instruments `com.pangate.**` and protobuf so
  coverage guides the fuzzer through the Frame wire format.
- CI workflow `fuzz` runs both targets nightly and on demand.

## Benchmarks

JMH benchmarks (spec §11) live in `src/jmh/java` and run with the `jmh` profile: `make bench` (results in
`gateway-service/target/jmh.json`; pass `JMH_ARGS=...` for other JMH options, e.g. `JMH_ARGS=-l` to list).

Baseline, 2026-11-05 (cloud dev container, 1 fork, 3 warm-up + 5 measured iterations of 10 s):

| Benchmark | Setup | Score |
|---|---|---|
| `FrameCodecBench.decodeUpstream` | UPSTREAM frame, 256 B payload | 217.6 ± 4.3 ns/op |
| `FrameCodecBench.encodePush` | PUSH frame, 256 B payload | 139.2 ± 3.0 ns/op |
| `PrincipalIndexBench.get` | 100k principals | 55.7 ± 7.5 ns/op |
| `PrincipalIndexBench.addRemove` | second device added and removed, 100k principals | 192.5 ± 17.9 ns/op |
| `DeadlineWheelBench.tick` | 100k sessions, deadlines over 60 s (~1.7k due per tick) | 13.7 ± 1.4 µs/op |

## Edge features (M3b)

- **Certificate rotation:** replace `gateway.crt`/`gateway.key` on disk; new connections get the new certificate within `gateway.tls.reload-interval` (30 s). A broken pair is ignored (`gw_tls_reloads_total{result="failed"}`). TLS session ticket keys and resume ticket keys are re-read every 60 s.
- **PROXY protocol:** `gateway.listeners.proxy-protocol.enabled=true` with `trusted-cidrs` (the LB subnets). Only those peers must send a PROXY v1/v2 header; loopback is always direct.
- **SSE fallback and HTTP upstream:** `POST /v1/sse` (body `Hello`, tenant switch `sse.fallback.enabled`) and `POST /v1/upstream` (`Authorization: Ticket <resume>`, body `Frame(UPSTREAM)`), both `application/x-protobuf`.
- **Tracing one principal:** produce `trace:<principal>` → `ControlEvent{issued_at_ms, trace: TracePrincipal{principal_id}}` on `gw.control`; the `pangate.trace` logger then logs that principal's sessions (hashed) for up to 15 minutes. A tombstone on the same key stops it.
- **OTel spans:** set `gateway.tracing.otlp-endpoint` (OTLP/HTTP) to export `gw.push.handoff` / `gw.push.write` for sampled pushes.
