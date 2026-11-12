# Changelog

## Unreleased

### push-api-service: spec §6 metrics
- `pushapi.produce{result}`, `pushapi.idempotency{result}`, `pushapi.ratelimited{kind}`, `pushapi.seq.latency` and
  `pushapi.status.lag` are emitted; the core reports them through `AcceptMetrics` (no Micrometer in the core).
- ADR 0027: ORDERED order is guaranteed per Push API pod only; clients reorder by seq.
- Platform architecture diagram shows sub-projects ①–④ as merged and the Push API as built in P1.
- `SeqEpochWatcherClusterIT`: failover detection on a 3-master Redis Cluster (restarting one master bumps only its
  slots, once).

### push-api-service (sub-project 4, P1)
- New module `push-api-service` (spec `docs/superpowers/specs/2026-11-10-push-api-design.md`):
  - `POST /v1/push`, `POST /v1/push:batch` (≤ 100 items, per-principal order), `GET /v1/messages/{msgId}`, with API tokens of auth-service checked offline against its JWKS;
  - gRPC `PushService.SendStream` with SPIFFE allowlist (the gateway's KICK);
  - rules V1–V14, scopes, kill switches and per-pod rate limits from `gw.control`, revocation of jti and API clients;
  - `Idempotency-Key` with reserved `msgId`/seq, `seq:{p}` / `topicseq:{t}`, `seqepochs` bump on Redis failover (`run_id`) and `POST /admin/v1/seqepochs:bump`;
  - message status in Redis from `push.offline` / `push.delivered`;
  - config in `push-api.yml`; `PushApiIT` runs Push API, router and two gateways in one JVM.
- Contracts: `TenantLimits.push_rate = 8`, `push_burst = 9`, `broadcast_rate = 10` (additive).
- Router: a CONTROL KICK goes to every live node (single-session replaces the old registry entry before the KICK, so routing by entry missed the old session).
- Build: `push-router-service` and `push-api-service` publish a plain main jar (exec classifier at plugin level).
- Devenv: compose service `push-api`; the gateway sends KICK to it; `fake-pushapi` moves to profile `tools`; `make push-api-local`; `make smoke` pushes through the API (`PUSH via API`, `STATUS delivered`, `OFFLINE`).
- Measurement (dev machine): 10 000 pushes through `POST /v1/push:batch` accepted in 2.7 s (≈ 3.6 k/s), all delivered.
- ADR 0022–0026.

### push-router-service (sub-project 3)
- New module `push-router-service` (spec `docs/superpowers/specs/2026-11-10-push-router-design.md`):
  - consumes `cN.push.requests` with Confluent Parallel Consumer 0.5.3.3 (`KEY` order, virtual-thread workers);
  - `DeliveryJob` applies the delivery outcome table: ACK wait, two retries, offline reasons, receipts, BROADCAST to every live node, CONTROL once per node;
  - one gRPC `PushStream` per gateway with batching, back-pressure and ACK matching;
  - `NODE_DOWN` on `cN.gw.lifecycle`; `/ready` includes the `router` indicator; metrics `router.*`;
  - config in `router.yml`; `RouterIT` runs a real gateway in the same JVM.
- Gateway: publishes `node:addr:{id}` (`gateway.grpc.advertise-host`); `gateway-service`'s executable jar is now `-exec.jar` and the main artifact is a plain jar.
- devtools: `push` (also `--count`/`--principals` load loops) and `await-offline`; `fake-pushapi --kafka`; `smoke --kafka --redis` adds PUSH via Kafka and OFFLINE (`--no-router` keeps the fake-router step).
- Devenv: compose service `router`; `fake-pushapi` produces to Kafka; `make images` compiles before Jib and builds the router; `make router-local`.
- Measurement (dev machine): 10 000 EPHEMERAL to 1 000 sessions, all delivered; ≈ 2.5–4 k records/s on a cold JVM; p50 1.2–1.7 s / p99 1.8–2.3 s under a single burst.
- ADR 0019–0021.

### Push-path contracts (sub-project 2)
- `platform-contracts`, spec `docs/superpowers/specs/2026-11-10-push-contracts-design.md`:
  - `PushRecord`, `OfflineRecord` and `DeliveredRecord` for `cN.push.requests`, `cN.push.offline` and `cN.push.delivered`;
  - `PushService.SendStream` accepts MESSAGE, device MESSAGE, BROADCAST and CONTROL, and returns typed responses (`code`, `msg_id`, `seq`, `topic_seq`, `duplicate`);
  - `ConnectionEvent.event_id` and `EVENT_TYPE_NODE_DOWN`.
- New module `platform-contracts-testkit`:
  - topic, key and header conventions;
  - deterministic fixtures and golden files;
  - validation cases V1–V9;
  - registry value reference cases;
  - `PushPathScenario` on real Kafka.
- Gateway:
  - registry value v2 `nodeId|epoch|connectedAtMs|platform|device`, while still reading v1;
  - lifecycle events carry a ULID `event_id`.
- Devenv: `kafka-init` creates `c1.push.requests` (64 partitions), `c1.push.offline` (16) and `c1.push.delivered` (16).
- ADR 0016–0018.

### auth-service (sub-project 1 after 0.1.0)
- New module `auth-service` (spec `docs/superpowers/specs/2026-11-05-auth-service-design.md`):
  - OAuth2 client credentials, giving ES256 API tokens with `aud=pangate-api`;
  - `POST /v1/tokens`, giving gateway connect tokens with `aud=pangate-gw`;
  - JWKS with publish-ahead key rotation (30 d lifetime, 7 d overlap, 1 h publish-ahead) under a PostgreSQL advisory lock;
  - `POST /v1/revocations` through a transactional outbox to `gw.control`, with tombstones on expiry;
  - internal Admin API on the management port;
  - rate limits, 503 when PostgreSQL is down while JWKS keeps serving, KEK fail-fast, metrics.
- Contracts: `Revoke.client_id = 5`, key `revoke:c:<client_id>`. The gateway ignores these keys without counting a bad record.
- `gateway-service` publishes a `verifier` classifier jar (package `com.pangate.gateway.auth`) for Auth's contract test.
- Gateway issuer by profile:
  - profile `dev` reads JWKS from `http://auth:8090`;
  - prod issuer defaults to `https://auth.pangate`;
  - profile `local` keeps devtools-signed tokens.
- Devenv: compose adds `postgres` and `auth` and drops the `issuer` container. `make smoke` mints through Auth and ends with revoke → close 4003. New targets: `make auth-local`, `make auth-local-check`.
- devtools: `AuthClient`; `smoke`/`token` accept `--auth-url --client-file [--client]`.
- ADR 0013–0015.

## 0.1.0 — 2026-11-05

First release of `gateway-service` and its base project (`platform-parent`, `platform-contracts`,
`platform-spring-starter`, `gateway-devtools`, `platform-devenv`). Design: `docs/superpowers/specs/2026-11-03-gateway-service-design.md`; decisions: `docs/adr/`.

### M1 — single node
- Raw Netty 4.2 data plane on Spring Boot 3.5: WSS `:8443` and TCP `:8883` over TLS 1.3 (BoringSSL), management on `:8081`.
- `HELLO` authentication with JWT (`JwksCache`) or resume ticket; `PING`/`PONG`, `REAUTH`, close codes.
- Shared-nothing event loops (`LoopContext`) with one `DeadlineWheel` per loop; self-routing `connId`.
- `AcceptGate` and `UpgradeGuard`: token bucket, caps, per-IP limits, admission ramp.
- `DevBootstrap`, Maven Wrapper, `Makefile`, Jib images, devtools `issuer`/`token`/`client`, compose devenv.

### M2 — cluster
- Redis registry valid by node epoch, "higher epoch wins" lease, resync after lease loss.
- Upstream and lifecycle to Kafka (two producers: reliable and telemetry); `gw.control` consumer (revoke, cell map, tenant config).
- gRPC `PushStream`: unicast and RPC by delivery class, `PendingAckWindow`, `DeliveryAck` batching.
- `SUBSCRIBE`, topic broadcast with pacing, control push, single-session `KICK`; devtools `fake-router` and `fake-pushapi`; `make smoke`.

### M3 — hardening
- Outbound budget, load-shedding ladder (levels 1–5), slow-consumer policy, kernel metrics.
- Batched drain on SIGTERM or `/admin/drain`, connection recycling, warm-up before ready, admission ramp, kill switches.
- TLS certificate hot reload, TLS session ticket and resume ticket key rotation.
- SSE fallback `POST /v1/sse` and `POST /v1/upstream` (per-principal rate limit, CORS).
- PROXY protocol v1/v2 from trusted CIDRs; tenant connection cap with borrowing through Redis.
- `trace.principal` kill switch and OTel spans `gw.push.handoff` / `gw.push.write`.
- Netty leak detection by profile; production JVM flags in the image; fail fast in prod without `-XX:MaxDirectMemorySize`.
- Jazzer fuzz tests for `FrameCodec` and `HelloAuthHandler`; JMH benchmarks; ADR 0001–0012.

### Known gaps
- The 50k idle-session check has only run with 18k sessions (environment limit `nofile` = 20 000); run
  `make load-check N=50000` on a machine with `ulimit -n` ≥ 120 000.
- Open questions in spec §14: `seqepochs` contract with Push API, router "one MESSAGE per conn_id" contract, mTLS for
  IoT devices, whether PROXY protocol is on by default.
- Not included: Helm chart, publishing artifacts to GitHub Packages.
