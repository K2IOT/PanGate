# Changelog

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
