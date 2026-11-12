# push-api-service

Push API (spec: [docs/superpowers/specs/2026-11-10-push-api-design.md](../docs/superpowers/specs/2026-11-10-push-api-design.md);
contracts: [docs/superpowers/specs/2026-11-10-push-contracts-design.md](../docs/superpowers/specs/2026-11-10-push-contracts-design.md)).
The only way down to clients: tenant backends push over HTTP with an API token from auth-service, internal services
(the gateway's KICK) over gRPC `PushService.SendStream`. One `PushAcceptor` validates (V1–V14), authorizes, applies the
tenant's kill switches and rate limits, deduplicates by `Idempotency-Key`, allocates `seq:{p}` / `topicseq:{t}`, builds
the `PushRecord` with a complete `Frame`, and produces it to `c1.push.requests` for the push-router.

## Run

| Mode | How | Notes |
|---|---|---|
| local | `make infra-up`, `make auth-local`, then `make push-api-local` | HTTP `:8085`, gRPC `:9443` plaintext, management `:8084`; JWKS from `http://localhost:8090` |
| dev | `make images up` | Container `push-api`; host ports 8085 / 9443 / 8084; the gateway sends KICK to `push-api:9443` |
| prod | Image `pangate/push-api-service` | Redis Cluster, mTLS with SPIFFE allowlist, JWKS `https://auth.pangate` |

Configuration files are `push-api.yml` and `push-api-<profile>.yml` (`spring.config.name=push-api`), so the ITs can run
Push API, router and gateways in one JVM.

```bash
TOKEN=$(curl -s -u "t1-backend:<secret from .dev/auth/clients.json>" -d grant_type=client_credentials \
  localhost:8090/oauth2/token | jq -r .access_token)
curl -s -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" localhost:8085/v1/push \
  -d '{"target":{"type":"principal","id":"t1:me"},"receipt":true,"payload":{"type":"hello"}}'
curl -s -H "Authorization: Bearer $TOKEN" localhost:8085/v1/messages/<msgId>
```

## Endpoints

| Port | Endpoint | Who | Result |
|---|---|---|---|
| 8080 | `POST /v1/push` | Bearer token API, scope `push:send` (MESSAGE) / `push:broadcast` (topic) | `202 {msgId, seq?, seqEpoch?, topicSeq?, cell, duplicate?}` |
| 8080 | `POST /v1/push:batch` | same; `{items: [...]}` ≤ 100, ≤ 1 MB | `200 {results: [{index, status, …}]}`; one principal's items in index order |
| 8080 | `GET /v1/messages/{msgId}` | `push:send` or `push:broadcast` | `{state: accepted \| delivered \| offline, …}`; `delivered` only with `receipt=true` |
| 9443 | gRPC `PushService.SendStream` | gateway, push-router (SPIFFE allowlist) | `PushResponse` per `request_id`, out of order |
| 8081 | `POST /admin/v1/seqepochs:bump` `{slots}` / `{all:true}` | API token with `admin` of tenant `pangate` | `{bumped, epochs}`; `{bumped, maxEpoch}` for `all` |
| 8081 | `/ready`, `/live`, `/actuator/prometheus` | operations | readiness includes `pushapi`: `gw.control` replayed and JWKS loaded |

Errors are `application/problem+json` with `code`: `invalid_request`, `rule_V1`…`rule_V14`, `payload_too_large`,
`invalid_token` (401), `token_revoked`, `forbidden`, `tenant_blocked`, `broadcast_disabled` (403), `rate_limited` (429 +
`Retry-After`), `unavailable` (503 + `Retry-After`), `batch_too_large`.

## Configuration (`push-api.*`)

| Key | Default | Meaning |
|---|---|---|
| `cell` | `c1` | Topic prefix and the `TenantCfg` cell scope |
| `kafka.bootstrap`, `kafka.produce-timeout`, `kafka.status-group` | `kafka:9092`, `5s`, `push-api-status` | Produce waits for the ack at most this long |
| `redis.uri`, `redis.topology`, `redis.timeout` | `redis://redis-state:6379`, `cluster`, `200ms` | Redis state: `seq:{p}`, `topicseq:{t}`, `seqepochs`, `idem:*`, `msg:*` |
| `auth.jwks-uri`, `auth.issuer`, `auth.audience` | `https://auth.pangate/...`, `https://auth.pangate`, `pangate-api` | API token check |
| `limits.push-rate`, `push-burst`, `broadcast-rate`, `status-rate` | 1000, 2000, 10, 100 | Per tenant **per pod**; `TenantLimits.push_rate/push_burst/broadcast_rate` on `gw.control` override |
| `ttl.default-ttl`, `ttl.max` | 86400 s, 604800 s | Message expiry |
| `idempotency.ttl`, `pending-ttl`, `wait-for` | 1 h, 30 s, 2 s | `Idempotency-Key` dedup |
| `seqepochs.refresh` | 5 s | `seqepochs` cache refresh and failover check |
| `grpc.port`, `grpc.plaintext`, `grpc.allowed-spiffe`, `grpc.tls.*` | 9443, false, gateway + push-router of cell-1 | mTLS server |
| `management-port` | 8081 | Second Tomcat connector |

## Metrics

`pushapi.requests{surface, code}`, `pushapi.accept.latency{kind}`, `pushapi.seqepoch.bumps{reason}`,
`pushapi.seq.regressions`, `pushapi.seqepoch.cache_age_seconds`, `pushapi.control.records{result=malformed}`,
`pushapi.revocations.active`, `pushapi.status.updates{topic, result}`, `pushapi.produce{result=ok|failed}`,
`pushapi.idempotency{result=new|duplicate|pending_wait|pending_timeout}`, `pushapi.ratelimited{kind}`,
`pushapi.seq.latency`, `pushapi.status.lag` (router outcome `at_ms` → status written).

## Failure behaviour

| Event | Behaviour |
|---|---|
| Kafka slow or down | `503`, `retry_after_ms=500`; the idempotency key is released, and a retry with it reuses the same `msgId` and seq |
| Redis state down | `503` for anything needing seq or idempotency; EPHEMERAL without a key still works; status writes are best effort |
| Redis state failover (new `run_id`) | `seqepochs` of the shard's slots bumped once (script on `runids{seqepochs}`), above every epoch the pod knew |
| JWKS unreachable | Cached keys keep working; a pod that never loaded a JWKS stays not ready |
| `gw.control` revoke / kill switch | Applied within about a second; a new pod is ready only after replaying the topic |
| SIGTERM | gRPC and HTTP drain (≤ 10 s), then the producer, listeners and Redis close |

## Measurement (P1, dev machine)

Local profile: auth, one gateway, router and Push API from their jars; 1 000 idle sessions; 10 000 EPHEMERAL MESSAGEs
as 100 `POST /v1/push:batch` of 100 items, 10 in parallel (curl), with the tenant's rate raised for the run. All 10 000
accepted in 2.7 s (≈ 3.6 k pushes/s) and delivered 4.6 s after the first request. `pushapi.accept.latency` per item
p50 141 ms / p99 384 ms (each item waits for its Kafka ack; items of one principal run in order); router delivery
latency p50 2.1 s / p99 2.6 s under the burst. With the default limits (1 000/s, burst 2 000) the same run gets 4 066
accepted and 5 934 `429`.
