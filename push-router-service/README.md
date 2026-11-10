# push-router-service

Push router (spec: [docs/superpowers/specs/2026-11-10-push-router-design.md](../docs/superpowers/specs/2026-11-10-push-router-design.md);
contracts: [docs/superpowers/specs/2026-11-10-push-contracts-design.md](../docs/superpowers/specs/2026-11-10-push-contracts-design.md)).
It consumes `cN.push.requests`, finds each target's live connections in the gateway registry, sends `PushItem`s over one
gRPC `PushStream` per gateway, waits for ACKs, and writes the outcome to `cN.push.offline` / `cN.push.delivered`. It
also publishes `NODE_DOWN` on `cN.gw.lifecycle` when a gateway loses its lease or changes epoch.

## Run

| Mode | How | Notes |
|---|---|---|
| local | `make infra-up`, a gateway (`make run-local` or the IDE, profile `local`), then `make router-local` | Kafka `localhost:9092`, Redis `localhost:6379` standalone, plaintext gRPC; management `:8083` |
| dev | `make images up` | Container `router` in compose (profile `dev`); management `:8081` inside the container, `:8083` on the host |
| prod | Image `pangate/push-router-service` | Redis Cluster, mTLS to the gateways (`router.grpc.tls.*`) |

Configuration files are `router.yml` and `router-<profile>.yml` (`spring.config.name=router`), not `application.yml`:
`RouterIT` runs a real gateway in the same JVM, whose `application.yml` must stay the only one on the classpath.

Try it by hand against `make up`:

```bash
java -jar gateway-devtools/target/gateway-devtools-0.1.0-exec.jar push --kafka localhost:9092 \
  --redis redis://localhost:6379 --principal t1:me --payload '{"hi":1}'
java -jar gateway-devtools/target/gateway-devtools-0.1.0-exec.jar await-offline --kafka localhost:9092 --msg-id <id>
```

## Configuration (`router.*`)

| Key | Default | Meaning |
|---|---|---|
| `cell` | `c1` | Topic prefix: `c1.push.requests`, `c1.push.offline`, `c1.push.delivered`, `c1.gw.lifecycle` |
| `kafka.bootstrap`, `kafka.group` | `kafka:9092`, `push-router` | Consumer group; `group.instance.id` = hostname |
| `kafka.max-concurrency` | `20000` | Records in flight (one virtual thread each), ordered per key |
| `ack-timeout` | `6s` | Wait for a RELIABLE / ORDERED ACK per connection |
| `retries` | `[100ms, 400ms]` | Pauses before re-reading the registry and resending |
| `registry.uri`, `registry.topology` | `redis://redis-registry:6379`, `cluster` | Gateway registry Redis (`standalone` in local/dev) |
| `registry.cache`, `registry.timeout` | `2s`, `200ms` | `reg:{p}` cache; lookup timeout (then the record is retried) |
| `directory-refresh` | `1s` | Refresh of `nodes:live`, `node:{id}`, `node:addr:{id}` |
| `batch.max`, `batch.linger` | `256`, `2ms` | `PushBatch` size and linger per gateway stream |
| `max-in-flight-per-node` | `10000` | Unanswered items per gateway stream (back-pressure) |
| `grpc.plaintext`, `grpc.tls.{cert,key,trust}` | `false` | Plaintext in local/dev, mTLS in prod |

## Metrics (`/actuator/prometheus`)

| Metric | Tags |
|---|---|
| `router.records` | `kind` (message, broadcast, control, unknown), `result` (delivered, offline, dropped, invalid) |
| `router.delivery.latency` | `class`: `accepted_at_ms` → delivered (ACKED / WRITTEN) |
| `router.offline` | `reason` |
| `router.inflight` | — |
| `router.links` | `state=open` |
| `router.registry.latency` | — |
| `router.node_down` | — |
| Parallel Consumer meters | — |

`router.retries` and `router.broadcast.skipped` from spec §5 are not emitted yet.

## Health

`/ready` (management port) includes the `router` indicator: UP when the node directory refreshed within the last 10 s
and the consumer is polling. `/live` is the liveness state.

## Failure behaviour

| Event | Behaviour |
|---|---|
| Registry Redis slow or down | Lookup timeout → the record is retried by Parallel Consumer (100 ms → 5 s); never marked offline for this |
| Gateway dies | Pending items → `NACK_CLOSED` → retry with a fresh registry read; then `NODE_GONE` / `NO_SESSION`; `NODE_DOWN` published |
| Gateway overloaded | `NACK_OVERFLOW` / `SHED` / `SLOW_CONSUMER`: no retry, offline with that reason |
| Record does not parse or breaks the push rules | Skipped, `router.records{result=invalid}`, logged with topic/partition/offset (never the payload) |
| Outcome produce fails | The record stays unfinished and is processed again; gateways and clients dedup by `msg_id` |
| SIGTERM | Stop polling, wait ≤ 10 s for records in flight, commit, close links; unfinished records are redelivered after restart |

## Measurement (R1, dev machine)

Local profile, one gateway, 1 000 idle sessions (`devtools client --load 1000`), 10 000 EPHEMERAL MESSAGEs produced as
one burst with `devtools push --count 10000 --principals 1000` (≈ 6–7 k records/s from the producer): all 10 000
delivered; the router drained the burst ≈ 4 s after the first produce, about 2.5 k–4 k records/s on a cold JVM.
`router.delivery.latency{class=ephemeral}` p50 1.2–1.7 s, p99 1.8–2.3 s, dominated by queueing behind the burst (two runs).
