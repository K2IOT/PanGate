# chat-service

Chat core of the PanGate chat app (CH1, spec `docs/superpowers/specs/2026-11-12-chat-core-design.md`): one-to-one
chats, groups of up to 500 and channels, on tenant `chat` of PanGate. Users are `chat:<userId>` principals; the
service itself only knows user ids (ADR 0028).

```
client ──WebSocket UPSTREAM cmd (op=chat.*)──▶ gateway ──▶ c1.gw.upstream.cmd ─┐
client ──HTTP POST /v1/conversations/{id}/messages ─────────────────────────────┤ Ingest (checks, limits)
                                                                                ▼
                         chat.commands (key = conversation) ──▶ Sequencer (dedup, LWT convSeq, ScyllaDB)
                                                                                ▼
                         chat.events ──▶ FanOut ──gRPC SendStream──▶ Push API ──▶ router ──▶ gateways ──▶ clients
```

## Run

- Devenv: `make images up` starts `chat` (profile `app`) with ScyllaDB, Kafka, Redis, auth and the Push API;
  `make smoke` runs the `CHAT 1-1`, `CHAT group` and `CHAT channel` steps. HTTP on host `8086`, management `8087`.
- From the jar against `make infra-up` plus `make scylla-up`: `java -jar chat-service/target/chat-service-*-exec.jar
  --spring.profiles.active=local` (HTTP 8086, management 8087, Push API at `localhost:9443` plaintext).
- Tokens: connect tokens of auth-service for tenant `chat` (`devtools token --auth-url … --client chat-backend`).

## Client protocol

Over the gateway WebSocket, `UPSTREAM` with headers `type=cmd` and `op`:

| `op` | payload | effect |
|---|---|---|
| `chat.send` | `ChatSend` | a message; every member's devices get a PUSH `type=chat.message` with `ChatMessage` |
| `chat.channel.open` | `ChannelOpen` | subscribes all of the user's connections to `chat:chan:<convId>` (public channel, or member) |
| `chat.channel.close` | `ChannelClose` | unsubscribes |

`UPSTREAM_ACK` means "received"; the message is in the conversation when its `ChatMessage` (same `clientMsgId`)
arrives. A refused command comes back as PUSH `type=chat.rejected` with `ChatRejected{reason}`: `invalid`,
`too_long`, `not_found`, `not_member`, `rate_limited`. Clients dedup by `ChatMessage.msg_id`: a retry may deliver a
message twice, never as two messages.

## HTTP API

`Authorization: Bearer <connect token>` on every `/v1/**` call; errors are `application/problem+json` with `code`
`invalid_request`, `not_found`, `forbidden`, `rate_limited`, `unavailable` or `invalid_token`.

| Endpoint | |
|---|---|
| `POST /v1/conversations/{id}/messages` | `Idempotency-Key` (= `clientMsgId`, a ULID) required; `{text, replyToMsgId?, mentionUserIds?}` → `201 {msgId, convSeq, sentAt}`, or `202 {clientMsgId}` after 3 s / without Redis |
| `GET /v1/conversations?cursor=&limit=` | my conversations, newest message first (`nextCursor`) |
| `GET /v1/conversations/{id}` | member, or anyone for a public channel |
| `GET /v1/conversations/{id}/messages?before=\|after=&limit=` | history by `convSeq`, ≤ 100 per page; neither = latest page |
| `POST /v1/sync` | `{conversations:[{id, lastSeq}], limitPerConversation?}` → messages after each cursor, or `tooMany` |
| `POST /v1/conversations` | `{kind: group\|channel, title, memberIds?, public?}`; the caller is owner |
| `POST /v1/dm/{userId}` | the pair's DM, created once |
| `POST /v1/conversations/{id}/members` · `DELETE …/members/{userId}` | owner or admin; anyone may leave |
| `POST /v1/conversations/{id}/join` | public channels |

A client's sync cursor is the highest `convSeq` it has received. `lastSeq` from `/v1/sync` (without `tooMany`) is
exactly that; `lastSeq` elsewhere (conversation, history, `tooMany`) is the conversation's reserved `last_seq`, which
can run ahead of what is written for a moment: use it to see that something is new, not as the next cursor.

## Configuration (`chat.*`, `chat.yml`)

| Key | Default | |
|---|---|---|
| `cell` / `management-port` | `c1` / `8081` | |
| `kafka.bootstrap`, `kafka.{commands,events,dlq,upstream}-topic`, `kafka.produce-timeout` | `kafka:9092`, `chat.commands`, `chat.events`, `chat.commands.dlq`, `c1.gw.upstream.cmd`, `5s` | |
| `scylla.{contact-points,local-dc,keyspace,timeout,replication-factor}` | `scylla:9042`, `datacenter1`, `chat`, `1s`, `1` | `LOCAL_QUORUM` / `LOCAL_SERIAL`; schema statements get 30 s |
| `redis.{uri,timeout,member-cache-ttl}` | `redis://redis:6379`, `200ms`, `60s` | |
| `auth.{jwks-uri,issuer,audience}` | `…`, `https://auth.pangate`, `pangate-gw` | |
| `push-api.{target,plaintext,timeout}` | `dns:///push-api:9443`, `false`, `2s` | mTLS client certificates are not wired in CH1 |
| `limits.*` | user 20/s burst 40, conversation 50/s burst 100, 500 members, 4096 bytes, 50 mentions | per pod |
| `http.send-wait` | `3s` | |

## Metrics

`chat.send.rejected{reason}`, `chat.ingest.records{result}`, `chat.send.latency` (command received → event written),
`chat.sequencer.lag`, `chat.lwt.conflicts`, `chat.dedup.hits`, `chat.fanout.push{result}`, `chat.fanout.lag`
(sent → pushed), `chat.rejected.push{result}`, `chat.channel.control{result}`, `chat.api.requests{route,code}`,
`chat.revocations.active`.

## Failure behaviour

| Failure | Behaviour |
|---|---|
| Kafka down | WebSocket: the gateway answers `ERROR 4500`, the client resends the same `clientMsgId`. HTTP: `503` + `Retry-After`. Sequencer and fan-out do not commit |
| ScyllaDB down (1 s timeout) | the sequencer rewinds and retries; reads answer `503`; `/ready` turns 503 after 10 s without an answer |
| Redis down | dedup reads ScyllaDB, members read ScyllaDB, HTTP sends answer `202` at once |
| Redis errors on a membership change | the change is stored, the request answers `503` (retry is safe): a cached list must not outlive a removal |
| Push API down | fan-out retries the failed requests (100 ms → 5 s), rewinds after 30 s; nothing is committed before it was sent |
| ScyllaDB fails after the pushes | fan-out retries only the inbox writes, not the pushes |
| Long fan-out backlog | polls of ≤ 100 events, a 120 s step budget, progress committed; `max.poll.interval.ms` 10 min |
| Rebalance, two sequencers on one partition | the old one's LWT fails; it drops its batch; no `convSeq` is used twice |
| Malformed record | counted and written to `chat.commands.dlq` |

## Measurement (CH1, dev machine)

`ChatLoadIT` (`./mvnw -f chat-service verify -Dit.test=ChatLoadIT -Dchat.load=true`): Push API, router, two gateways
and chat-service in one JVM, Kafka, ScyllaDB (`--smp 1`) and Redis in containers, on one machine with 4 vCPU and
15 GiB. Latency is per delivery, from the send to the PUSH at the client.

| Run | Deliveries | p50 | p99 | max |
|---|---|---|---|---|
| 1-1 over WebSocket, 10 chats, 20 messages/s | 200/200 | 113 ms | 219 ms | 229 ms |
| 1-1 over WebSocket, 50 chats, 100 messages/s | 1 000/1 000 | 132 ms | 443 ms | 471 ms |
| group of 500, 20 messages over HTTP at 5/s (2 500 pushes/s) | 10 000/10 000 | 98 ms | 276 ms | 281 ms |
| channel with 10 000 sessions (clients in a second JVM), 10 messages at 2/s | 100 000/100 000 | 168 ms | 300 ms | 313 ms |

At 100 messages/s the 1-1 p99 is above the spec's 300 ms. Where the time goes, as means:
- command received → event written: 83 ms, of which 54 ms is the wait in `chat.commands`;
- event → Push API answered: 77 ms.

Everything shares the four cores: seven services, three containers and the clients. Partitions are processed in
parallel by ingest, sequencer and fan-out; before that change the group p99 was 722 ms and the 1-1 p99 was 981 ms.

## Tests

`./mvnw -f chat-service verify`: unit tests, ScyllaDB/Kafka/Redis ITs, `QueryApiIT` (with the Push API in the JVM)
and `ChatE2EIT` (Push API, router, two gateways and chat-service in one JVM; devtools WebSocket clients).
