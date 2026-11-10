# push-api-service — Design Spec

- **Ngày:** 2026-11-10
- **Trạng thái:** Implemented (P1, plan `docs/superpowers/plans/2026-11-10-push-api-p1.md`). Đo trên máy dev: 10 000 push EPHEMERAL qua `POST /v1/push:batch` (100 mục, 10 song song) tới 1 000 phiên: accept hết trong 2,7 s (≈ 3,6 k/s), giao hết sau 4,6 s; `pushapi.accept.latency` mỗi mục p50 141 ms / p99 384 ms
- **Nguồn kiến trúc:** *Netty Gateway — 10M Active Connections* ("tài liệu gốc"); số mục trong ngoặc, ví dụ (§3.2), là mục của tài liệu gốc.
- **Các spec liên quan:**
  - "spec contracts": `docs/superpowers/specs/2026-11-10-push-contracts-design.md` (C1): `PushRequest`/`PushResponse`, luật V1–V14 (§3.1), ánh xạ HTTP (§3.2), state seq và idempotency (§3.3), `PushRecord` và luật §1.1.
  - "spec router": `docs/superpowers/specs/2026-11-10-push-router-design.md` (R1).
  - "spec auth": `docs/superpowers/specs/2026-11-05-auth-service-design.md` (A1): token API, JWKS, scope, `revoke:c:<client_id>`.
  - "spec gateway": `docs/superpowers/specs/2026-11-03-gateway-service-design.md`: KICK qua `SendStream` (§6.6), `seqepochs` (§6.7).
- **Vị trí trong lộ trình:** sub-project 4/4: (1) auth (A1), (2) contracts (C1), (3) push-router (R1), **(4) push-api**.

---

## 0. Mục tiêu, phạm vi

**Mục tiêu.** Push API là đường xuống duy nhất (§3.2): nhận push của tenant backend (HTTP) và của service nội bộ, trong đó có KICK của gateway (gRPC), rồi produce `PushRecord` lên `cN.push.requests` cho router.

**Quyết định đã chốt khi brainstorm (2026-11-10):**

| # | Câu hỏi | Quyết định |
|---|---|---|
| P1 | Bề mặt của milestone đầu | HTTP `POST /v1/push` (token API qua JWKS, scope) **và** gRPC `PushService.SendStream` (mTLS allowlist); thay `fake-pushapi` trong devenv |
| P2 | Rate limit / quota | Token bucket trong bộ nhớ mỗi pod, theo tenant × kind; `QUOTA_EXCEEDED` chưa dùng |
| P3 | Bump `seqEpoch` sau failover | Tự động: theo dõi `run_id` của master từng shard Redis state; thêm endpoint admin để bump thủ công |
| P4 | Tính năng thêm | Chặn token bị thu hồi (`gw.control`), `POST /v1/push:batch`, kill switch và rate theo tenant (`TenantCfg`), `GET /v1/messages/{msgId}` |
| P5 | Lưu trạng thái message | Redis hash `msg:{t}:<id>`, TTL theo message |
| P6 | Kiến trúc | Một service, một lõi `PushAcceptor` dùng chung cho HTTP và gRPC (phương án A) |

**Tiêu chí thành công.** Tenant backend lấy token từ auth, gọi `POST /v1/push`, client nhận PUSH qua router và gateway, `GET /v1/messages/{id}` trả `delivered`. KICK của gateway đi qua Push API thật. `make smoke` xanh không cần `fake-pushapi`.

**Ngoài phạm vi P1:**
- quota ngày và billing (`CODE_QUOTA_EXCEEDED`);
- Inbox và `topic_log`;
- fan-out theo thành viên topic;
- định tuyến nhiều cell qua `cellmap`;
- webhook cho receipt.

**Bất biến:**
1. Chỉ Push API produce vào `cN.push.requests`. Mọi record nó produce đều thoả luật §1.1 của spec contracts. Record sai luật là bug: trả `500`, không produce.
2. `tenant_id` của HTTP luôn lấy từ token, không bao giờ từ body.
3. Thứ tự theo principal được giữ: trong một batch hay một stream gRPC, các request cùng principal được produce theo thứ tự đến.
4. Log không chứa payload hay token.

---

## 1. Kiến trúc

Module mới `push-api-service/`, package `com.pangate.pushapi`.
- **Parent:** `platform-parent` 0.1.0.
- **Phụ thuộc:** `platform-contracts`, `platform-spring-starter`, `kafka-clients`, `lettuce-core`, `grpc-netty-shaded`, `nimbus-jose-jwt`, Spring Web (MVC trên virtual threads).
- **Test:** `platform-contracts-testkit`; `PushApiIT` thêm `push-router-service`, `gateway-service` (jar thường) và `gateway-devtools`.
- **Image:** Jib distroless Java 21, `pangate/push-api-service`.
- **Cấu hình:** `push-api.yml` / `push-api-<profile>.yml` (`spring.config.name=push-api`), để IT chạy chung JVM với gateway và router.

| Cổng | Bề mặt | Ai gọi |
|---|---|---|
| `:8080` HTTP | `POST /v1/push`, `POST /v1/push:batch`, `GET /v1/messages/{msgId}` | Tenant backend, Bearer token API (`aud=pangate-api`) |
| `:9443` gRPC | `PushService.SendStream` | Gateway (KICK), service nội bộ; mTLS + allowlist SPIFFE (plaintext ở local/dev) |
| `:8081` quản trị | `/ready`, `/live`, `/actuator/prometheus`, `POST /admin/v1/seqepochs:bump` | Vận hành |

```
HTTP /v1/push ─┐  (JWT/JWKS, scope, revoke)            ┌─▶ Kafka c1.push.requests (chờ ack)
               ├─▶ PushAcceptor ────────────────────────┤
gRPC SendStream┘  (mTLS allowlist)                     └─▶ Redis state: seq:{p}, topicseq:{t},
                    validate V1–V14 → kill switch →          idem:{t}:{k}, msg:{t}:<id>
                    rate limit → idempotency → seq →
                    Frame + PushRecord → produce

ControlListener   ◀── gw.control   (revoke jti/client, TenantCfg)
StatusListener    ◀── c1.push.offline, c1.push.delivered  ──▶ msg:{t}:<id>
SeqEpochWatcher   ── INFO run_id mỗi 5 s ──▶ bump seqepochs cho các slot của shard bị failover
```

| Lớp | Trách nhiệm |
|---|---|
| `PushRequests` | V1–V14 trên `PushRequest` → `CODE_INVALID` kèm tên luật |
| `HttpMapping` | JSON ↔ `PushRequest`/`PushResponse` theo spec contracts §3.2 |
| `Callers` | Quyền: scope `push:send` cho MESSAGE, `push:broadcast` cho BROADCAST; CONTROL chỉ qua gRPC; gRPC theo allowlist SPIFFE |
| `ApiTokenVerifier` | ES256 offline qua JWKS của auth; `aud`, `iss`, `exp`; jti và client bị thu hồi |
| `TenantPolicies` | Kill switch và rate theo tenant: mặc định ← fleet ← cell ← tenant |
| `RateLimiter` | Token bucket trong bộ nhớ theo tenant × kind |
| `Idempotency` | `idem:{t}:{k}`: chiếm chỗ (`pending`), produce, lưu `PushResponse` (`done`) |
| `SeqAllocator` | `INCR seq:{p}` / `topicseq:{t}`; `seq_epoch` từ cache `seqepochs`; phát hiện seq lùi |
| `RecordBuilder` | `msg_id` ULID, `Frame` (ADR 0017), `PushRecord`, `expires_at_ms`; kiểm lại bằng luật §1.1 |
| `RecordProducer` | Producer idempotent `acks=all`; key và header theo testkit; chặn tới khi ack |
| `PushAcceptor` | Ghép các bước §2; HTTP và gRPC cùng gọi vào |
| `MessageStatus`, `StatusListener` | Ghi `accepted` khi accept; cập nhật `delivered` / `offline` từ hai topic kết quả |
| `ControlListener` | Đọc `gw.control` từ đầu (compact): revoke, `TenantCfg` |
| `SeqEpochWatcher` | Phát hiện `run_id` đổi và bump epoch các slot của shard đó |

Lõi (`PushRequests`, `HttpMapping`, `Callers`, `TenantPolicies`, `RateLimiter`, `Idempotency`, `SeqAllocator`, `RecordBuilder`, `PushAcceptor`) không import Spring và test được không cần Spring.

---

## 2. Đường accept

**Thứ tự** (HTTP và gRPC dùng chung từ bước 2):

1. **Xác thực.**
   - HTTP: Bearer token API (claim theo spec auth §3: `tenant`, `client_id`, `scope`, `jti`, `iat`). ES256 offline theo JWKS; `aud=pangate-api`, `iss`, `exp`/`nbf` với độ lệch đồng hồ 30 s; jti không bị thu hồi; `client_id` không bị chặn (§4.1). Sai → `401`, bị thu hồi → `403`.
   - gRPC: SPIFFE ID của peer nằm trong `push-api.grpc.allowed-spiffe`. Plaintext (local/dev) thì bỏ qua.
   - HTTP: `tenant_id` lấy từ claim `tenant` của token.
2. **V1–V14** → `CODE_INVALID` (`400`), `error` nêu luật.
3. **Phân quyền.** MESSAGE cần `push:send`; BROADCAST cần `push:broadcast`; CONTROL qua HTTP → `CODE_FORBIDDEN` (`403`).
4. **Kill switch** (§4.1) → `CODE_FORBIDDEN`. CONTROL qua gRPC luôn được cho qua (KICK cần cho single-session).
5. **Rate limit** theo tenant × kind → `CODE_RATE_LIMITED` (`429`, `retry_after_ms`, header `Retry-After`). CONTROL qua gRPC không bị giới hạn.
6. **Idempotency, đọc / chiếm** khi có `idempotency_key` (§2.1).
7. **Cấp seq** (§3): MESSAGE RELIABLE/ORDERED tới principal → `INCR seq:{p}`; BROADCAST RELIABLE/ORDERED → `INCR topicseq:{t}`. Device target, EPHEMERAL và CONTROL không có seq. Bỏ qua nếu bước 6 trả về một `pending` cũ đã có seq.
8. **Dựng record.**
   - `msg_id` ULID (hoặc giá trị của `pending` cũ); `accepted_at_ms` = now; `expires_at_ms` = now + `ttl_sec` (0 → 86 400).
   - `Frame{type=PUSH, msg_id, class, payload, headers, seq/seq_epoch hoặc topic/topic_seq, collapse_key}` theo ADR 0017. Class `UNSPECIFIED` → RELIABLE; priority `UNSPECIFIED` → NORMAL.
   - `PushRecord` với `cell_id`, `receipt`, `sender_principal_id`; kiểm lại bằng luật §1.1.
9. **Produce** lên `cN.push.requests`: key và header theo testkit (`PushKeys`, `PushHeaders`), chờ ack (timeout `produce-timeout`, 5 s). Lỗi → `CODE_UNAVAILABLE` (`503`, `retry_after_ms=500`), xoá idempotency `pending`.
10. **Trạng thái:** `msg:{t}:<id>` = `accepted` (§4.2), ghi **trước** bước 9: kết quả của router có thể tới StatusListener trước một lần ghi sau produce, và kết quả cho key chưa tồn tại bị bỏ. `state` chỉ được đặt khi chưa có, nên lần accept retry không ghi đè `delivered`/`offline`. Lỗi chỉ log và đếm, không làm request thất bại. CONTROL không ghi trạng thái. Produce lỗi thì trạng thái `accepted` còn lại tới hết TTL; retry cùng key dùng lại đúng `msgId` đó (review luồng P1).
11. **Idempotency, ghi `done`** (§2.1).
12. **Trả lời:** HTTP `202 {msgId, seq?, seqEpoch?, topicSeq?, cell}`; gRPC `PushResponse{ok=true, CODE_OK, msg_id, seq, seq_epoch, topic_seq, cell_id}`.

**Lỗ hổng seq.** Seq được cấp trước khi produce; produce lỗi để lại một lỗ trong `seq:{p}`. Client thấy lỗ và đồng bộ lại (SDK, Inbox Phase 2). Push API không bao giờ cấp hai lần cùng một seq trong cùng epoch, trừ cửa sổ failover ở §3.3.

### 2.1 Idempotency (`idem:{<tenant>}:{<key>}`, spec contracts §3.3)

| Bước | Lệnh | Kết quả |
|---|---|---|
| Đọc | `GET` | `done\|<PushResponse base64>` → trả đúng response, `duplicate=true`, `CODE_OK`, không produce |
| | | `pending\|…` còn sống → chờ tối đa `idempotency.wait` (2 s), hỏi lại mỗi 50 ms; vẫn `pending` → `CODE_UNAVAILABLE`, `retry_after_ms=200` |
| | | không có → chiếm chỗ |
| Chiếm | `SET … pending\|<msgId>\|<seq>\|<seqEpoch>\|<topicSeq> NX PX 30000` sau bước 7 | thua cuộc đua → quay lại "Đọc" |
| Produce lỗi | `DEL` (chỉ khi value vẫn là `pending` của mình, script so sánh) | lần retry chạy lại từ đầu |
| Thành công | `SET … done\|<base64> PX 3600000` | |

- **Pod chết sau produce, trước `done`:** `pending` hết hạn sau 30 s. Để không cấp `msgId`/seq mới cho cùng key, giá trị `pending` được sao vào `idemlast:{t}:{k}` (TTL 1 h) ngay khi chiếm. Lần chiếm sau đọc `:last` và dùng lại đúng `msgId`/seq cũ khi produce lần nữa, nếu cùng kind, class và target (ADR 0023; review cuối P1). Router, gateway và client dedup theo `msg_id`, nên client không thấy hai bản.
- **Không so body:** cùng key với body khác vẫn là duplicate (spec contracts §3.3).
- **Không có `idempotency_key`:** không chạm Redis cho idempotency.

### 2.2 Batch `POST /v1/push:batch`

- Body `{items: [ <như body POST /v1/push>, kèm idempotencyKey? ] }`, 1..100 mục, ≤ 1 MB.
- Mỗi mục đi qua bước 2–12 độc lập; rate limit tính từng mục.
- Các mục cùng target key (principal, hoặc `topic:<t>`) xử lý tuần tự theo `index`; khác key thì song song trên virtual threads.
- Trả `200 {results: [{index, status (HTTP status của mục), msgId?, seq?, seqEpoch?, topicSeq?, code?, error?, duplicate?}]}`. Body hỏng hoặc quá giới hạn → `400`/`413` cho cả batch.
- Header `Idempotency-Key` bị bỏ qua cho batch; mỗi mục có `idempotencyKey` riêng.

### 2.3 gRPC `SendStream`

- Mỗi `PushRequest` xử lý trên một virtual thread. Response có thể về không theo thứ tự và được ghép bằng `request_id`.
- Các request cùng target key trên một stream xếp hàng tuần tự, như batch.
- Tối đa 1 000 request đang xử lý mỗi stream: khi đầy thì không `request(1)` thêm (flow control của gRPC).
- `ok = (code == CODE_OK)` cho bên gọi 0.1.0. V9: request KICK 5 field của gateway 0.1.0 phải ra `CODE_OK`.

### 2.4 HTTP

- `Content-Type: application/json`; body đơn ≤ 128 KB. `payload` là JSON bất kỳ, ghi thành bytes UTF-8; `headers["content-type"] = application/json`.
- Ánh xạ field đúng bảng §3.2 của spec contracts; header `Idempotency-Key` → `idempotency_key`.
- `POST /v1/push` đơn cũng đi qua hàng đợi theo target key như batch, nên các push cùng principal tới một pod cùng lúc vào Kafka đúng thứ tự seq. Giữa nhiều pod thì không đảm bảo (review luồng P1).
- Lỗi là `application/problem+json` với `code` ổn định: `invalid_request`, `payload_too_large`, `rule_V1`…`rule_V14`, `forbidden`, `tenant_blocked`, `rate_limited`, `unavailable`, `invalid_token`.

---

## 3. Seq và `seqepochs`

### 3.1 Cấp seq

- `INCR seq:{p}` / `INCR topicseq:{t}` trên Redis state, timeout `redis.timeout` (200 ms) → `CODE_UNAVAILABLE`.
- `seq_epoch` = `seqepochs[SeqKeys.slot(principal)]` từ cache `int[16384]` (slot không có = 0), làm mới bằng `HGETALL seqepochs` mỗi `seqepochs.refresh` (5 s) và ngay sau mỗi lần bump. BROADCAST không có epoch.
- **Seq lùi:** một LRU 100 000 mục giữ seq lớn nhất pod đã cấp cho mỗi principal. Nếu `INCR` trả giá trị ≤ giá trị đó, `SeqAllocator` gọi `SeqEpochWatcher` ngay, chờ bump xong rồi cấp lại (`pushapi.seqepoch.bumps{reason=regression}` nếu watcher bump).

### 3.2 Phát hiện failover (`SeqEpochWatcher`)

Chạy mỗi 5 s, và ngay khi Lettuce báo reconnect hoặc topology đổi.
- **Cluster:** `CLUSTER SHARDS` → các master cùng dải slot; `INFO server` trên từng master → `run_id`.
- **Standalone** (local/dev): một shard phủ 0..16383. Redis khởi động lại không có persistence cũng đổi `run_id`, và khi đó seq đúng là đã mất.
- `run_id` đã biết lưu ở hash `runids{seqepochs}`: hash tag đặt key này cùng slot với `seqepochs`, nên không thêm field lạ vào `seqepochs` (gateway coi mọi field là số slot). Field `<slotStart>-<slotEnd>`, value `run_id`.
- **Script Lua** trên shard giữ `seqepochs`, chạy nguyên tử:
  - dải chưa có trong `runids` → ghi `run_id`, không bump (lần đầu thấy, hoặc dải mới sau resharding);
  - `run_id` khác giá trị đã lưu → ghi `run_id` mới và `HINCRBY seqepochs <slot> 1` cho mọi slot của dải;
  - dải không còn tồn tại → xoá field.
  Nhiều pod cùng thấy một failover chỉ bump một lần.

### 3.3 Cửa sổ rủi ro (hạn chế đã biết)

Từ lúc failover tới lúc bump, thường dưới 1 s nhờ sự kiện reconnect và tối đa khoảng 5 s, một seq mới có thể trùng seq đã cấp trước failover mà vẫn mang epoch cũ. Phát hiện seq lùi (§3.1) thu hẹp cửa sổ này cho những principal pod đã thấy. Phần còn lại được dedup theo `msg_id`; SDK và Inbox Phase 2 xử lý.

### 3.4 Bump thủ công

`POST /admin/v1/seqepochs:bump` trên cổng quản trị, body `{slots: [int…]}` hoặc `{all: true}`. Cần Bearer token API có scope `admin` của tenant `pangate`. Trả `{bumped, epochs: {slot: epoch}}`; khi `all` thì rút gọn thành `{bumped, maxEpoch}`; ghi log audit.

---

## 4. Consumer

### 4.1 `ControlListener` (`gw.control`)

Đọc từ đầu khi khởi động, không dùng consumer group (giống gateway).

| Key | Hành vi |
|---|---|
| `revoke:j:<jti>` | jti bị thu hồi tới `expires_at_ms` |
| `revoke:c:<client_id>` | Chặn mọi token của client đó có `iat` < `issued_at_ms` của sự kiện, tới `expires_at_ms` |
| `revoke:p:*`, `revoke:d:*` | Bỏ qua (phiên client, việc của gateway) |
| `tenantcfg:fleet`, `tenantcfg:cell:<c>`, `tenantcfg:tenant:<t>` | Gộp policy: mặc định ← fleet ← cell (cell của Push API) ← tenant |
| Tombstone | Xoá state của key |
| `cellmap:*`, `trace:*`, bản ghi hỏng | Bỏ qua; bản ghi hỏng được đếm |

**Policy:**
- `KillSwitches.tenant_blocked = true` → mọi push của tenant bị `CODE_FORBIDDEN` (`tenant_blocked`), trừ CONTROL qua gRPC.
- `KillSwitches.broadcast_enabled = false` → BROADCAST bị `CODE_FORBIDDEN`.
- `TenantLimits` thêm (additive, `buf breaking` xanh, gateway bỏ qua): `optional uint32 push_rate = 8; optional uint32 push_burst = 9; optional uint32 broadcast_rate = 10;`. Đơn vị: record/giây trên mỗi pod. Mặc định từ `push-api.limits`.

**Readiness:** `/ready` UP chỉ sau khi đã đọc tới offset cuối lúc khởi động. Sau đó mất Kafka thì state cuối được giữ. Mục thu hồi hết hạn bị dọn mỗi phút.

### 4.2 Trạng thái message (`StatusListener`, `MessageStatus`)

Hash `msg:{<tenant>}:<msgId>` trên Redis state:

| Field | Ghi bởi |
|---|---|
| `state` = `accepted` \| `delivered` \| `offline` | accept / listener |
| `kind`, `class`, `receipt`, `target` | accept |
| `acceptedAt`, `expiresAt` | accept |
| `deliveredConns`, `lastDeliveredAt`, `lastStatus` | `cN.push.delivered` |
| `offlineReason`, `attempts`, `offlineAt` | `cN.push.offline` |

- TTL = `expiresAt` − now + 1 h, đặt khi accept.
- Consumer group `push-api-status` trên `cN.push.offline` và `cN.push.delivered`, `CooperativeStickyAssignor`, commit sau khi ghi Redis. Redis lỗi → không commit, thử lại.
- Ghi bằng script Lua: chỉ khi key tồn tại; `delivered` thắng `offline` (bản giao lại sau restart router không ghi đè kết quả tốt hơn); `deliveredConns` tăng mỗi `DeliveredRecord`.
- Router chỉ ghi `cN.push.delivered` khi `receipt=true`, nên message không receipt chỉ có `accepted` hoặc `offline`. BROADCAST chỉ có `accepted`.

`GET /v1/messages/{msgId}`: token API có `push:send` hoặc `push:broadcast`; tenant lấy từ token. Trả `200 {msgId, state, kind, class, receipt, acceptedAt, expiresAt, deliveredConnections?, lastStatus?, offlineReason?, attempts?}`, hoặc `404`. Rate limit `limits.status-rate` (100/s mỗi tenant mỗi pod).

---

## 5. Xử lý lỗi

| Sự cố | Hành vi |
|---|---|
| Kafka chậm hoặc sập | Produce timeout 5 s → `503` / `CODE_UNAVAILABLE`, `retry_after_ms=500`; idempotency `pending` bị xoá. `/ready` vẫn UP |
| Redis state chậm hoặc sập | `INCR` / idempotency timeout 200 ms → `503`. EPHEMERAL không idempotency vẫn chạy; ghi trạng thái lỗi chỉ log và đếm. Không cấp seq trong bộ nhớ |
| JWKS không lấy được | Dùng key đã cache (làm mới 5 phút; `kid` lạ → thử ngay, tối đa 1 lần/10 s). Chưa từng có JWKS → `/ready` 503 |
| `gw.control` chưa đọc xong lúc khởi động | `/ready` 503 |
| Token hết hạn, sai `aud`/`iss`, sai chữ ký | `401` + `WWW-Authenticate: Bearer error="invalid_token"` |
| Token bị thu hồi, thiếu scope, tenant bị chặn | `403` |
| JSON sai, quá lớn, vi phạm V1–V14 | `400` / `413`, `application/problem+json` |
| Record dựng ra sai luật §1.1 | `500`, log error, không produce |
| SIGTERM | `/ready` 503; dừng nhận request; chờ request đang chạy tối đa 10 s; đóng stream gRPC (gateway nối lại pod khác); flush producer |

---

## 6. Cấu hình và profile

```yaml
push-api:
  cell: c1
  kafka: { bootstrap: "kafka:9092", produce-timeout: 5s, status-group: push-api-status }
  redis: { uri: "redis://redis-state:6379", topology: cluster, timeout: 200ms }
  auth: { jwks-uri: "https://auth.pangate/.well-known/jwks.json", issuer: "https://auth.pangate", audience: pangate-api }
  limits: { push-rate: 1000, push-burst: 2000, broadcast-rate: 10, status-rate: 100 }
  ttl: { default: 86400s, max: 604800s }
  idempotency: { ttl: 1h, pending-ttl: 30s, wait: 2s }
  seqepochs: { refresh: 5s }
  grpc:
    port: 9443
    plaintext: false
    max-in-flight-per-stream: 1000
    allowed-spiffe: [spiffe://pangate/cell-1/gateway, spiffe://pangate/cell-1/push-router]
    tls: { cert: push-api.crt, key: push-api.key, trust: spiffe-bundle.pem }
  http: { max-body: 128KB, max-batch: 100, max-batch-body: 1MB }
server: { port: 8080 }
management: { server: { port: 8081 } }
```

| Profile | Kafka / Redis | JWKS / issuer | gRPC | HTTP / gRPC / quản trị |
|---|---|---|---|---|
| `local` | `localhost:9092` / `redis://localhost:6379` standalone | auth local `http://localhost:8090` | plaintext | 8085 / 9443 / 8084 |
| `dev` | `kafka:29092` / `redis://redis:6379` standalone | `http://auth:8090` | plaintext | 8080 / 9443 / 8081 (host 8085 / 9443 / 8084) |
| `prod` | Theo môi trường, Redis Cluster | `https://auth.pangate` | mTLS + allowlist | 8080 / 9443 / 8081 |

Issuer của `local`/`dev` lấy đúng giá trị `iss` mà auth ở profile đó phát hành.

**Metrics:**
- `pushapi.requests{surface=http|grpc, kind, code}`
- `pushapi.accept.latency{kind}`
- `pushapi.produce{result}`
- `pushapi.idempotency{result=new|duplicate|pending_wait|pending_timeout}`
- `pushapi.ratelimited{kind}`
- `pushapi.seq.latency`
- `pushapi.seqepoch.bumps{reason=failover|admin|regression}`
- `pushapi.seqepoch.cache_age_seconds`
- `pushapi.control.records{key_type, result}`
- `pushapi.revocations.active{kind}`
- `pushapi.status.updates{topic, result}`
- `pushapi.status.lag`

---

## 7. Thay đổi ngoài module

| Module | Thay đổi |
|---|---|
| `platform-contracts` | `TenantLimits` thêm `push_rate = 8`, `push_burst = 9`, `broadcast_rate = 10` (additive) |
| `gateway-service` | Chỉ cấu hình: `application-dev.yml` `push-api.target: dns:///push-api:9443` |
| `gateway-devtools` | `smoke` thêm `PUSH via API` (token API từ auth bằng `t1-backend`, `POST /v1/push`, client nhận và ACK) và `STATUS delivered` (`GET /v1/messages/{id}`); KICK đi qua Push API thật. `push`/`fake-pushapi` giữ làm công cụ dev |
| `platform-devenv` | Service `push-api` (profile `app`; phụ thuộc kafka-init, redis, auth healthy); `fake-pushapi` chuyển sang profile `tools`; gateway phụ thuộc `push-api`; `make images` build push-api; `make push-api-local`; MODULES thêm `push-api-service` sau `push-router-service` |
| `auth-service` | Không đổi (seed `t1-backend` đã có `push:send push:broadcast`) |

---

## 8. Kiểm thử

| Tầng | Nội dung |
|---|---|
| Unit (lõi, không Spring) | `PushRequests`: mọi ca của `PushRequestCases`, gồm V9. `HttpMapping` theo bảng §3.2. `RecordBuilder`: mọi record qua `PushRecordRules`, `Frame` đúng ADR 0017, `expires_at`. `Callers`/scope. `RateLimiter`. `TenantPolicies` gộp fleet ← cell ← tenant. `Idempotency`: done / pending / chờ / `:last` dùng lại `msgId`+seq / produce lỗi xoá key. `SeqAllocator`: epoch theo slot (`SeqKeys`), seq lùi. Thu hồi: jti, client theo `iat`, tombstone, hết hạn |
| Integration (Testcontainers Kafka + Redis, JWKS tĩnh) | `POST /v1/push` → `PushRecord` đúng key, header, luật (`PushPathScenario`). Hai request cùng `Idempotency-Key` song song → một record, một `duplicate=true`. Batch 100 mục, thứ tự theo principal. `SendStream`: KICK → `PushRecord{CONTROL}`; response không theo thứ tự ghép đúng `request_id`. `gw.control`: revoke jti/client → `403` trong ≤ 1 s; `tenant_blocked`; `broadcast_enabled=false`; rate từ `TenantCfg`. Readiness chờ replay. `StatusListener`: offline/delivered, `delivered` thắng `offline`. `SeqEpochWatcher`: khởi động lại container Redis → bump đúng một lần với hai watcher song song |
| End-to-end `PushApiIT` | Push API + router + gateway thật trong một JVM: token API → `POST /v1/push` → client WebSocket nhận PUSH, ACK → `GET` = `delivered` (receipt). Principal offline → `offline NO_SESSION`. Hai phiên single-session → KICK của gateway qua Push API → phiên cũ đóng 4012 |
| Devenv | `make images up smoke down`: `PUSH via API`, `STATUS delivered`, `OFFLINE`, revoke 4003; KICK qua Push API |
| Đo | 10 000 push qua `POST /v1/push:batch` (100 mục) tới 1 000 phiên: thông lượng accept, p50/p99 của `pushapi.accept.latency` |

---

## 9. ADR mới

| ADR | Quyết định |
|---|---|
| 0022 | Push API là một service; một lõi `PushAcceptor` dùng chung cho HTTP và gRPC |
| 0023 | Idempotency `pending` mang sẵn `msgId`/seq (kèm `:last`) để retry sau crash dùng lại đúng giá trị |
| 0024 | Phát hiện failover Redis state qua `run_id`; bump `seqepochs` bằng script CAS trên `runids{seqepochs}` |
| 0025 | Trạng thái message trong Redis, TTL theo message; `delivered` chỉ có khi `receipt=true` |
| 0026 | Rate limit push theo tenant là token bucket mỗi pod, cấu hình qua `TenantLimits` (additive) |

---

## 10. Milestone P1 và tiêu chí hoàn thành

1. Unit, IT và `PushApiIT` xanh; `make verify` xanh.
2. `buf breaking` xanh với `TenantLimits` mở rộng; test cũ của gateway, router và auth không đổi.
3. Chạy được ở `local` (`make push-api-local`) và `dev` (compose).
4. `make smoke` xanh với `PUSH via API`, `STATUS delivered`, `OFFLINE`, revoke 4003; KICK đi qua Push API thật.
5. Kill switch và thu hồi token có hiệu lực trong ≤ 1 s sau khi sự kiện lên `gw.control`.
6. ADR 0022–0026, README, CHANGELOG, `release-check` xanh, số đo ghi vào spec.

---

## 11. Câu hỏi còn mở

1. Rate theo pod là xấp xỉ: tổng thực tế = rate × số pod. Khi số pod thay đổi nhiều, có cần chia rate theo số pod đang sống (đọc từ Redis) không? P1: không.
2. `GET /v1/messages/{id}` cho BROADCAST chỉ có `accepted`. Có cần đếm số node đã nhận không? Cần router ghi thêm kết quả; để Phase 2.
