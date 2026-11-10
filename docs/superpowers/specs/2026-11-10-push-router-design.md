# push-router-service — Design Spec

- **Ngày:** 2026-11-10
- **Trạng thái:** Implemented (R1, plan `docs/superpowers/plans/2026-11-10-push-router-r1.md`). Đo trên máy dev: 10 000 EPHEMERAL tới 1 000 phiên, giao đủ 10 000; ≈ 2,5–4 k record/s (JVM lạnh); `router.delivery.latency` p50 1,2–1,7 s, p99 1,8–2,3 s khi bắn một loạt (chủ yếu là thời gian xếp hàng)
- **Nguồn kiến trúc:** *Netty Gateway — 10M Active Connections* ("tài liệu gốc"); số mục trong ngoặc, ví dụ (§6), là mục của tài liệu gốc.
- **Các spec liên quan:**
  - "spec contracts": `docs/superpowers/specs/2026-11-10-push-contracts-design.md` (C1). Spec này dựa trên nó, đặc biệt là bảng kết quả giao ở §1.1a.
  - "spec gateway": `docs/superpowers/specs/2026-11-03-gateway-service-design.md`.
- **Vị trí trong lộ trình:** sub-project 3/4: (1) auth (A1), (2) contracts (C1), **(3) push-router**, (4) push-api.

---

## 0. Mục tiêu, phạm vi

**Mục tiêu.** Hiện thực Push Router của một cell. Router làm các việc sau:
- tiêu thụ `cN.push.requests`;
- phân giải target qua registry v2;
- giao tới gateway qua gRPC `PushStream`;
- áp dụng §1.1a của spec contracts: retry, offline, receipt;
- commit offset an toàn;
- phát `NODE_DOWN`.

Devenv đi đúng luồng thật Kafka → router → gateway trước khi có Push API.

**Quyết định đã chốt (brainstorm 2026-11-10):**

| # | Câu hỏi | Quyết định |
|---|---|---|
| R1 | Router tìm địa chỉ gRPC của gateway thế nào | Gateway ghi `node:addr:{id}` = `host:port`, cùng TTL với lease |
| R2 | Chạy đầu-cuối khi chưa có Push API | `devtools push` và `fake-pushapi` produce `PushRecord` lên Kafka; `make smoke` đi qua router |
| R3 | Nền tảng | Spring Boot làm vỏ (cấu hình, actuator, starter); lõi Java thuần; Kafka qua **Confluent Parallel Consumer** |
| R4 | Mô hình và thứ tự của Parallel Consumer | Imperative (`poll`), worker là **virtual threads** (override `setupWorkerPool`), `ProcessingOrder.KEY` |

**Ngoài phạm vi:**
- Push API thật, Offline Delivery, Inbox;
- mTLS trong devenv;
- autoscale;
- đạt ngưỡng 125k msg/s (chỉ đo để ghi lại số liệu).

**Bất biến:**
1. **Không mất record.** Một record chỉ được tính là xong (offset được commit) khi đã giao xong, hoặc khi `OfflineRecord`/`DeliveredRecord` của nó đã được Kafka ack.
2. **Thứ tự trong principal.** Cùng key thì tuần tự, vì `KEY` cho phép mỗi key chỉ một record đang bay.
3. **Router không đọc payload.** Router chỉ bọc `PushRecord.frame` vào `PushItem` (ADR 0017).

---

## 1. Kiến trúc

Module mới `push-router-service/`, package `com.pangate.router`.
- **Parent:** `platform-parent` 0.1.0.
- **Phụ thuộc:** `platform-contracts`, `platform-spring-starter`, `parallel-consumer-core` 0.5.3.3 (ghim version), `grpc-netty-shaded`, `lettuce-core`, `kafka-clients`.
- **Test:** `platform-contracts-testkit` và `gateway-service` (jar thường, để chạy gateway thật trong IT).
- **Image:** Jib distroless Java 21.

```
Kafka cN.push.requests ──▶ RouterConsumer (PC, KEY, virtual threads, maxConcurrency 20 000)
                              │ PushRecord → DeliveryJob.run()  (chặn tới khi record xong)
        ┌─────────────────────┼──────────────────────────┐
        ▼                     ▼                          ▼
  RegistryLookup         NodeDirectory              GatewayLinks
  reg:{p} → entry sống   nodes:live, node:{id},     1 stream PushStream / node,
  cache 2 s              node:addr:{id}, draining   batch ≤ 256 / 2 ms, ACK theo delivery_id
        └─────────────────────┼──────────────────────────┘
                              ▼
          OutcomeRecorder ──▶ cN.push.offline, cN.push.delivered
          NodeDownWatcher ──▶ cN.gw.lifecycle (NODE_DOWN)
```

| Lớp | Trách nhiệm |
|---|---|
| `RouterConsumer` | Bọc `VirtualThreadParallelConsumer extends ParallelEoSStreamProcessor`, có override `setupWorkerPool(int)` trả `ThreadPoolExecutor` dùng `Thread.ofVirtual().factory()`. Options: `ordering = KEY`, `maxConcurrency`, `commitMode = PERIODIC_CONSUMER_ASYNCHRONOUS`. Parse `PushRecord`; record hỏng thì bỏ qua (§4) |
| `DeliveryJob` | §1.1a của spec contracts cho một record (§2) |
| `RegistryLookup` | `HGETALL reg:{p}`, timeout 200 ms; parse theo `RegistryValues` (v1/v2); lọc entry có epoch bằng epoch hiện tại của node trong `NodeDirectory`; cache 2 s; `invalidate(principal)` khi retry |
| `NodeDirectory` | `Map<nodeId, Node(epoch, address, draining)>`, làm mới mỗi `directory-refresh` |
| `GatewayLinks` | `CompletableFuture<AckItem.Status> send(String nodeId, PushItem)`; mỗi node một channel và một stream; batch, flow control, ghép ACK |
| `OutcomeRecorder` | Produce `OfflineRecord`/`DeliveredRecord` với producer idempotent, `acks=all`, zstd, key theo `PushKeys`, header theo `PushHeaders`; chặn tới khi Kafka ack |
| `NodeDownWatcher` | Phát `NODE_DOWN` khi một node đã thấy sống mất lease hoặc đổi epoch |

Lõi (`DeliveryJob`, `RegistryLookup`, `NodeDirectory`, `GatewayLinks`, `OutcomeRecorder`, `NodeDownWatcher`) không import Spring và test được không cần Spring.

---

## 2. `DeliveryJob`

Đầu vào: một `PushRecord` hợp lệ. Đầu ra: return (record xong) hoặc ném lỗi tạm thời (PC retry record với backoff 100 ms → 5 s).

**MESSAGE** (target là principal hoặc device):

1. **Hết hạn.** Nếu `now > expires_at_ms`: RELIABLE/ORDERED → `OfflineRecord{EXPIRED, attempts 0}`; EPHEMERAL → bỏ (`router.records{result=dropped}`).
2. **Tìm kết nối.** `RegistryLookup.live(principal)`; với target device thì giữ entry thoả `RegistryValues.matchesDevice`. Bỏ entry của node đang drain.
3. **Không có kết nối:** RELIABLE/ORDERED → `OfflineRecord{NO_SESSION, attempts 0}`; EPHEMERAL → bỏ.
4. **Gửi.** Với mỗi kết nối: `PushItem{KIND_MESSAGE, delivery_id mới, tenant_id, principal_id, conn_id, frame, priority, enqueued_at_ms = accepted_at_ms}` qua `GatewayLinks.send`, song song.
5. **EPHEMERAL:** xong khi mọi `send` đã được nhận vào stream (không chờ ACK).
6. **RELIABLE/ORDERED:** chờ kết quả từng kết nối, mỗi lần chờ tối đa `ack-timeout` (6 s; quá thì coi như `NACK_TIMEOUT`):
   - `STATUS_ACKED` / `STATUS_WRITTEN`: kết nối đã giao. Nếu có `receipt` → `DeliveredRecord` với đúng status đó, key `sender_principal_id`.
   - `NACK_TIMEOUT`, `NACK_CLOSED`, `NACK_NOT_FOUND`, stream đứt: đưa vào vòng retry.
   - `NACK_OVERFLOW`, `NACK_SLOW_CONSUMER`, `NACK_SHED`: kết nối thất bại, không retry.
7. **Retry** (tối đa 2 lần, sau `retries[0]` = 100 ms và `retries[1]` = 400 ms): `invalidate`, tra lại registry, gửi tới các kết nối còn sống chưa giao (kết nối mới của cùng principal hoặc device cũng tính). Không còn kết nối thì lý do cuối là `NO_SESSION`, hoặc `NODE_GONE` nếu lần thất bại trước là do stream tới node đã chết đứt.
8. **Kết thúc:**
   - Có ít nhất một kết nối đã giao → xong.
   - Không có → `OfflineRecord{reason của lần thử cuối, attempts = tổng PushItem đã gửi}`.

**BROADCAST:** `PushItem{KIND_BROADCAST, topic, frame, priority}` tới mọi node sống, không drain. Xong khi mọi `send` đã được nhận vào stream. Node đứt trong lúc gửi thì bỏ qua (`router.broadcast.skipped`). Không có offline hay receipt.

**CONTROL:** `PushItem{KIND_CONTROL, tenant_id, principal_id, control}` tới mỗi node có entry sống của principal (mỗi node một lần, kể cả node đang drain). Không có entry thì xong ngay. Không có offline hay receipt. **KICK** (single-session) đi tới mọi node sống: khi gateway gửi KICK, nó đã thay entry cũ bằng entry mới trong `reg:{p}`, nên không định tuyến theo entry được (sửa trong P1 của push-api). Router không chờ ack của KICK: ack không đổi kết quả gì, còn chờ thì partition phải đợi node chậm nhất; mỗi link vẫn ghi KICK theo thứ tự.

**Lỗi tạm** (Redis timeout, produce offline/delivered lỗi): ném lỗi để PC retry record. Phần đã giao có thể được giao lại; gateway và client dedup theo `msg_id`.

---

## 3. Gateway, danh bạ node, link gRPC, NODE_DOWN

### 3.1 Thay đổi ở gateway (additive)

- `NodeIdentity` ghi `node:addr:{nodeId}` = `<advertise-host>:<grpc.port>`, với `SET … EX 15` ở mọi lần ghi và làm mới lease. Xoá khoá khi dừng.
- Cấu hình mới `gateway.grpc.advertise-host`:
  - mặc định `${HOSTNAME:localhost}`;
  - profile `dev`: `gateway`;
  - profile `local`: `127.0.0.1`.
- `RegistryKeys.addr(nodeId)` = `"node:addr:{" + nodeId + "}"`.
- Test cũ không sửa. Test mới: khoá được ghi, được làm mới, và bị xoá khi dừng.

### 3.2 `NodeDirectory`

Mỗi `directory-refresh` (1 s):
1. `ZRANGEBYSCORE nodes:live (now−30 000) +inf`.
2. Pipeline `GET node:{id}`, `GET node:addr:{id}`, `EXISTS node:draining:{id}`.

Node sống khi có epoch và địa chỉ. Kết quả là ảnh chụp bất biến, thay nguyên khối. Làm mới lỗi thì giữ ảnh cũ. Không làm mới được quá 10 s thì `/ready` → 503.

### 3.3 `GatewayLinks`

- **Channel:** mỗi node một `ManagedChannel` (`NettyChannelBuilder`), keepalive 10 s, header `pangate-push-version: 1`.
- **Stream:** `PushStreamGrpc.newStub(channel).stream(responseObserver)`, mở khi lần đầu cần.
- **Gửi:** item vào hàng đợi của link; một tác vụ gửi `PushBatch` khi đủ `batch.max` (256) hoặc sau `batch.linger` (2 ms), chỉ khi `isReady()`.
- **Giới hạn:** tối đa `max-in-flight-per-node` (10 000) item đang chờ ACK; đầy thì `send` chặn (virtual thread).
- **Ghép ACK:** `delivery_id` là `AtomicLong` của router; map `delivery_id → future`; mỗi `AckItem` hoàn tất future tương ứng; với BROADCAST/CONTROL/EPHEMERAL, future hoàn tất khi item đã được ghi vào stream.
- **Stream lỗi / `onCompleted` (gateway drain):** mọi future đang chờ → `NACK_CLOSED`; kết nối lại với backoff 100 ms → 5 s, có jitter.
- **Epoch đổi hoặc node biến khỏi `NodeDirectory`:** đóng link; future → `NACK_CLOSED`.
- **TLS:** `router.grpc.plaintext=true` cho local/dev; prod dùng mTLS với `router.grpc.tls.{cert,key,trust}` lấy qua `SecretSource`.

### 3.4 `NodeDownWatcher`

So ảnh chụp mới với ảnh cũ. Một node có trong ảnh cũ mà không còn trong ảnh mới (mất lease) hoặc đã đổi epoch sẽ sinh `ConnectionEvent{EVENT_TYPE_NODE_DOWN, node_id, epoch cũ, cell_id, at_ms, event_id ULID}`:
- đích `cN.gw.lifecycle`;
- key `node_id`;
- header có `tenant-id` rỗng.

Mỗi router chỉ phát một lần cho mỗi `node_id:epoch`. Router mới khởi động không phát cho những node chưa từng thấy.

---

## 4. Xử lý lỗi

| Sự cố | Hành vi |
|---|---|
| Redis registry chậm hoặc sập | Lookup timeout 200 ms → lỗi tạm → PC retry record; không đánh dấu offline. `/ready` 503 sau 10 s không làm mới được danh bạ |
| Kafka sập | PC kết nối lại; record đang bay vẫn chờ ACK; produce outcome lỗi → record chưa xong → xử lý lại |
| Gateway chết | Future → `NACK_CLOSED` → retry tra lại registry → giao được nếu phiên đã chuyển node, không thì offline `NODE_GONE`/`NO_SESSION`; `NODE_DOWN` được phát |
| Gateway quá tải | `NACK_OVERFLOW` / `SHED` / `SLOW_CONSUMER` → không retry (§1.1a) |
| Record hỏng (parse lỗi hoặc trái `PushRecordRules`) | Bỏ qua và tính là xong; metric `router.records{result=invalid}`; log `topic`, `partition`, `offset`, không log payload |
| Rebalance, restart | `CooperativeStickyAssignor`, `group.instance.id` = hostname, `session.timeout.ms` 30 s; PC commit offset những record đã xong; record chưa xong được xử lý lại |
| SIGTERM | Dừng nhận record mới; chờ record đang bay tối đa 10 s; commit; đóng link |

---

## 5. Cấu hình, metrics, profile

```yaml
router:
  cell: c1
  kafka: { bootstrap: "kafka:9092", group: push-router, max-concurrency: 20000 }
  ack-timeout: 6s
  retries: [100ms, 400ms]
  registry: { uri: "redis://redis-registry:6379", topology: cluster, cache: 2s, timeout: 200ms }
  directory-refresh: 1s
  batch: { max: 256, linger: 2ms }
  max-in-flight-per-node: 10000
  grpc: { plaintext: false, tls: { cert: router.crt, key: router.key, trust: spiffe-bundle.pem } }
```

| Profile | Kafka / Redis | gRPC | Cổng quản trị |
|---|---|---|---|
| `local` | `localhost:9092` / `redis://localhost:6379` standalone | plaintext | 8083 |
| `dev` | `kafka:29092` / `redis://redis:6379` standalone | plaintext | 8081 (trong container) |
| `prod` | Theo môi trường | mTLS | 8081 |

**Metrics:**
- `router.records{kind,result}`, với result ∈ delivered / offline / dropped / invalid;
- `router.delivery.latency{class}`;
- `router.offline{reason}`;
- `router.inflight`;
- `router.links{state}`;
- `router.registry.latency`;
- `router.retries`;
- `router.node_down`;
- `router.broadcast.skipped`;
- metric của Parallel Consumer (lag, số record đang xử lý).

---

## 6. Devenv và devtools

| Nơi | Thay đổi |
|---|---|
| `platform-devenv/docker-compose.yml` | Service `router` (profile `app`, image `pangate/push-router-service:dev`, phụ thuộc kafka-init, redis, gateway healthy) |
| `gateway-devtools` | Lệnh `push --bootstrap --cell --principal [--device] [--topic] [--class] [--receipt] [--payload]`: dựng `PushRecord` (msg_id ULID, `seq` đọc rồi `INCR` `seq:{p}` cho RELIABLE/ORDERED tới principal), kiểm bằng `PushRecordRules`, produce theo `PushKeys`/`PushHeaders`. Lệnh `await-offline --msg-id` đọc `c1.push.offline` |
| `fake-pushapi` | KICK nhận qua gRPC `SendStream` → `PushRecord{CONTROL, kick}` produce lên `c1.push.requests`; không gọi gateway trực tiếp nữa |
| `SmokeCommand` | Thêm bước `PUSH via Kafka` (client nhận PUSH, ACK) và `OFFLINE` (principal không online → `OfflineRecord NO_SESSION`); bước push qua `fake-router` giữ lại cho chế độ không có router (`--no-router`) |
| `Makefile` | `MODULES` thêm `push-router-service` sau `gateway-service`; `images` build router; `router-local` |

---

## 7. Kiểm thử

| Mức | Nội dung |
|---|---|
| Unit | `DeliveryJob` với lookup, link và đồng hồ giả, phủ hết các dòng §1.1a (hết hạn; không phiên; ACKED/WRITTEN và receipt; TIMEOUT → 2 retry → offline với `attempts` 3; CLOSED → tra lại → giao ở node khác; OVERFLOW không retry; fan-out 2 kết nối, 1 giao → không offline; target device; EPHEMERAL; BROADCAST; CONTROL; lỗi Redis → ném lỗi). `RegistryLookup` chạy hết `RegistryValues.cases()` và lọc epoch. `NodeDirectory`/`NodeDownWatcher` (mất lease, đổi epoch, drain, làm mới lỗi). `GatewayLinks` (batch 256 / 2 ms, ghép ACK, stream đứt → `NACK_CLOSED`, max-in-flight chặn). Worker thread của PC là virtual |
| IT (Testcontainers Kafka + Redis, gateway thật trong JVM test, client devtools) | Client nhận PUSH và ACK → `DeliveredRecord` khi có receipt; principal không online → `OfflineRecord NO_SESSION`; client không ACK → `NACK_TIMEOUT` → offline với `attempts` 3; target device chỉ tới đúng device; BROADCAST tới subscriber; CONTROL KICK → 4012; dừng gateway → `NODE_DOWN`; restart router giữa chừng → record được giao lại (client thấy trùng `msg_id`); 20 MESSAGE của một principal tới đúng thứ tự. Mọi record đều qua `PushPathScenario` (kiểm key và header) |
| Gateway | `node:addr:{id}` được ghi, làm mới và xoá |
| E2E | `make smoke` có hai bước mới, KICK đi qua Kafka và router |
| Đo | Một lần đo trên máy dev (N principal, M msg) ghi vào spec; không đặt ngưỡng |

---

## 8. ADR mới

| ADR | Quyết định |
|---|---|
| 0019 | Router dùng Confluent Parallel Consumer, `KEY`, imperative trên virtual threads (override `setupWorkerPool`, ghim 0.5.3.3) |
| 0020 | Gateway công bố `node:addr:{id}` để router tìm địa chỉ |
| 0021 | Devenv dùng Kafka làm đầu vào cho `fake-pushapi` và `devtools push`, thay cho Push API cho tới ④ |

---

## 9. Milestone R1 và tiêu chí hoàn thành

1. Unit và IT của router xanh; `make verify` xanh trên mọi module.
2. Gateway ghi `node:addr:{id}`; test cũ không phải sửa.
3. Router chạy được ở `local` (`make router-local`) và trong compose `dev`.
4. `make smoke` xanh với `PUSH via Kafka` và `OFFLINE`. KICK đi qua Kafka và router được chứng minh bằng `RouterIT` (gateway thật) và test của `fake-pushapi` → Kafka.
5. IT chứng minh: restart router không mất message; dừng gateway sinh `NODE_DOWN`.
6. Đã viết ADR 0019–0021; `make release-check` xanh; CI xanh.

---

## 10. Câu hỏi còn mở

1. Thông lượng mỗi principal bị giới hạn bởi `KEY` (một record đang bay mỗi principal). Nếu số đo thực tế cho thấy không đủ, chuyển sang `UNORDERED` cộng hàng đợi theo principal cho ORDERED/CONTROL. Contract không phải đổi.
2. `setupWorkerPool` là API `protected` trong package `internal` của Parallel Consumer. Mỗi lần nâng version phải chạy lại test "worker là virtual thread".
