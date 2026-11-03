# Gateway M2a (Cluster: infra, registry, upstream, control) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Đưa gateway M1 lên chạy cluster với Redis và Kafka. Cụ thể: node lease theo epoch; registry `reg:{p}`; `seq` khi HELLO; upstream và lifecycle lên Kafka; consumer `gw.control` (revoke, cellmap, tenantcfg). Cả local (infra trên `localhost`, gateway chạy từ IDE) và dev (toàn bộ trong compose) đều phải chạy được.

**Architecture:** Giữ mô hình shared-nothing của M1. Mọi I/O tới Redis (Lettuce async) và Kafka (`kafka-clients`, `max.block.ms=0`) đều bất đồng bộ; callback quay về owner loop qua `eventLoop().execute`. `InfraLifecycle` (phase `MAX − 1900`) kết nối dependency với retry có hạn. `NodeIdentity` (`MAX − 1800`) giữ lease. Cập nhật session sang registry và lifecycle đi qua `SessionListener` gắn vào `SessionTracker`. State control là các snapshot copy-on-write, được đọc trên hot path.

**Tech Stack:** Như M1, thêm Lettuce `6.6.0.RELEASE` và `kafka-clients 3.9.2` (cả hai do Spring Boot 3.5.16 quản lý version), Testcontainers `1.21.4` (`junit-jupiter`, `kafka`), image `redis:7.4.2` và `apache/kafka:3.9.1`.

**Spec:** `docs/superpowers/specs/2026-11-03-gateway-service-design.md` (mục `§x.y`). Plan này hiện thực nửa đầu của **M2** (spec §12). Phần còn lại, gọi là M2b, có plan riêng: gRPC `PushStreamService`, `AckBatcher`, `PendingAckWindow`, `TopicIndex`/SUBSCRIBE, `BroadcastPacer`, control push, `KickClient` cùng single-session, `fake-router`/`fake-pushapi` và smoke push → ack.

## Global Constraints

- Mọi ràng buộc trong "Global Constraints" của plan M1 (`docs/superpowers/plans/2026-11-03-gateway-m1-single-node.md`) vẫn giữ nguyên.
- Version: `lettuce-core` và `kafka-clients` lấy theo BOM của Spring Boot 3.5.16 (`6.6.0.RELEASE`, `3.9.2`); Testcontainers theo BOM (`1.21.4`). Không pin lại trong `platform-parent`, và không thêm `spring-data-redis` hay `spring-kafka`.
- Netty vẫn là `4.2.20.Final` cho mọi module, kể cả Netty mà Lettuce kéo vào. Enforcer `bannedDependencies` vẫn cấm `io.projectreactor.netty:*`; `reactor-core` (do Lettuce kéo vào) được phép.
- Key Redis (spec §6.1–§6.7), viết đúng có dấu ngoặc nhọn làm hash tag: `node:epoch:{<nodeId>}`, `node:{<nodeId>}`, `nodes:live`, `reg:{<principalId>}`, `seq:{<principalId>}`, `seqepochs`. Mọi script Lua chỉ đụng **một** key.
- Lease: TTL 15 s, renew mỗi 5 s, quy tắc "epoch lớn hơn thắng" (script §6.1 nguyên văn). Script trả `-1` → fence → `ProcessExit.exit(2, …)`.
- Giá trị registry: `"<nodeId>|<epoch>|<connectedAtMs>|<platform>"`. Retry queue tối đa 10 000 entry, delay 100 ms → 1 s → 5 s rồi bỏ.
- Lettuce: một `DefaultClientResources` dùng chung (`ioThreadPoolSize=2`, `computationThreadPoolSize=2`); `TimeoutOptions` cố định `gateway.redis.command-timeout` (2 s); `requestQueueSize=10000`; `disconnectedBehavior=REJECT_COMMANDS` (khi mất kết nối, lệnh lỗi ngay chứ không xếp hàng trên heap). Topology `cluster` dùng `RedisClusterClient` với periodic refresh 30 s và adaptive refresh.
- Kafka producer (spec §5.5): `reliable` (`acks=all`, `enable.idempotence=true`) và `telemetry` (`acks=1`, `enable.idempotence=false`), mỗi cái `buffer.memory=128MB`. Cấu hình chung: `linger.ms=5`, `batch.size=65536`, `compression.type=zstd`, `max.block.ms=0`, **`request.timeout.ms=5000`, `delivery.timeout.ms=10000`** (quyết định của plan: client `cmd` nhận `UNAVAILABLE` trong ≤ 10 s thay vì 120 s mặc định). Key là `String`, value là `byte[]` (protobuf).
- Topic (spec §2.4): `<prefix>.gw.lifecycle` (32 partition), `<prefix>.gw.upstream.<type>` (64) cho từng type trong `gateway.kafka.upstream-types`, và `gw.control` (8, `cleanup.policy=compact`). `prefix` mặc định là `c1`. Type `cmd` và `cmd_result` đi producer `reliable`; các type khác đi `telemetry`.
- Consumer `gw.control`: `group.id=gw-<nodeId>`, `enable.auto.commit=false`, `assign` toàn bộ partition, `seekToBeginning`, không commit. Value deserializer là `byte[]`; record lỗi thì bỏ qua và đếm.
- Mọi callback của Lettuce hoặc Kafka mà động tới `Session` đều phải `session.loop().execute(...)`. Không có lời gọi blocking trên `gw-io-*`. BlockHound chỉ được whitelist `org.apache.kafka.clients.producer.KafkaProducer#send`, với lý do: `max.block.ms=0` nên chỉ giữ lock accumulator trong thời gian rất ngắn. Lettuce dispatch chỉ được thêm vào whitelist nếu test chứng minh nó bị BlockHound bắt, và phải ghi rõ lý do.
- Hằng số control cố định: revoke `principal` → đóng 4003; `tenant.blocked` → HELLO 4009 với `retry_after_ms=300000`, session đang mở → 4013 với `retryAfter=300000`; migrate cell → `RECONNECT{re_bootstrap=true, delay_ms ∈ [0,5000]}` cho 2% session của tenant mỗi 5 s, rồi đóng 1001 sau `delay + 30 s`.
- Mọi test cần Docker (Redis/Kafka thật) đặt trong `src/it/java` với tên `*IT.java`, dùng fixture chung `InfraContainers`. Unit test (`src/test/java`) không cần Docker.
- Metric mới dùng tên Micrometer dạng chấm, giống M1 (`gw.handshakes`). Danh sách cụ thể nằm trong từng task.
- Commit sau mỗi task theo Conventional Commits. Trước commit chạy `node .gitnexus/run.cjs detect-changes --scope staged --repo .`; trước khi sửa một symbol đã có, chạy `node .gitnexus/run.cjs impact <Symbol> --direction upstream --repo .` (CLAUDE.md).

## Review Focus

1. **Redis chết giữa chừng (không phải lúc boot).** HELLO mới vẫn thành công; `CONNECTED` đến trong khoảng 200 ms với `seq_unknown=true`, không bị treo. Ghi registry đi vào retry queue, session đang mở không bị ảnh hưởng. Test: Task 11 `connectedSentWithSeqUnknownWhenSeqReadTimesOut`; Task 16 `redisPauseKeepsSessionsAndReEpochsOnRecovery` (HELLO trong lúc pause vẫn được `CONNECTED`).
2. **Kafka từ chối hoặc chậm.** `cmd` nhận `ERROR{4500,"UNAVAILABLE",retryable=true}`, kết nối vẫn mở, loop không bị block. Test: Task 12 `cmdFailureAnswersUnavailableAndKeepsSession` (dùng `MockProducer.errorNext`); `sendThrowingSynchronouslyIsHandled`.
3. **Cùng principal reconnect nhanh** (đóng rồi HELLO lại ngay, hoặc hai thiết bị). Khi unregister kết nối cũ, entry của kết nối mới vẫn phải còn; một lần retry register muộn của session đã đóng không được ghi lại entry. Test: Task 10 `unregisterRemovesOnlyOwnConnId`, `retryOfClosedSessionIsSkipped`.
4. **`gw.control` có record hỏng, event lạ hoặc tombstone cho key chưa từng thấy.** Consumer bỏ qua record đó, tăng `gw.control.consumer.errors`, catch-up vẫn hoàn tất và `/ready` lên 200. Test: Task 9 `malformedAndUnknownRecordsAreSkippedAndCatchUpCompletes`.
5. **Restart cùng `nodeId` khi lease cũ còn sống** (R3; crash rồi start lại ngay). Process mới lấy epoch lớn hơn và không bị fence; nếu process cũ vẫn còn chạy, nó tự fence ở lần renew kế tiếp. Test: Task 7 `restartWithLiveLeaseTakesOverWithHigherEpoch`, `olderIncarnationFencesOnRenew`.

---

## File Structure

```
platform-contracts/
├── buf.yaml                                   # thêm except cho tên RPC/service theo spec
└── proto/pangate/v1/ router.proto lifecycle.proto control.proto upstream.proto push.proto
platform-spring-starter/src/main/java/com/pangate/starter/kafka/PangateHeaders.java
platform-devenv/docker-compose.yml             # + kafka, kafka-init, redis (nhóm mặc định)
Makefile                                       # + infra-up, infra-down; doctor kiểm Kafka/Redis
gateway-service/
├── pom.xml                                    # + lettuce-core, kafka-clients, testcontainers (test)
├── src/main/resources/application{,-local,-dev}.yml, META-INF/spring.factories
└── src/main/java/com/pangate/gateway/
    ├── config/     GatewayProperties (+Redis, Kafka, limits inbound/topics), GatewayLimits, ClusterConfig (bean mới)
    ├── diagnostics/ DependencyUnavailableException, MissingTopicsException, DependencyFailureAnalyzer
    ├── lifecycle/  ProcessExit, Retry, RedisConnections, KafkaClients, TopicNames, TopicProvisioner, InfraLifecycle
    ├── registry/   RegistryKeys, NodeIdentity, RetryQueue, RegistryWriter, SeqReader, RedisSeqReader, RegistryResync
    ├── control/    RevocationList, CellMapView, TenantPolicy, TenantLimitsStore, ControlKeys, ControlEventHandler,
    │               ControlState, ControlEventConsumer, ControlEnforcer, TenantMigrator
    ├── upstream/   LifecyclePublisher, UpstreamPublisher, SessionRateLimiter, UpstreamPressure
    ├── auth/       HelloPolicy, SdkVersion
    ├── session/    SessionListener (mới); Session, SessionState, SessionTracker (sửa)
    ├── pipeline/   HelloAuthHandler, SessionHandler, PipelineDeps (sửa)
    ├── protocol/   Frames (+upstreamAck, reconnect)
    └── server/     NodeState (+nodeId, epoch, leaseAlive, controlCaughtUp)
gateway-service/src/it/java/com/pangate/gateway/
    ├── it/ InfraContainers, GatewayLocalIT (sửa), GatewayClusterIT
    └── (component IT) lifecycle/InfraLifecycleIT, registry/{NodeIdentityIT,RegistryWriterIT,RedisSeqReaderIT},
        control/ControlEventConsumerIT
gateway-devtools/src/main/java/com/pangate/devtools/{client/GatewayClient, cmd/SmokeCommand}.java
.github/workflows/ci.yml
```

Bean mới được đăng ký trong `config/ClusterConfig.java` để `GatewayConfig` của M1 không phình thêm. `GatewayConfig` chỉ sửa ở các bean đã có (`SessionTracker`, `PipelineDeps`).

---

### Task 1: Contracts — `router`, `lifecycle`, `control`, `upstream`, `push`

**Files:**
- Create: `platform-contracts/proto/pangate/v1/{router,lifecycle,control,upstream,push}.proto`
- Modify: `platform-contracts/buf.yaml`
- Test: `platform-contracts/src/test/java/com/pangate/contracts/v1/ClusterContractsTest.java`

**Interfaces:**
- Produces (Java package `com.pangate.contracts.v1`, `java_multiple_files=true`, proto package `pangate.v1`). Theo buf `STANDARD`, giá trị enum có tiền tố tên enum và giá trị 0 là `*_UNSPECIFIED`. Đây là khác biệt có chủ đích so với spec §6.3 (ở đó `MESSAGE = 0`, `ACKED = 0`):
  - `router.proto`: `service PushStream { rpc Stream(stream PushBatch) returns (stream DeliveryAck); }`; `PushBatch`, `PushItem` (field và số thứ tự như spec §6.3; `enum Kind { KIND_UNSPECIFIED=0; KIND_MESSAGE=1; KIND_BROADCAST=2; KIND_CONTROL=3; }`; `enum Priority { PRIORITY_UNSPECIFIED=0 /* = NORMAL */; PRIORITY_NORMAL=1; PRIORITY_CRITICAL=2; PRIORITY_BULK=3; }`), `ControlOp`, `Topics`, `Kick`, `RefreshConfig`, `DeliveryAck`, `AckItem` (`enum Status { STATUS_UNSPECIFIED=0; STATUS_ACKED=1; STATUS_WRITTEN=2; STATUS_NACK_TIMEOUT=3; STATUS_NACK_OVERFLOW=4; STATUS_NACK_CLOSED=5; STATUS_NACK_NOT_FOUND=6; STATUS_NACK_SLOW_CONSUMER=7; STATUS_NACK_SHED=8; }`). Import `pangate/v1/frame.proto`.
  - `lifecycle.proto`: `message ConnectionEvent { EventType type=1; string conn_id=2; string tenant_id=3; string principal_id=4; string device_id=5; string node_id=6; uint64 epoch=7; string cell_id=8; string platform=9; string sdk_version=10; uint32 close_code=11; int64 at_ms=12; }`, `enum EventType { EVENT_TYPE_UNSPECIFIED=0; EVENT_TYPE_CONNECTED=1; EVENT_TYPE_DISCONNECTED=2; EVENT_TYPE_HEARTBEAT_TIMEOUT=3; EVENT_TYPE_KICKED=4; }`.
  - `upstream.proto`: `message UpstreamRecord { string msg_id=1; string tenant_id=2; string principal_id=3; string device_id=4; string conn_id=5; string node_id=6; string cell_id=7; int64 received_at_ms=8; map<string,string> headers=9; bytes payload=10; }`.
  - `control.proto`, có comment ở đầu file định nghĩa contract key của topic `gw.control` (compacted; value rỗng là tombstone): `revoke:p:<principal>`, `revoke:d:<principal>/<device>`, `revoke:j:<jti>`, `cellmap:<tenant>`, `tenantcfg:fleet`, `tenantcfg:cell:<cell>`, `tenantcfg:tenant:<tenant>`.
    ```proto
    message ControlEvent { oneof event { Revoke revoke = 1; CellMap cell_map = 2; TenantCfg tenant_cfg = 3; } int64 issued_at_ms = 4; }
    message Revoke { oneof target { string principal_id = 1; DeviceRef device = 2; string jti = 3; } int64 expires_at_ms = 4; }
    message DeviceRef { string principal_id = 1; string device_id = 2; }
    message CellMap { string tenant_id = 1; string cell = 2; }
    enum Scope { SCOPE_UNSPECIFIED = 0; SCOPE_FLEET = 1; SCOPE_CELL = 2; SCOPE_TENANT = 3; }
    message TenantCfg { Scope scope = 1; string scope_id = 2; TenantLimits limits = 3; KillSwitches switches = 4; }
    message TenantLimits { optional uint32 max_conns = 1; optional uint32 inbound_rate = 2; optional uint32 inbound_burst = 3;
      optional uint32 topics_per_conn = 4; optional string min_sdk_version = 5; repeated string origin_allowlist = 6;
      optional bool single_session = 7; }
    message KillSwitches { optional uint32 heartbeat_interval_sec = 1; optional uint32 admission_rate_per_node = 2;
      optional bool broadcast_enabled = 3; optional bool delivery_downgrade_reliable = 4; optional bool upstream_telemetry_enabled = 5;
      optional bool tenant_blocked = 6; optional bool sse_fallback_enabled = 7; optional bool recycle_enabled = 8; }
    ```
    Revoke theo device mang cả `principal_id` vì `device_id` chỉ duy nhất trong phạm vi một principal. Đây là chi tiết hiện thực của spec §6.8.
  - `push.proto`: `service PushService { rpc SendStream(stream PushRequest) returns (stream PushResponse); }`; `message PushRequest { string request_id=1; Kind kind=2; string tenant_id=3; string principal_id=4; ControlOp control=5; enum Kind { KIND_UNSPECIFIED=0; KIND_CONTROL=1; } }`; `message PushResponse { string request_id=1; bool ok=2; string error=3; }`. Import `router.proto`.
- `buf.yaml`: `lint.except: [RPC_REQUEST_STANDARD_NAME, RPC_RESPONSE_STANDARD_NAME, RPC_REQUEST_RESPONSE_UNIQUE, SERVICE_SUFFIX]`, để giữ đúng tên `PushStream`, `PushBatch`, `DeliveryAck` của spec.

- [ ] **Step 1: Viết test thất bại**

```java
@Test void pushItemRoundTripsWithFrameAndControl() { /* KIND_CONTROL + ControlOp.kick(new_conn_id="c2", connected_at_ms=5) → parse lại bằng nhau */ }
@Test void connectionEventRoundTrips() { /* EVENT_TYPE_DISCONNECTED, close_code=4003, epoch=7 */ }
@Test void controlEventOptionalFieldsTrackPresence() {
  TenantCfg c = TenantCfg.newBuilder().setScope(Scope.SCOPE_TENANT).setScopeId("t1")
      .setSwitches(KillSwitches.newBuilder().setTenantBlocked(false)).build();
  TenantCfg p = TenantCfg.parseFrom(c.toByteArray());
  assertThat(p.getSwitches().hasTenantBlocked()).isTrue();
  assertThat(p.getSwitches().hasUpstreamTelemetryEnabled()).isFalse();
}
@Test void upstreamRecordKeepsHeadersAndPayload() { }
```

- [ ] **Step 2: Chạy để thấy thất bại**

Run: `./mvnw -f platform-contracts test`
Expected: lỗi compile (chưa có class).

- [ ] **Step 3: Viết 5 file proto và sửa `buf.yaml`** theo khối Interfaces.

- [ ] **Step 4: Chạy để thấy pass, cộng buf lint**

Run: `./mvnw -f platform-contracts install && docker run --rm -v "$PWD/platform-contracts:/w" -w /w bufbuild/buf:1.47.2 lint`
Expected: test pass; `buf lint` không in gì và exit 0.

- [ ] **Step 5: Commit**

```bash
git add platform-contracts
git commit -m "feat(contracts): router, lifecycle, control, upstream and push contracts"
```

---

### Task 2: Starter — `PangateHeaders`

**Files:**
- Create: `platform-spring-starter/src/main/java/com/pangate/starter/kafka/PangateHeaders.java`
- Modify: `platform-spring-starter/pom.xml` (`org.apache.kafka:kafka-clients`, `<optional>true</optional>`)
- Test: `platform-spring-starter/src/test/java/com/pangate/starter/kafka/PangateHeadersTest.java`

**Interfaces:**
- Produces: `final class PangateHeaders` gồm:
  - Hằng `TRACEPARENT="traceparent"`, `TENANT_ID="tenant-id"`, `CELL_ID="cell-id"`, `CONTENT_TYPE="content-type"`.
  - `static void write(Headers h, String traceparent /*nullable*/, String tenantId, String cellId, String messageName)`: ghi `content-type` = `"application/x-protobuf; schema=pangate.v1." + messageName`; bỏ qua `traceparent` khi null hoặc rỗng. Giá trị mã UTF-8.
  - `static Optional<String> read(Headers h, String name)`: lấy header cuối cùng có tên đó.

- [ ] **Step 1: Viết test thất bại**

```java
@Test void writesSchemaContentTypeAndSkipsEmptyTraceparent() {
  RecordHeaders h = new RecordHeaders();
  PangateHeaders.write(h, "", "t1", "cell-1", "UpstreamRecord");
  assertThat(PangateHeaders.read(h, "content-type")).hasValue("application/x-protobuf; schema=pangate.v1.UpstreamRecord");
  assertThat(PangateHeaders.read(h, "traceparent")).isEmpty();
  assertThat(PangateHeaders.read(h, "tenant-id")).hasValue("t1");
}
@Test void readReturnsLastValue() { }
```

- [ ] **Step 2:** Run `./mvnw -f platform-spring-starter test -Dtest=PangateHeadersTest`. Expected: lỗi compile.
- [ ] **Step 3:** Hiện thực.
- [ ] **Step 4:** Run `./mvnw -f platform-spring-starter install`. Expected: mọi test pass.
- [ ] **Step 5: Commit**

```bash
git add platform-spring-starter
git commit -m "feat(starter): PangateHeaders for Kafka records"
```

---

### Task 3: Devenv — Kafka, Redis, `make infra-up`

**Files:**
- Modify: `platform-devenv/docker-compose.yml`, `platform-devenv/README.md`, `Makefile`

**Interfaces:**
- Produces: các service thuộc nhóm mặc định (không gắn profile):
  - `kafka`: `apache/kafka:3.9.1`, KRaft một node. `KAFKA_NODE_ID=1`, `KAFKA_PROCESS_ROLES=broker,controller`, `KAFKA_LISTENERS=INTERNAL://0.0.0.0:29092,EXTERNAL://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093`, `KAFKA_ADVERTISED_LISTENERS=INTERNAL://kafka:29092,EXTERNAL://localhost:9092`, `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT`, `KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL`, `KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER`, `KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:9093`. Replication factor 1 cho offsets và transaction state, `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0`, `CLUSTER_ID` cố định. Port `9092:9092`. Healthcheck: `/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list`.
  - `kafka-init`: cùng image, `depends_on: kafka (service_healthy)`, chạy `kafka-topics.sh --create --if-not-exists` cho `c1.gw.lifecycle` (32 partition), `c1.gw.upstream.{telemetry,cmd,cmd_result,read,sdk.metrics}` (64) và `gw.control` (8, `--config cleanup.policy=compact`); exit 0.
  - `redis`: `redis:7.4.2`, port `6379:6379`, healthcheck `redis-cli ping`.
  - `gateway` (profile `app`): thêm `depends_on` gồm `kafka-init: service_completed_successfully` và `redis: service_healthy`. Có thêm port `9090:9090` (dành cho M2b).
- Makefile:
  - `infra-up`: `$(COMPOSE) up -d --wait --wait-timeout 90 kafka redis && $(COMPOSE) up kafka-init`.
  - `infra-down`: `$(COMPOSE) down -v`.
  - `doctor`: kiểm thêm `localhost:9092` và `localhost:6379` bằng `bash -c '</dev/tcp/127.0.0.1/<port>'`, in `ok  kafka localhost:9092` hoặc `WARN kafka localhost:9092 unreachable (make infra-up)`. Thêm port 9090 vào danh sách port phải còn trống.
  - Các target `local-check`, `memory-check`, `load-check` phụ thuộc `infra-up`.

- [ ] **Step 1: Kiểm cấu hình compose hợp lệ**

Run: `docker compose -f platform-devenv/docker-compose.yml config -q && docker compose -f platform-devenv/docker-compose.yml --profile app config -q`
Expected: exit 0 với file đã sửa. Trước khi sửa thì lệnh vẫn pass nhưng `config --services` chưa có `kafka`.

- [ ] **Step 2: Bật infra và kiểm topic**

Run: `make infra-up && docker compose -f platform-devenv/docker-compose.yml exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic gw.control | head -1 && make doctor`
Expected: dòng describe có `PartitionCount: 8` và `cleanup.policy=compact`; doctor in `ok  kafka localhost:9092` và `ok  redis localhost:6379`.

- [ ] **Step 3: Chạy lại `make infra-up` để kiểm tính idempotent.** Expected: exit 0, `kafka-init` không lỗi "already exists".

- [ ] **Step 4: Commit**

```bash
git add platform-devenv Makefile
git commit -m "feat(devenv): Kafka (INTERNAL/EXTERNAL listeners), Redis and make infra-up"
```

---

### Task 4: Nền gateway — dependency, cấu hình, `NodeState`, diagnostics, fixture IT

**Files:**
- Modify: `gateway-service/pom.xml`; `config/GatewayProperties.java`, `config/GatewayLimits.java`; `server/NodeState.java`; `application.yml`, `application-local.yml`, `application-dev.yml`; `META-INF/spring.factories`; `src/test/java/.../test/BlockHoundExtension.java`
- Create: `lifecycle/ProcessExit.java`, `diagnostics/{DependencyUnavailableException,MissingTopicsException,DependencyFailureAnalyzer}.java`, `src/it/java/com/pangate/gateway/it/InfraContainers.java`
- Test: `config/GatewayPropertiesTest.java` (bổ sung), `diagnostics/FailureAnalyzersTest.java` (bổ sung)

**Interfaces:**
- pom: thêm `io.lettuce:lettuce-core` và `org.apache.kafka:kafka-clients` (compile); `org.testcontainers:junit-jupiter` và `org.testcontainers:kafka` (test).
- `GatewayProperties` thêm các field:
  - `Redis redis`: `record Redis(RedisTopology topology, String registryUri, String stateUri, Duration commandTimeout)`, với `enum RedisTopology { STANDALONE, CLUSTER }` đặt trong `config`.
  - `Kafka kafka`: `record Kafka(String bootstrap, boolean createTopics, List<String> upstreamTypes, String topicPrefix, DataSize reliableBuffer, DataSize telemetryBuffer)`.
  - `Limits` thêm `int inboundRate, int inboundBurst, int topicsPerConn`; `GatewayLimits` mang theo ba giá trị này.
- `application.yml`, mặc định prod theo spec §9:
  - `gateway.redis`: `{topology: cluster, registry-uri: redis://redis-registry:6379, state-uri: redis://redis-state:6379, command-timeout: 2s}`.
  - `gateway.kafka`: `{bootstrap: kafka:9092, create-topics: false, upstream-types: [telemetry, cmd, cmd_result, read, sdk.metrics], topic-prefix: c1, reliable-buffer: 128MB, telemetry-buffer: 128MB}`.
  - `limits`: `inbound-rate: 50`, `inbound-burst: 100`, `topics-per-conn: 100`.
- `application-local.yml`: `redis: {topology: standalone, registry-uri: redis://localhost:6379, state-uri: redis://localhost:6379}`, `kafka: {bootstrap: localhost:9092, create-topics: true}`.
- `application-dev.yml`: `redis: {topology: standalone, registry-uri: redis://redis:6379, state-uri: redis://redis:6379}`, `kafka: {bootstrap: kafka:29092, create-topics: true}`.
- `NodeState` thêm các field volatile cùng getter/setter: `String nodeId`, `long epoch`, `boolean leaseAlive`, `boolean controlCaughtUp`.
- `@FunctionalInterface interface ProcessExit { void exit(int status, String reason); }`. Bean prod (Task 6) log ERROR `reason` rồi gọi `System.exit(status)` trên một thread mới tên `gw-exit`, không bao giờ gọi trên thread của Lettuce hay Kafka.
- `DependencyUnavailableException(String dependency, String target, Duration waited, String envVar)` kế thừa `RuntimeException`. `DependencyFailureAnalyzer` sinh:
  - description: `"<dependency> <target> is not reachable after <N> s"`, ví dụ `Kafka localhost:9092 is not reachable after 30 s`;
  - action: `"Run 'make infra-up' or set <envVar>"`.
- `MissingTopicsException(List<String> topics)`. Analyzer: description `"Kafka topics missing: <a, b>"`; action `"Create them or set gateway.kafka.create-topics=true (local/dev only)"`.
- `InfraContainers` (IT, singleton tĩnh, khởi động lười, dùng chung cho mọi IT trong JVM failsafe):
  - `static GenericContainer<?> redis()` (`redis:7.4.2`, expose 6379) và `static KafkaContainer kafka()` (`org.testcontainers.kafka.KafkaContainer("apache/kafka:3.9.1")`).
  - `static String redisUri()`, `static String kafkaBootstrap()`.
  - `static List<String> gatewayArgs()` trả `--gateway.redis.registry-uri=…`, `--gateway.redis.state-uri=…`, `--gateway.kafka.bootstrap=…`.
  - `static void flushRedis()`.
- `BlockHoundExtension`: thêm `builder.allowBlockingCallsInside("org.apache.kafka.clients.producer.KafkaProducer", "send")`, kèm comment nêu lý do như ở Global Constraints.

- [ ] **Step 1: Viết test thất bại**

```java
// GatewayPropertiesTest
@Test void localProfileBindsStandaloneRedisAndLocalKafka() {
  /* bind application.yml + application-local.yml: redis.topology()==STANDALONE, registryUri "redis://localhost:6379",
     kafka.bootstrap()=="localhost:9092", createTopics()==true, upstreamTypes() có 5 phần tử, limits.inboundRate()==50 */ }
@Test void prodDefaultsDoNotCreateTopics() { /* createTopics()==false, topology CLUSTER, reliableBuffer == 128MB */ }
// FailureAnalyzersTest
@Test void dependencyUnavailableNamesTargetAndFix() {
  FailureAnalysis a = new DependencyFailureAnalyzer().analyze(new DependencyUnavailableException("Kafka", "localhost:9092",
      Duration.ofSeconds(30), "GATEWAY_KAFKA_BOOTSTRAP"));
  assertThat(a.getDescription()).isEqualTo("Kafka localhost:9092 is not reachable after 30 s");
  assertThat(a.getAction()).isEqualTo("Run 'make infra-up' or set GATEWAY_KAFKA_BOOTSTRAP");
}
@Test void missingTopicsListsThem() { }
```

- [ ] **Step 2:** Run `./mvnw -f gateway-service test -Dtest='GatewayPropertiesTest,FailureAnalyzersTest'`. Expected: lỗi compile.
- [ ] **Step 3:** Hiện thực; đăng ký analyzer mới trong `spring.factories`.
- [ ] **Step 4: Kiểm enforcer và Netty version**

Run: `./mvnw -f gateway-service verify -DskipITs && ./mvnw -f gateway-service dependency:tree -Dincludes=io.netty | grep -v 4.2.20.Final | grep io.netty || echo "netty ok"`
Expected: build xanh (enforcer và `banDuplicateClasses` pass); in ra `netty ok`.

- [ ] **Step 5: Commit**

```bash
git add gateway-service
git commit -m "feat(gateway): Redis/Kafka configuration, node state fields, dependency diagnostics, IT infra fixture"
```

---

### Task 5: `RedisConnections`, `KafkaClients`, `TopicNames`, `TopicProvisioner`

**Files:**
- Create: `lifecycle/{RedisConnections,KafkaClients,TopicNames,TopicProvisioner}.java`
- Test: `src/test/java/.../lifecycle/TopicNamesTest.java`, `src/it/java/com/pangate/gateway/lifecycle/InfraClientsIT.java`

**Interfaces:**
- Consumes: `GatewayProperties.Redis`, `GatewayProperties.Kafka` (Task 4); `MissingTopicsException` (Task 4).
- Produces:
  - `RedisConnections(GatewayProperties.Redis cfg)` implements `AutoCloseable`:
    - `void connect()`: blocking, chỉ gọi từ `InfraLifecycle`. Nếu hai URI giống nhau thì dùng chung một kết nối.
    - `RedisClusterAsyncCommands<String,String> registry()` và `state()`: ném `IllegalStateException` nếu chưa `connect()`.
    - `void close()`.
    - Cấu hình client như Global Constraints.
  - `TopicNames(String prefix, List<String> upstreamTypes)`:
    - `String upstream(String type)` → `"<prefix>.gw.upstream.<type>"`; `String lifecycle()` → `"<prefix>.gw.lifecycle"`; `static final String CONTROL = "gw.control"`.
    - `boolean isUpstreamType(String type)`; `static boolean isReliable(String type)`, true khi type là `cmd` hoặc `cmd_result`.
    - `List<NewTopic> all()`: partition theo Global Constraints, replication `Optional.empty()`, `gw.control` có `cleanup.policy=compact`.
  - `KafkaClients(GatewayProperties.Kafka cfg, String nodeId)` implements `AutoCloseable`:
    - `Admin admin()`: tạo lười, `request.timeout.ms=5000`.
    - `void startProducers()`.
    - `Producer<String,byte[]> reliable()`, `Producer<String,byte[]> telemetry()`.
    - `double maxBufferRatio()`: lấy giá trị lớn nhất của `1 − buffer-available-bytes / buffer-total-bytes` trên hai producer, đọc từ `metrics()`.
    - `Consumer<String,byte[]> newControlConsumer()`.
    - `void close(Duration timeout)`.
    - `client.id` lần lượt là `gw-<nodeId>-reliable`, `gw-<nodeId>-telemetry`, `gw-<nodeId>-control`.
    - Constructor có thêm overload package-private nhận sẵn hai `Producer` để test dùng `MockProducer`.
  - `TopicProvisioner.ensure(Admin admin, TopicNames names, boolean create, Duration timeout)`: dùng `listTopics` để tìm topic thiếu. Nếu `create`, gọi `createTopics` và bỏ qua `TopicExistsException`; nếu không thì ném `MissingTopicsException`.

- [ ] **Step 1: Viết test thất bại**

```java
// TopicNamesTest
@Test void namesFollowPrefix() { assertThat(new TopicNames("c1", List.of("cmd")).upstream("cmd")).isEqualTo("c1.gw.upstream.cmd"); }
@Test void allHasPartitionCountsAndCompactControl() { /* lifecycle 32, upstream 64 mỗi type, gw.control 8 + cleanup.policy=compact */ }
@Test void onlyCmdAndCmdResultAreReliable() { }
// InfraClientsIT (InfraContainers)
@Test void standaloneSharesOneConnectionWhenUrisEqual() { /* registry()==state(); registry().ping().get() == "PONG" */ }
@Test void provisionerCreatesMissingTopicsIdempotently() { /* ensure(create=true) 2 lần → listTopics chứa đủ; lần 2 không lỗi */ }
@Test void provisionerFailsListingMissingWhenCreateDisabled() { /* prefix "zz" → MissingTopicsException chứa "zz.gw.lifecycle" */ }
@Test void producersReportBufferRatioNearZeroWhenIdle() { /* startProducers(); maxBufferRatio() < 0.01 */ }
@Test void lettuceRunsOnNetty42() { /* ping thành công + io.netty.util.Version.identify() chỉ có 4.2.20.Final */ }
```

- [ ] **Step 2:** Run `./mvnw -f gateway-service verify -Dtest=TopicNamesTest -Dit.test=InfraClientsIT`. Expected: lỗi compile.
- [ ] **Step 3:** Hiện thực.
- [ ] **Step 4:** Chạy lại lệnh ở Step 2. Expected: pass.
- [ ] **Step 5: Commit**

```bash
git add gateway-service
git commit -m "feat(gateway): Redis connections, Kafka clients and topic provisioning"
```

---

### Task 6: `Retry` và `InfraLifecycle`

**Files:**
- Create: `lifecycle/{Retry,InfraLifecycle}.java`, `config/ClusterConfig.java`
- Test: `src/test/java/.../lifecycle/RetryTest.java`, `src/it/java/com/pangate/gateway/lifecycle/InfraLifecycleIT.java`

**Interfaces:**
- Consumes: Task 4 và Task 5.
- Produces:
  - `Retry.untilDeadline(String dependency, String target, String envVar, Duration timeout, Sleeper sleeper, Callable<T> attempt) → T`. Backoff 1, 2, 4, 5, 5… s nhưng không vượt quá thời gian còn lại; hết hạn thì ném `DependencyUnavailableException(dependency, target, timeout, envVar)` với cause là lỗi cuối. `interface Sleeper { void sleep(Duration d) throws InterruptedException; }`.
  - `InfraLifecycle implements SmartLifecycle`, `PHASE = Integer.MAX_VALUE − 1900`. Constructor: `(GatewayProperties p, RedisConnections redis, KafkaClients kafka, TopicNames topics)`. `start()` chạy tuần tự, mỗi bước bọc trong `Retry` với `p.startup().dependencyTimeout()`:
    1. `redis.connect()` rồi `PING` trên `registry()` và `state()`. Dependency `"Redis"`, target là URI registry, env `GATEWAY_REDIS_REGISTRY_URI`.
    2. `admin().describeCluster().nodes().get(5 s)`. Dependency `"Kafka"`, target là `bootstrap`, env `GATEWAY_KAFKA_BOOTSTRAP`.
    3. `TopicProvisioner.ensure(...)`, không retry: `MissingTopicsException` thoát ngay.
    4. `startProducers()`, rồi `partitionsFor` mọi topic upstream và lifecycle trên producer tương ứng (làm nóng metadata để `max.block.ms=0` không fail ở lần `send` đầu).
  - Task 9 và Task 11 sẽ thêm bước vào `start()`.
  - `stop()`: `kafka.close(10 s)`, rồi `redis.close()`.
  - `ClusterConfig` (`@Configuration(proxyBeanMethods=false)`) đăng ký các bean: `RedisConnections`, `KafkaClients`, `TopicNames`, `ProcessExit` (bản prod) và `InfraLifecycle`. Các task sau tiếp tục thêm bean vào đây.

- [ ] **Step 1: Viết test thất bại**

```java
// RetryTest (Sleeper giả ghi lại các khoảng ngủ, đồng hồ giả)
@Test void retriesWithBackoffThenSucceeds() { /* fail 3 lần rồi OK → sleeps [1s,2s,4s] */ }
@Test void throwsDependencyUnavailableAtDeadline() { /* timeout 6s, luôn fail → sleeps tổng ≤ 6s, exception.target()=="localhost:9092" */ }
// InfraLifecycleIT
@Test void startsAgainstContainersAndCreatesTopics() { }
@Test void kafkaUnreachableFailsWithinTimeoutWithDiagnostic() {
  /* bootstrap "localhost:1", dependency-timeout 3s: start() ném DependencyUnavailableException trong < 6 s, analyzer
     description == "Kafka localhost:1 is not reachable after 3 s" */ }
@Test void redisUnreachableFailsWithinTimeoutWithDiagnostic() { /* "Redis redis://localhost:1 is not reachable after 3 s" */ }
```

- [ ] **Step 2:** Run `./mvnw -f gateway-service verify -Dtest=RetryTest -Dit.test=InfraLifecycleIT`. Expected: lỗi compile.
- [ ] **Step 3:** Hiện thực.
- [ ] **Step 4:** Thêm `InfraContainers.gatewayArgs()` vào `GatewayLocalIT.start(...)` (từ task này gateway cần Redis và Kafka để start). Run `./mvnw -f gateway-service verify`. Expected: mọi unit test và IT (gồm 12 IT của M1) pass.
- [ ] **Step 5: Commit**

```bash
git add gateway-service
git commit -m "feat(gateway): InfraLifecycle with bounded dependency retry and startup diagnostics"
```

---

### Task 7: `NodeIdentity` — epoch, lease, fence, re-epoch

**Files:**
- Create: `registry/{RegistryKeys,NodeIdentity}.java`
- Modify: `config/ClusterConfig.java` (bean `NodeIdentity`, `ReadinessCheck` "lease"), `metrics/GatewayMetrics.java`
- Test: `src/it/java/com/pangate/gateway/registry/NodeIdentityIT.java`

**Interfaces:**
- Consumes: `RedisConnections`, `NodeState`, `TickScheduler`, `ProcessExit`.
- Produces:
  - `RegistryKeys`: `static String epoch(String nodeId)` → `"node:epoch:{"+nodeId+"}"`; `node(nodeId)` → `"node:{"+nodeId+"}"`; `reg(p)` → `"reg:{"+p+"}"`; `seq(p)` → `"seq:{"+p+"}"`; hằng `NODES_LIVE="nodes:live"`, `SEQ_EPOCHS="seqepochs"`.
  - `NodeIdentity implements SmartLifecycle`, `PHASE = Integer.MAX_VALUE − 1800`. Constructor: `(String nodeId, RedisConnections redis, NodeState node, TickScheduler tick, LongSupplier monoMillis, LongSupplier wallMillis, ProcessExit exit, GatewayMetrics metrics)`.
    - `start()`: blocking, tối đa 5 s. Chạy `INCR` epoch → `node.epoch(e)`, `node.nodeId(id)`; chạy script lease (`EVALSHA`, gặp `NOSCRIPT` thì `EVAL`) với `(epoch, 15)`; `ZADD nodes:live <wallMs> <nodeId>`; `leaseAlive=true`; lên lịch `renew()` mỗi 5 s trên `gw-tick`.
    - `void renew()`: async, public để test gọi trực tiếp.
      - Kết quả `1` → `lastOk = mono`, `ZADD`, rồi `leaseAlive=true`.
      - Kết quả `-1` → `node.draining(true)` và `exit.exit(2, "node <id> fenced: epoch <e> superseded")`.
      - Lỗi Redis → tăng `gw.lease.renew.failures`.
      - Ở đầu mỗi lần `renew()`: nếu `mono − lastOk > 15 s` thì `leaseAlive=false`. Khi `leaseAlive==false` và Redis trả lời được, gọi `reEpoch()` thay vì renew.
    - `CompletionStage<Long> reEpoch()`: `INCR` → epoch mới → script lease → `leaseAlive=true` → gọi mọi listener `onReEpoch`.
    - `void onReEpoch(LongConsumer listener)`; `long epoch()`.
    - `stop()`: hủy lịch renew; chạy script xóa có điều kiện (`if GET == epoch then DEL`, để không xóa lease của incarnation mới hơn) rồi `ZREM nodes:live <nodeId>`; timeout 2 s, lỗi thì chỉ WARN.
  - Bean `ReadinessCheck`: `leaseAlive ? empty : "node lease lost"`.
  - Metric: `gw.lease.renew.failures` (counter), `gw.node.epoch` (gauge).

- [ ] **Step 1: Viết test thất bại** (IT, Redis thật, `monoMillis` giả, `ProcessExit` ghi lại lời gọi)

```java
@Test void startTakesNewEpochAndLeaseWithTtl15() { /* epoch==1; GET node:{n1}=="1"; 0 < TTL ≤ 15; ZSCORE nodes:live n1 != null */ }
@Test void restartWithLiveLeaseTakesOverWithHigherEpoch() { /* a.start(); b.start() cùng nodeId (không stop a) → b.epoch()==2, b không exit */ }
@Test void olderIncarnationFencesOnRenew() { /* tiếp test trên: a.renew() → exit(2, "...fenced...") được gọi, node.draining()==true */ }
@Test void leaseLostAfter15sThenReEpochsOnRecovery() {
  /* renew lỗi (đóng kết nối Redis hoặc pause container) và mono += 16 s → leaseAlive false; khôi phục → renew()
     → epoch tăng 1, listener onReEpoch nhận epoch mới, leaseAlive true */ }
@Test void stopDeletesOnlyOwnLease() { /* b (epoch 2) giữ lease; a.stop() → GET node:{n1} vẫn "2" */ }
```

- [ ] **Step 2:** Run `./mvnw -f gateway-service verify -Dit.test=NodeIdentityIT`. Expected: lỗi compile.
- [ ] **Step 3:** Hiện thực.
- [ ] **Step 4:** Chạy lại lệnh ở Step 2. Expected: pass.
- [ ] **Step 5: Commit**

```bash
git add gateway-service
git commit -m "feat(gateway): node epoch and lease with fencing and re-epoch on recovery"
```

---

### Task 8: State control — `RevocationList`, `CellMapView`, `TenantPolicy`, `TenantLimitsStore`

**Files:**
- Create: `control/{RevocationList,CellMapView,TenantPolicy,TenantLimitsStore,ControlKeys}.java`, `auth/SdkVersion.java`
- Test: `src/test/java/.../control/{RevocationListTest,CellMapViewTest,TenantLimitsStoreTest,ControlKeysTest}.java`, `auth/SdkVersionTest.java`

**Interfaces:**
- Consumes: `TenantCfg`, `TenantLimits`, `KillSwitches`, `Scope`, `Revoke` (Task 1); `ResumeTicketService.jtiHash(String)` (M1); `GatewayLimits` (Task 4).
- Produces:
  - `RevocationList`: `ConcurrentHashMap<String, Long>`.
    - Key builder tĩnh: `principalKey(p)` → `"p:"+p`; `deviceKey(p,d)` → `"d:"+p+"/"+d`; `jtiKey(long hash)` → `"j:"+hash`; `static Optional<String> keyOf(Revoke r)` (jti được băm bằng `jtiHash`).
    - `void add(String key, long expiresAtMs)`, `void remove(String key)`.
    - `boolean isRevoked(String principal, String device, long jtiHash, long nowMs)`: entry hết hạn coi như không có.
    - `int sweep(long nowMs)`, `int size()`.
    - Không có hard cap. Gauge `gw.revocations` để alert, thay cho "tối đa 128 MB" của spec §3.4.
  - `CellMapView(String ownCell)`:
    - `void put(String tenant, String cell)`, `void remove(String tenant)`.
    - `boolean servesTenant(String tenant)`: tenant chưa có mapping thì trả `true` (quyết định của plan, để local và dev chạy được khi chưa có event nào).
    - `Optional<String> cellOf(String tenant)`.
  - `record TenantPolicy(int heartbeatSec, int inboundRate, int inboundBurst, int topicsPerConn, int maxConns, String minSdkVersion, Set<String> originAllowlist, boolean singleSession, boolean blocked, boolean telemetryEnabled, boolean broadcastEnabled, boolean downgradeReliable, boolean recycleEnabled, boolean sseFallbackEnabled, int admissionRatePerNode)`:
    - `long idleTimeoutSec()` = `3L * heartbeatSec`.
    - `static TenantPolicy defaults(GatewayLimits l)`: `heartbeat = l.heartbeatInterval()`, inbound và topics lấy từ `l`; `maxConns = 0` (không giới hạn); `minSdk = ""`; allowlist rỗng; `telemetry = true`; `broadcast = true`; các cờ còn lại `false`; `admissionRatePerNode = 0`.
  - `TenantLimitsStore(String ownCell, Supplier<GatewayLimits> limits)`:
    - `TenantPolicy policy(String tenantId)`: O(1), đọc một snapshot `volatile Map<String,TenantPolicy>`; tenant không có mặt thì trả policy hợp nhất fleet + cell.
    - `void apply(TenantCfg cfg)`: scope `CELL` với `scope_id` khác `ownCell` thì bỏ qua.
    - `void remove(Scope scope, String id)`.
    - Hợp nhất theo thứ tự defaults → fleet → cell của node → tenant. Field `optional` có mặt mới ghi đè; `origin_allowlist` chỉ ghi đè khi khác rỗng.
    - Mỗi lần `apply` hoặc `remove` tính lại toàn bộ snapshot (copy-on-write). TenantCfg rất hiếm nên chi phí chấp nhận được.
    - `TenantLimitsStore` gộp luôn vai trò `KillSwitches` của spec §3.1: switch là field của `TenantPolicy`.
  - `ControlKeys`: `static Parsed parse(String recordKey)` với `sealed interface Parsed permits RevokeKey, CellMapKey, TenantCfgKey, Unknown`. Key `revoke:j:<jti>` được chuyển thành `RevocationList.jtiKey(jtiHash(jti))`.
  - `SdkVersion.compare(String a, String b)`: so sánh từng đoạn số phân tách bằng dấu chấm; bỏ hậu tố sau `-` hoặc `+`; đoạn thiếu hoặc không phải số tính là 0.

- [ ] **Step 1: Viết test thất bại**

```java
// RevocationListTest
@Test void revokedByPrincipalDeviceOrJti() { }
@Test void expiredEntriesIgnoredAndSwept() { /* add(…, now-1) → isRevoked false; sweep → size 0 */ }
@Test void deviceRevokeScopedToPrincipal() { /* d:t1:u1/d1 không chặn t1:u2/d1 */ }
// CellMapViewTest
@Test void unmappedTenantIsServedAndForeignCellIsNot() { }
// TenantLimitsStoreTest
@Test void tenantOverridesCellOverridesFleet() {
  /* fleet heartbeat 60, cell-1 heartbeat 45, tenant t1 heartbeat 20 → t1:20, t2:45; cell-2 cfg bị bỏ qua */ }
@Test void absentOptionalKeepsLowerScope() { /* tenant cfg chỉ set tenant_blocked → heartbeat vẫn của fleet */ }
@Test void removeRestoresLowerScope() { }
@Test void unknownTenantGetsFleetCellPolicy() { }
// ControlKeysTest
@Test void parsesAllKeyShapes() { /* 7 dạng key ở control.proto + "garbage" → Unknown */ }
// SdkVersionTest
@Test void comparesNumericSegments() { /* "1.10.0" > "1.9.9"; "2.0" == "2.0.0"; "1.2.3-beta" == "1.2.3"; "x" < "0.0.1" */ }
```

- [ ] **Step 2:** Run `./mvnw -f gateway-service test -Dtest='RevocationListTest,CellMapViewTest,TenantLimitsStoreTest,ControlKeysTest,SdkVersionTest'`. Expected: lỗi compile.
- [ ] **Step 3:** Hiện thực.
- [ ] **Step 4:** Chạy lại lệnh ở Step 2. Expected: pass.
- [ ] **Step 5: Commit**

```bash
git add gateway-service
git commit -m "feat(gateway): control state (revocations, cell map, tenant policies)"
```

---

### Task 9: `ControlEventConsumer` và catch-up

**Files:**
- Create: `control/{ControlEventHandler,ControlState,ControlEventConsumer}.java`
- Modify: `lifecycle/InfraLifecycle.java` (bước 5: `consumer.start()`; khi stop: `consumer.stop()` trước producer), `config/ClusterConfig.java` (bean, `ReadinessCheck` "control", sweep revocation mỗi 60 s trên `gw-tick`), `metrics/GatewayMetrics.java`
- Test: `src/test/java/.../control/ControlStateTest.java`, `src/it/java/com/pangate/gateway/control/ControlEventConsumerIT.java`

**Interfaces:**
- Consumes: `KafkaClients.newControlConsumer()` (Task 5); store của Task 8; `NodeState`, `ProcessExit`.
- Produces:
  - `interface ControlEventHandler`, mọi method đều `default` rỗng:
    - `onRevoke(String revocationKey, long expiresAtMs)`, `onRevokeRemoved(String revocationKey)`;
    - `onCellMap(String tenant, String cell)`, `onCellMapRemoved(String tenant)`;
    - `onTenantCfg(TenantCfg cfg)`, `onTenantCfgRemoved(Scope scope, String id)`.
  - `ControlState(RevocationList, CellMapView, TenantLimitsStore, LongSupplier wallMillis) implements ControlEventHandler`: cập nhật store. Revoke có `expires_at_ms ≤ now` thì bỏ qua.
  - `ControlEventConsumer(KafkaClients kafka, NodeState node, List<ControlEventHandler> handlers, GatewayMetrics metrics, ProcessExit exit, Duration catchUpLimit /* 5 phút */, LongSupplier monoMillis)`:
    - `void start()`: tạo thread platform `gw-ctl` (daemon). Thread này `assign` toàn bộ partition của `gw.control`, `seekToBeginning`, chụp `endOffsets`, rồi lặp `poll(500 ms)`.
    - Value null là tombstone, dispatch theo `ControlKeys`. Value khác null được parse thành `ControlEvent` và dispatch theo `event_case`. Handler chạy theo thứ tự danh sách: `ControlState` trước, `ControlEnforcer` (Task 15) sau.
    - Khi mọi partition có `position ≥ snapshot` → `node.controlCaughtUp(true)` (đã bằng `true` từ đầu nếu snapshot toàn 0).
    - Quá `catchUpLimit` mà chưa catch-up → `exit.exit(1, "gw.control not caught up within 5m")`.
    - Record lỗi (parse hỏng, key `Unknown`, `event` không set) → bỏ qua, `gw.control.consumer.errors{reason=bad_record}`.
    - Exception của `poll` → `gw.control.consumer.errors{reason=poll}`, backoff 1, 5, 30 s (giữ ở 30 s), giữ nguyên state, tiếp tục.
    - `void stop()`: `wakeup()`, join 5 s, `close()`.
    - `boolean caughtUp()`.
  - Metric: `gw.control.events{type=revoke|cellmap|tenantcfg|tombstone}`, `gw.control.consumer.errors{reason}`, `gw.revocations` (gauge).

- [ ] **Step 1: Viết test thất bại**

```java
// ControlStateTest
@Test void dispatchesToStoresAndIgnoresExpiredRevokes() { }
// ControlEventConsumerIT (Kafka thật, topic gw.control tạo bằng TopicProvisioner, mỗi test một prefix-free topic name riêng nếu cần)
@Test void caughtUpImmediatelyOnEmptyTopic() { }
@Test void replaysCompactedStateBeforeCaughtUp() { /* publish revoke p:t1:u9 trước khi start → khi caughtUp() true thì isRevoked(t1:u9) true */ }
@Test void tombstoneRemovesState() { }
@Test void malformedAndUnknownRecordsAreSkippedAndCatchUpCompletes() {
  /* value = bytes rác; key "zzz"; ControlEvent rỗng → errors{bad_record} == 3, caughtUp() true, record hợp lệ sau đó vẫn áp dụng */ }
@Test void neverCaughtUpExitsAfterLimit() { /* catchUpLimit 2 s, endOffsets giả không đạt (consumer stub) → exit(1, …) */ }
```

- [ ] **Step 2:** Run `./mvnw -f gateway-service verify -Dtest=ControlStateTest -Dit.test=ControlEventConsumerIT`. Expected: lỗi compile.
- [ ] **Step 3:** Hiện thực.
- [ ] **Step 4:** Chạy lại lệnh ở Step 2. Expected: pass.
- [ ] **Step 5: Commit**

```bash
git add gateway-service
git commit -m "feat(gateway): gw.control consumer with compacted replay, catch-up readiness and error backoff"
```

---

### Task 10: `SessionListener`, `RetryQueue`, `RegistryWriter`

**Files:**
- Create: `session/SessionListener.java`, `registry/{RetryQueue,RegistryWriter}.java`
- Modify: `session/SessionTracker.java` (thêm `List<SessionListener>`), `session/Session.java` (`connectedAtMs`), `config/GatewayConfig.java` (bean `SessionTracker` nhận `List<SessionListener>`), `config/ClusterConfig.java`, `metrics/GatewayMetrics.java`
- Test: `src/test/java/.../registry/RetryQueueTest.java`, `session/SessionTrackerListenersTest.java`, `src/it/java/com/pangate/gateway/registry/RegistryWriterIT.java`

**Interfaces:**
- Consumes: `RedisConnections`, `RegistryKeys`, `NodeState`, `TickScheduler`.
- Produces:
  - `interface SessionListener { default void activated(Session s) {} default void closed(Session s) {} }`. Cả hai method chạy trên owner loop.
  - `SessionTracker`:
    - Constructor thêm tham số cuối `List<SessionListener> listeners`.
    - `onActivated` gọi `activated` sau khi đã thêm vào index.
    - `onClosed` gọi `closed` sau mọi bookkeeping, và chỉ khi `s.identity() != null`.
    - Exception từ listener được bắt và log WARN (lấy mẫu), không làm hỏng cleanup.
  - `Session`: thêm `long connectedAtMs()`, gán lúc `activate` bằng tham số mới `long connectedAtMs`. Chữ ký mới: `activate(Identity, String connId, long connectedAtMs, long authAtSec, long expAtSec, long authJtiHash, List<String> topics)`. `HelloAuthHandler` truyền `System.currentTimeMillis()`.
  - `RetryQueue(ScheduledExecutorService tick, int capacity /* 10_000 */, GatewayMetrics metrics)`:
    - `void retryLater(String kind, Supplier<CompletionStage<?>> op, BooleanSupplier stillWanted)`: lần thử thứ 1, 2, 3 lần lượt sau 100 ms, 1 s, 5 s; lỗi cả ba thì `gw.registry.write.errors{reason=dropped}`; đầy hàng đợi thì `{reason=overflow}`; `stillWanted` false lúc tới hạn thì bỏ, không đếm.
    - `int size()`.
  - `RegistryWriter(RedisConnections redis, NodeState node, RetryQueue retry, GatewayMetrics metrics) implements SessionListener`:
    - `void loadScripts()`: `SCRIPT LOAD`, được `InfraLifecycle` gọi sau bước Redis.
    - `CompletionStage<Map<String,String>> register(Session s, boolean single)`: chạy script §6.2 với `ARGV = connId, "<nodeId>|<epoch>|<connectedAtMs>|<platform>", single ? "1" : "0"`; gặp `NOSCRIPT` thì dùng `EVAL`; trả về các entry cũ. Lỗi → `gw.registry.write.errors{reason=redis}` rồi `retryLater` với `stillWanted = () -> s.state() == ACTIVE || s.state() == DRAINING` (sau Task 15, ở Task 10 chỉ là `ACTIVE`).
    - `void unregister(String principal, String connId)`: `HDEL`; lỗi thì retry (`stillWanted` luôn true).
    - `activated(s)` → `register(s, false)`. Chế độ single-session dùng ở M2b.
    - `closed(s)` → `unregister(principal, connId)`.

- [ ] **Step 1: Viết test thất bại**

```java
// RetryQueueTest (ScheduledExecutor giả điều khiển thời gian)
@Test void retriesAt100ms1s5sThenDrops() { }
@Test void overflowCountsAndRejects() { /* capacity 2, thêm 3 → errors{overflow}==1 */ }
@Test void retryOfClosedSessionIsSkipped() { /* stillWanted false → op không chạy lại */ }
// SessionTrackerListenersTest (EmbeddedChannel)
@Test void listenersSeeActivationAndCloseOnce() { }
@Test void throwingListenerDoesNotBreakCleanup() { /* activeCount về 0, PrincipalIndex rỗng */ }
// RegistryWriterIT
@Test void registerWritesValueWithNodeEpochAndPlatform() { /* HGET reg:{t1:u1} <connId> == "n1|7|<ms>|android" */ }
@Test void unregisterRemovesOnlyOwnConnId() { /* 2 connId cùng principal → HDEL một cái, cái kia còn */ }
@Test void singleModeReturnsAndReplacesPrevious() { /* register(single=true) trả entry cũ; HGETALL chỉ còn entry mới */ }
@Test void noscriptFallsBackToEval() { /* SCRIPT FLUSH rồi register → vẫn ghi được */ }
@Test void redisDownGoesToRetryQueue() { /* pause container → register lỗi → retry.size()==1 → unpause → sau ≤ 6 s entry có mặt */ }
```

- [ ] **Step 2:** Run `./mvnw -f gateway-service verify -Dtest='RetryQueueTest,SessionTrackerListenersTest' -Dit.test=RegistryWriterIT`. Expected: lỗi compile.
- [ ] **Step 3:** Hiện thực. Trước khi sửa, chạy `node .gitnexus/run.cjs impact SessionTracker --direction upstream --repo .` và `impact activate`, rồi cập nhật mọi chỗ gọi `Session.activate` (test M1 và `HelloAuthHandler`).
- [ ] **Step 4:** Run `./mvnw -f gateway-service verify -Dit.test=RegistryWriterIT`. Expected: mọi unit test (gồm test M1) và `RegistryWriterIT` pass.
- [ ] **Step 5: Commit**

```bash
git add gateway-service
git commit -m "feat(gateway): session listeners and Redis registry writer with bounded retry queue"
```

---

### Task 11: `SeqReader` và `CONNECTED` bất đồng bộ

**Files:**
- Create: `registry/{SeqReader,RedisSeqReader}.java`
- Modify: `pipeline/HelloAuthHandler.java`, `pipeline/PipelineDeps.java`, `config/GatewayConfig.java`, `config/ClusterConfig.java`, `lifecycle/InfraLifecycle.java` (bước: `seq.refreshEpochs()` một lần, lỗi chỉ WARN, rồi `seq.start()`)
- Test: `src/test/java/.../pipeline/HelloAuthHandlerTest.java` (bổ sung), `src/it/java/com/pangate/gateway/registry/RedisSeqReaderIT.java`

**Interfaces:**
- Consumes: `RedisConnections.state()`, `RegistryKeys`, `TickScheduler`.
- Produces:
  - `interface SeqReader`:
    - `CompletionStage<OptionalLong> read(String principal)`: key không tồn tại → `OptionalLong.of(0)`; timeout hoặc lỗi → `OptionalLong.empty()`. Không bao giờ hoàn tất bằng exception.
    - `int seqEpoch(String principal)`.
  - `RedisSeqReader(RedisConnections redis, ScheduledExecutorService tick, Duration timeout /* 200 ms */) implements SeqReader`:
    - `read`: `GET seq:{p}` với `orTimeout(200 ms)`.
    - `void refreshEpochs()`: `HGETALL seqepochs` → `int[16384]`, field là slot dạng số; field lỗi thì bỏ qua. Swap qua `AtomicReference`. Lỗi thì giữ mảng cũ.
    - `seqEpoch(p)` = `epochs[SlotHash.getSlot(p)]`.
    - `void start()`: refresh mỗi 5 s trên `gw-tick`.
  - `HelloAuthHandler`: constructor thêm `SeqReader seq`. Sau `session.activate` và `tracker.onActivated` (listener ghi registry và lifecycle chạy song song):
    - Đặt `autoRead=false`, gọi `seq.read(principal)`.
    - Khi hoàn tất thì `session.loop().execute(...)`. Nếu session không còn `ACTIVE`: release mọi frame đang giữ và dừng. Ngược lại gửi `CONNECTED` với `seq` = giá trị đọc được (`seq_unknown=false`) hoặc `seq=0, seq_unknown=true` khi empty, và `seq_epoch = seq.seqEpoch(p)`. Sau đó chuyển tiếp frame đang giữ, `autoRead=true`, gỡ handler.
    - `PipelineDeps` thêm field `SeqReader seq`.

- [ ] **Step 1: Viết test thất bại**

```java
// HelloAuthHandlerTest (SeqReader giả)
@Test void connectedCarriesSeqAndEpochFromReader() { /* read → 41, seqEpoch → 3 ⇒ CONNECTED.seq 41, seq_epoch 3, seq_unknown false */ }
@Test void connectedSentWithSeqUnknownWhenSeqReadTimesOut() { /* read → empty ⇒ seq_unknown true; channel vẫn mở */ }
@Test void framesPipelinedDuringSeqReadProcessedAfterConnected() {
  /* read chưa hoàn tất; client gửi PING → chưa có PONG; hoàn tất read → thứ tự outbound: CONNECTED rồi PONG */ }
@Test void sessionClosedBeforeSeqReadCompletesReleasesHeldFrames() { /* refCnt các frame giữ == 0, không gửi CONNECTED */ }
// RedisSeqReaderIT
@Test void readsSeqAndMissingKeyIsZero() { }
@Test void seqEpochUsesClusterSlotOfPrincipal() { /* HSET seqepochs <SlotHash.getSlot("t1:u1")> 5 → refreshEpochs → seqEpoch == 5 */ }
@Test void timesOutAfter200msAsEmpty() { /* pause container → read hoàn tất empty trong < 500 ms */ }
```

- [ ] **Step 2:** Run `./mvnw -f gateway-service verify -Dtest=HelloAuthHandlerTest -Dit.test=RedisSeqReaderIT`. Expected: các test mới fail.
- [ ] **Step 3:** Hiện thực. Trước khi sửa `HelloAuthHandler`, chạy `node .gitnexus/run.cjs impact HelloAuthHandler --direction upstream --repo .`.
- [ ] **Step 4:** Chạy lại lệnh ở Step 2 với `./mvnw -f gateway-service verify -Dit.test=RedisSeqReaderIT`. Expected: mọi unit test và IT này pass.
- [ ] **Step 5: Commit**

```bash
git add gateway-service
git commit -m "feat(gateway): seq read on HELLO with 200 ms timeout and slot-based seq epochs"
```

---

### Task 12: Lifecycle event, upstream, `SessionRateLimiter`

**Files:**
- Create: `upstream/{LifecyclePublisher,UpstreamPublisher,SessionRateLimiter}.java`
- Modify: `pipeline/SessionHandler.java`, `pipeline/PipelineDeps.java`, `protocol/Frames.java`, `session/Session.java` (field rate limit), `config/ClusterConfig.java`, `config/GatewayConfig.java`, `metrics/GatewayMetrics.java`
- Test: `src/test/java/.../upstream/{SessionRateLimiterTest,UpstreamPublisherTest,LifecyclePublisherTest}.java`, `pipeline/SessionHandlerTest.java` (bổ sung)

**Interfaces:**
- Consumes: `KafkaClients` (Task 5, test dùng `MockProducer`), `TopicNames`, `NodeState`, `PangateHeaders` (Task 2), `TenantLimitsStore` (Task 8), `SessionListener` (Task 10).
- Produces:
  - `Frames.upstreamAck(String msgId)`: frame `UPSTREAM_ACK`, `msg_id` = id gốc, không có payload.
  - `Session` thêm `float tokens`, `long lastRefillNanos`, `int strikes`, `long lastStrikeSec`, đều package-private cho `SessionRateLimiter`. Token khởi tạo bằng burst ở lần dùng đầu.
  - `SessionRateLimiter.check(Session s, TenantPolicy p, long nowNanos, long nowSec) → Verdict { OK, LIMITED, CLOSE }` (spec §5.5.1):
    - Nạp lại token với `inboundRate` mỗi giây, trần `inboundBurst`; còn ≥ 1 token thì trừ và trả `OK`.
    - Hết token: giây vi phạm ngay sau giây vi phạm trước thì `strikes++`, ngược lại `strikes=1`; cùng một giây chỉ tính một strike. `strikes ≥ 3` → `CLOSE`, ngược lại `LIMITED`.
  - `LifecyclePublisher(KafkaClients kafka, TopicNames topics, NodeState node, String cell, GatewayMetrics metrics, LongSupplier wallMillis) implements SessionListener`:
    - `activated` → `publish(s, EVENT_TYPE_CONNECTED)`.
    - `closed`: `closeReason == "HEARTBEAT_TIMEOUT"` → `HEARTBEAT_TIMEOUT`; `closeCode == DUPLICATE_SESSION` → `KICKED`; còn lại `DISCONNECTED`. `close_code = closeCode.code()`.
    - `void publish(Session s, EventType t)` cũng được dùng cho resync (Task 14).
    - Gửi trên producer `reliable`, topic `lifecycle()`, key `principalId`, header theo `PangateHeaders` (`messageName="ConnectionEvent"`).
    - Lỗi gửi (cả ném đồng bộ lẫn qua callback) → `gw.lifecycle.publish.errors`, không retry.
  - `UpstreamPublisher(KafkaClients kafka, TopicNames topics, NodeState node, String cell, TenantLimitsStore tenants, GatewayMetrics metrics, LongSupplier wallMillis, LongSupplier nanos)`. Method `void publish(ChannelHandlerContext ctx, Session s, Frame f, String forcedType /* null, hoặc "cmd_result" cho RPC_RES */)` chạy trên owner loop:
    1. Rate limit: `LIMITED` → `ERROR{4009,"RATE_LIMITED",retryable=true,fatal=false}` và `gw.upstream{result=rate_limited}`; `CLOSE` → `s.close(RATE_LIMITED, "RATE_LIMITED")`.
    2. `type = forcedType != null ? forcedType : f.headers["type"]`. Type không thuộc `upstream-types` → `ERROR{4000,"unknown type",false,false}`. `msg_id` rỗng → `ERROR{4000,"missing msg_id",false,false}`.
    3. `!tenants.policy(t).telemetryEnabled()` và type không reliable → drop, `gw.upstream{type,result=dropped}`.
    4. Dựng `UpstreamRecord`: `received_at_ms = wall`; `headers` = `f.headers` trừ `type`; header Kafka theo `PangateHeaders` (`traceparent` lấy từ `f.headers`). Key là `principalId`.
    5. Type reliable → producer `reliable`; callback (chạy trên thread Kafka) → `s.loop().execute(...)`: thành công gửi `UPSTREAM_ACK`, lỗi gửi `ERROR{4500,"UNAVAILABLE",retryable=true,fatal=false}`; chỉ gửi khi channel còn active. Type khác → producer `telemetry`, callback chỉ đếm metric. `send` ném đồng bộ được xử lý giống lỗi callback.
    6. Metric `gw.upstream{type,result=ok|error|dropped|rate_limited|unknown_type}`.
  - `SessionHandler`:
    - `FRAME_TYPE_UPSTREAM` → `upstream.publish(ctx, s, f, null)`.
    - `FRAME_TYPE_RPC_RES` → `upstream.publish(ctx, s, f, "cmd_result")`.
    - `SUBSCRIBE` và `UNSUBSCRIBE` vẫn trả `UNSUPPORTED` cho tới M2b.

- [ ] **Step 1: Viết test thất bại**

```java
// SessionRateLimiterTest
@Test void burst100ThenLimited() { }
@Test void threeConsecutiveViolatingSecondsClose() { /* giây 10, 11 → LIMITED; giây 12 → CLOSE */ }
@Test void gapResetsStrikes() { /* giây 10, 12 → strikes==1 */ }
// UpstreamPublisherTest (EmbeddedChannel + MockProducer)
@Test void cmdProducesRecordAndAcksAfterBrokerAck() {
  /* record topic "c1.gw.upstream.cmd", key "t1:u1", value.conn_id == connId, header content-type schema=pangate.v1.UpstreamRecord;
     chưa completeNext → chưa có UPSTREAM_ACK; completeNext() → UPSTREAM_ACK msg_id khớp */ }
@Test void cmdFailureAnswersUnavailableAndKeepsSession() { /* errorNext(new TimeoutException()) → ERROR 4500 retryable, channel open */ }
@Test void sendThrowingSynchronouslyIsHandled() { /* producer ném BufferExhaustedException → ERROR 4500, không exception lên pipeline */ }
@Test void telemetryGoesToTelemetryProducerWithoutAck() { }
@Test void telemetryDroppedWhenSwitchOff() { /* tenant cfg upstream_telemetry_enabled=false → không record, cmd vẫn đi */ }
@Test void unknownTypeAndMissingMsgIdAnswer4000NotClose() { }
@Test void rpcResGoesAsCmdResult() { }
// LifecyclePublisherTest (MockProducer)
@Test void connectedAndDisconnectedEventsCarryEpochAndCloseCode() { }
@Test void heartbeatTimeoutAndKickedMapped() { }
```

- [ ] **Step 2:** Run `./mvnw -f gateway-service test -Dtest='SessionRateLimiterTest,UpstreamPublisherTest,LifecyclePublisherTest,SessionHandlerTest'`. Expected: lỗi compile.
- [ ] **Step 3:** Hiện thực (chạy `impact SessionHandler` trước).
- [ ] **Step 4:** Run `./mvnw -f gateway-service test`. Expected: mọi unit test pass.
- [ ] **Step 5: Commit**

```bash
git add gateway-service
git commit -m "feat(gateway): upstream and lifecycle publishing to Kafka with per-session rate limit"
```

---

### Task 13: Áp lực buffer Kafka (`UpstreamPressure`)

**Files:**
- Create: `upstream/UpstreamPressure.java`
- Modify: `session/Session.java` (bộ đếm upstream 10 s, cờ paused), `upstream/UpstreamPublisher.java` (cộng byte), `config/ClusterConfig.java`, `metrics/GatewayMetrics.java`
- Test: `src/test/java/.../upstream/UpstreamPressureTest.java`

**Interfaces:**
- Consumes: `KafkaClients.maxBufferRatio()` (Task 5), `LoopRegistry`, `TickScheduler`.
- Produces:
  - `Session`:
    - `void addUpstreamBytes(int n, long nowSec)`: cửa sổ 10 s, reset lười khi `nowSec − windowStart ≥ 10`.
    - `long upstreamBytes10s(long nowSec)`: cửa sổ đã cũ thì trả 0.
    - `boolean upstreamPaused()`, `void upstreamPaused(boolean)`.
  - `UpstreamPublisher` gọi `s.addUpstreamBytes(f.getPayload().size(), nowSec)` cho mọi frame được chấp nhận.
  - `UpstreamPressure(DoubleSupplier maxBufferRatio, LoopRegistry loops, GatewayMetrics metrics)`:
    - `void sample()`: chạy mỗi 100 ms trên `gw-tick`.
      - `ratio > 0.9` và chưa pause → với mỗi loop, `execute` chọn N = `max(1, ceil(1% × sessionCount))` session `ACTIVE` có `upstreamBytes10s` lớn nhất (min-heap cỡ N), đặt `autoRead=false` và `upstreamPaused=true`.
      - `ratio < 0.5` và đang pause → mọi loop mở lại toàn bộ session đang paused.
    - `boolean paused()`.
  - Metric: gauge `gw.kafka.buffer.ratio`, counter `gw.upstream.paused.sessions`.

- [ ] **Step 1: Viết test thất bại**

```java
// UpstreamPressureTest (LoopFixture của M1, 200 session trên một loop, bytes khác nhau)
@Test void pausesTopOnePercentNoisiestAbove90() { /* ratio 0.95 → đúng 2 session ồn nhất autoRead false */ }
@Test void staysPausedBetween50And90() { /* ratio 0.7 → không đổi */ }
@Test void resumesAllBelow50() { }
@Test void staleWindowCountsAsZero() { /* bytes ở giây 0, now 11 → upstreamBytes10s == 0 */ }
```

- [ ] **Step 2:** Run `./mvnw -f gateway-service test -Dtest=UpstreamPressureTest`. Expected: lỗi compile.
- [ ] **Step 3:** Hiện thực.
- [ ] **Step 4:** Run `./mvnw -f gateway-service test`. Expected: pass.
- [ ] **Step 5: Commit**

```bash
git add gateway-service
git commit -m "feat(gateway): pause noisiest upstream sessions when a Kafka producer buffer exceeds 90%"
```

---

### Task 14: `RegistryResync`

**Files:**
- Create: `registry/RegistryResync.java`
- Modify: `config/ClusterConfig.java` (`nodeIdentity.onReEpoch(e -> resync.resync())`)
- Test: `src/test/java/.../registry/RegistryResyncTest.java`

**Interfaces:**
- Consumes: `RegistryWriter.register(Session, boolean)` (Task 10), `LifecyclePublisher.publish(Session, EventType)` (Task 12), `LoopRegistry`.
- Produces: `RegistryResync(LoopRegistry loops, RegistryWriter registry, LifecyclePublisher lifecycle, int perLoopPerSecond /* 300 */)`:
  - `void resync()`: với mỗi loop, `execute` để chụp `LongArrayList` các `sessionKey` của session `ACTIVE` lúc đó; sau đó cứ mỗi 100 ms (`eventLoop().schedule`) xử lý `perLoopPerSecond / 10` key. Với mỗi key, session còn tồn tại và còn `ACTIVE` thì gọi `register(s, false)` và `publish(s, EVENT_TYPE_CONNECTED)`.
  - Một lần `resync()` mới trong lúc lần trước chưa xong sẽ hủy lần trước (cờ generation theo loop).
  - `int pending()`: số key còn lại trên mọi loop, dùng cho test và metric `gw.registry.resync.pending`.

- [ ] **Step 1: Viết test thất bại**

```java
@Test void resyncsActiveSessionsAt300PerLoopPerSecond() {
  /* 1 loop, 900 session ACTIVE; RegistryWriter/LifecyclePublisher giả đếm → sau 1 s mô phỏng: 300; sau 3 s: 900 */ }
@Test void skipsSessionsClosedMeanwhile() { }
@Test void newResyncSupersedesRunningOne() { }
```

- [ ] **Step 2:** Run `./mvnw -f gateway-service test -Dtest=RegistryResyncTest`. Expected: lỗi compile.
- [ ] **Step 3:** Hiện thực.
- [ ] **Step 4:** Run `./mvnw -f gateway-service test`. Expected: pass.
- [ ] **Step 5: Commit**

```bash
git add gateway-service
git commit -m "feat(gateway): paced registry resync after re-epoch"
```

---

### Task 15: Áp dụng control — `HelloPolicy`, revoke, blocked, heartbeat theo tenant, migrate cell

**Files:**
- Create: `auth/HelloPolicy.java`, `control/{ControlEnforcer,TenantMigrator}.java`
- Modify: `pipeline/HelloAuthHandler.java`, `pipeline/SessionHandler.java` (REAUTH, cho phép state `DRAINING`), `pipeline/PipelineDeps.java`, `session/{Session,SessionState,SessionTracker}.java`, `protocol/Frames.java` (`reconnect`), `upstream/UpstreamPublisher.java` (không đổi API), `config/{GatewayConfig,ClusterConfig}.java`
- Test: `src/test/java/.../auth/HelloPolicyTest.java`, `control/{ControlEnforcerTest,TenantMigratorTest}.java`, `session/SessionTest.java` (bổ sung), `pipeline/HelloAuthHandlerTest.java` (bổ sung)

**Interfaces:**
- Consumes: store của Task 8 và Task 9; `PrincipalHandshakeLimiter` (M1); `PrincipalIndex`, `LoopRegistry`.
- Produces:
  - `HelloPolicy(String cell, CellMapView cells, RevocationList revocations, TenantLimitsStore tenants, PrincipalHandshakeLimiter limiter, LongSupplier wallMillis)`:
    - `record Candidate(String ticketCell, String tenant, String principal, String device, long jtiHash, String sdkVersion, String origin /* nullable */)`.
    - `record Rejection(CloseCode code, long retryAfterMs, String metricResult)`.
    - `Optional<Rejection> check(Candidate c)`, kiểm đúng thứ tự spec §4.6 bước 3:
      - c: `!cell.equals(ticketCell) || !cells.servesTenant(tenant)` → `WRONG_CELL`, `"wrong_cell"`.
      - d: `revocations.isRevoked(...)` → `REVOKED`, `"revoked"`.
      - e: `policy.blocked()` → `RATE_LIMITED`, retry 300000, `"blocked"`.
      - f: `minSdkVersion` khác rỗng và `SdkVersion.compare(sdk, min) < 0` → `UPGRADE_REQUIRED`, `"sdk_too_old"`.
      - g: `origin != null`, allowlist khác rỗng và không chứa `origin` → `AUTH_FAILED`, `"origin"`.
      - h: `!limiter.tryAcquire(principal)` → `RATE_LIMITED` với retry mặc định, `"rate_limited"`.
    - Check i (tenant cap và mượn quota trên Redis) thuộc M3.
  - `HelloAuthHandler`:
    - Thay tham số `PrincipalHandshakeLimiter` bằng `HelloPolicy` và `TenantLimitsStore`.
    - Sau a/b (chữ ký, device) gọi `policy.check`. Với ticket thì `ticketCell = t.cell()`; với JWT thì `ticketCell = cell`.
    - Bị từ chối → `session.close(code, name, retryAfterMs)`.
    - `CONNECTED.heartbeat_sec = tenants.policy(t).heartbeatSec()`.
  - `SessionHandler.reauth`: dùng `policy.check` với `origin = s.origin()` thay cho lời gọi limiter trực tiếp.
  - `Session`:
    - `close(CloseCode, String reason, long retryAfterMs)`; overload 2 tham số dùng `defaultRetryAfterMs`.
    - `SessionState.DRAINING`.
    - `void drain(long delayMs, boolean reBootstrap)`: gửi `RECONNECT{delay_ms, re_bootstrap}`, state → `DRAINING`, `drainDeadlineSec = now + ceil(delayMs/1000) + 30`.
    - `nextDeadlineSec`: với `DRAINING` là `min(drainDeadline, lastSeen + idle, expMono)`; khi đến hạn drain → `close(GOING_AWAY, "DRAINED")`.
    - Idle timeout = `tenants.policy(tenantId).idleTimeoutSec()`. `Session` nhận `TenantLimitsStore` qua constructor, do `SessionTracker` truyền vào. `HANDSHAKING` vẫn dùng `GatewayLimits`.
  - `Frames.reconnect(Reconnect r)`.
  - `ControlEnforcer(PrincipalIndex principals, LoopRegistry loops, TenantLimitsStore tenants, CellMapView cells, TenantMigrator migrator) implements ControlEventHandler`. Được đăng ký sau `ControlState` trong danh sách handler.
    - `onRevoke(key, …)`: key `p:` → `principals.get(p)`; với mỗi `ConnRef`, `loops.get(idx).execute(...)` đóng session `REVOKED`. Key `d:` hoặc `j:` → mọi loop quét `forEachSession` và đóng session khớp (`principal` + `device`, hoặc `authJtiHash`).
    - `onTenantCfg(cfg)`: chỉ xét scope `TENANT`. Nếu `policy(t).blocked()` là true sau khi `apply` → mọi loop đóng các session của tenant với `OVERLOADED` và retry 300000.
    - `onCellMap(t, c)`: `cells.servesTenant(t) == false` → `migrator.start(t)`, ngược lại `migrator.stop(t)`. `onCellMapRemoved` → `migrator.stop(t)`.
  - `TenantMigrator(LoopRegistry loops, CellMapView cells, LongSupplier randomDelayMs /* [0,5000] */)`:
    - `void start(String tenant)`, `void stop(String tenant)`.
    - `void tick()`: chạy mỗi 5 s trên `gw-tick`. Với mỗi tenant đang migrate và mỗi loop: ở lần đầu ghi lại `batch = max(1, ceil(0.02 × số session ACTIVE của tenant trên loop))`; mỗi tick gọi `drain(randomDelay, true)` cho tối đa `batch` session `ACTIVE` của tenant đó.
    - Tenant tự gỡ khỏi danh sách khi không còn session `ACTIVE` nào trên mọi loop.

- [ ] **Step 1: Viết test thất bại**

```java
// HelloPolicyTest
@Test void checksRunInSpecOrder() { /* tenant vừa bị revoke vừa blocked vừa sai cell → WRONG_CELL; bỏ sai cell → REVOKED; … */ }
@Test void blockedTenantGets4009With300s() { }
@Test void oldSdkGets4010() { }
@Test void originOutsideAllowlistGets4001AndEmptyAllowlistAllowsAll() { }
@Test void rateLimitIsLastAndOnlyConsumedWhenOtherChecksPass() { /* revoked → limiter.tryAcquire không bị gọi */ }
// ControlEnforcerTest (LoopFixture, EmbeddedChannel)
@Test void principalRevokeClosesAllItsSessionsWith4003() { }
@Test void jtiRevokeClosesOnlyMatchingSession() { }
@Test void tenantBlockedClosesOpenSessionsWith4013RetryAfter300s() { /* close reason chứa "retryAfter=300000" */ }
// TenantMigratorTest
@Test void drainsTwoPercentPerTickWithReBootstrap() { /* 100 session t1 trên 1 loop → tick: 2 session nhận RECONNECT re_bootstrap=true, state DRAINING */ }
@Test void stopsWhenTenantMappedBack() { }
// SessionTest
@Test void drainingClosesWith1001AfterDelayPlus30s() { }
@Test void tenantHeartbeatSwitchChangesIdleTimeoutImmediately() { /* heartbeat tenant 10 → idle 30 s thay vì 90 s */ }
// HelloAuthHandlerTest
@Test void revokedPrincipalGets4003() { }
@Test void foreignCellTenantGets4011() { }
@Test void connectedUsesTenantHeartbeat() { }
```

- [ ] **Step 2:** Run `./mvnw -f gateway-service test -Dtest='HelloPolicyTest,ControlEnforcerTest,TenantMigratorTest,SessionTest,HelloAuthHandlerTest'`. Expected: lỗi compile.
- [ ] **Step 3:** Hiện thực. Trước khi sửa, chạy `impact` cho `Session`, `HelloAuthHandler`, `SessionHandler` và `SessionTracker`. Cập nhật mọi test M1 đang tạo `Session` hoặc `SessionTracker` trực tiếp, có thể qua `LoopFixture`.
- [ ] **Step 4:** Run `./mvnw -f gateway-service test`. Expected: mọi unit test pass.
- [ ] **Step 5: Commit**

```bash
git add gateway-service
git commit -m "feat(gateway): enforce revocations, tenant policies and cell migration on HELLO, REAUTH and open sessions"
```

---

### Task 16: IT end-to-end M2a

**Files:**
- Modify: `src/it/java/com/pangate/gateway/it/GatewayLocalIT.java` (nếu cần: dùng `InfraContainers.flushRedis()` giữa các test)
- Create: `src/it/java/com/pangate/gateway/it/GatewayClusterIT.java`

**Interfaces:**
- Consumes: mọi task trước; `GatewayClient` của devtools (dùng `send` và `await`); `InfraContainers`.
- Produces: một gateway thật cho mỗi class, chạy profile `local` với infra trỏ vào container, `--gateway.node-id=gw-it-<random>`. Test đọc trực tiếp Redis (Lettuce) và Kafka (`KafkaConsumer`, `assign` + `seekToEnd` trước khi hành động).

- [ ] **Step 1: Viết các kịch bản IT**

```java
@Test void registryEntryWrittenOnHelloAndRemovedOnClose() { /* HGET reg:{p} connId bắt đầu bằng "<nodeId>|<epoch>|" ; close → await HEXISTS false */ }
@Test void connectedCarriesSeqAndSeqEpochFromRedis() { /* SET seq:{p} 41; HSET seqepochs <slot> 3; ctx.getBean(RedisSeqReader.class).refreshEpochs() → CONNECTED.seq 41, seq_epoch 3 */ }
@Test void lifecycleConnectedAndDisconnectedPublished() { /* c1.gw.lifecycle: CONNECTED rồi DISCONNECTED cùng conn_id, close_code 1000 */ }
@Test void upstreamCmdProducesRecordAndUpstreamAck() { /* record trên c1.gw.upstream.cmd, header tenant-id t1, UPSTREAM_ACK msg_id khớp */ }
@Test void unknownUpstreamTypeGetsError4000AndStaysOpen() { }
@Test void revokePrincipalClosesSessionWithin10sAndBlocksHello() {
  /* publish ControlEvent revoke principal lên gw.control (key revoke:p:<p>) → client nhận close 4003 trong < 10 s; HELLO lại → 4003 */ }
@Test void tenantBlockedClosesOpen4013AndRejectsHello4009() { }
@Test void cellMapMoveSendsReconnectReBootstrap() { /* CellMap t1 → cell-2 → RECONNECT re_bootstrap=true trong ≤ 6 s; HELLO mới → 4011 */ }
@Test void controlCatchUpGatesReadinessAndReplaysRevokes() { /* revoke publish TRƯỚC khi boot gateway thứ hai → /ready 200 rồi HELLO principal đó → 4003 */ }
@Test void leaseHasTtl15AndRestartTakesHigherEpoch() { /* TTL node:{id} ∈ (0,15]; ctx.close(); start lại cùng nodeId → epoch +1, /ready 200 */ }
@Test void redisPauseKeepsSessionsAndReEpochsOnRecovery() {
  /* client A mở; pause redis 20 s: A vẫn PING/PONG; /ready 503 lý do "node lease lost"; HELLO client B vẫn CONNECTED (seq_unknown);
     unpause → trong ≤ 15 s epoch +1, reg:{A} và reg:{B} có giá trị với epoch mới, lifecycle CONNECTED của A với epoch mới */ }
@Test void startupFailsFastWithDiagnosticWhenKafkaMissing() {
  /* start với --gateway.kafka.bootstrap=localhost:1 --gateway.startup.dependency-timeout=3s → exception gốc là
     DependencyUnavailableException, thông điệp analyzer "Kafka localhost:1 is not reachable after 3 s" */ }
```

- [ ] **Step 2: Chạy toàn bộ**

Run: `make bootstrap && ./mvnw -f gateway-service verify`
Expected: unit test, 12 IT của `GatewayLocalIT` và mọi IT mới đều xanh. Test pause Redis có thể mất khoảng 40 s.

- [ ] **Step 3: Sửa lỗi lộ ra** (nếu có) trong task sở hữu code đó, chạy lại Step 2.

- [ ] **Step 4: Commit**

```bash
git add gateway-service
git commit -m "test(gateway): M2a end-to-end ITs (registry, seq, lifecycle, upstream, control, lease drills)"
```

---

### Task 17: Smoke, compose dev, script, CI và tiêu chí hoàn thành M2a

**Files:**
- Modify: `gateway-devtools/src/main/java/com/pangate/devtools/client/GatewayClient.java`, `cmd/SmokeCommand.java` (cùng test của chúng), `platform-devenv/docker-compose.yml` (gateway), `platform-devenv/README.md`, `gateway-service/README.md`, `README.md`, `.github/workflows/ci.yml`, `docs/superpowers/specs/2026-11-03-gateway-service-design.md` (§12: ghi M2 chia thành M2a/M2b), plan này (mục kết quả đo)

**Interfaces:**
- Produces:
  - `GatewayClient.upstream(String type, String msgId, byte[] payload, Duration timeout) → Frame`: trả frame `UPSTREAM_ACK` hoặc `ERROR` có `msg_id` hoặc mã tương ứng.
  - `SmokeCommand`: sau `CONNECTED` và `PING`, gửi `upstream("cmd", <ulid>, "smoke".bytes)` và yêu cầu nhận `UPSTREAM_ACK`. In ra `SMOKE OK (connected, pong, upstream-ack)`.
  - CI:
    - Job `local-start` thêm `make infra-up` ngay sau `make bootstrap`.
    - Job `build` giữ `make verify` (Testcontainers chạy được trên Docker của `ubuntu-latest`).
    - Job `dev-start` không đổi lệnh, nhưng compose giờ có Kafka và Redis.
    - Thêm bước `docker compose … logs kafka redis` khi job fail.

- [ ] **Step 1: Viết test thất bại cho devtools**

```java
// GatewayClientTest (FakeGatewayServer trả UPSTREAM_ACK)
@Test void upstreamReturnsAck() { }
```

Run: `./mvnw -f gateway-devtools test -Dtest=GatewayClientTest`. Expected: lỗi compile.

- [ ] **Step 2:** Hiện thực devtools, compose, README và CI. Run `./mvnw -f gateway-devtools install`. Expected: pass.

- [ ] **Step 3: R1 và R3 ở local**

Run: `make bootstrap && make local-check`
Expected: kết thúc bằng `LOCAL START OK`. Restart ngay trên cùng nodeId `gateway-local` không bị fence (log có epoch tăng).

- [ ] **Step 4: Heap và tải idle**

Run: `make memory-check && make load-check N=18000`
Expected: heap/session < 5120 B; lag p99 < 20 ms. Ghi các con số vào mục kết quả.

- [ ] **Step 5: R2 ở dev**

Run: `make images && make up && make smoke; make down`
Expected: mọi container healthy trong ≤ 90 s; in `SMOKE OK (connected, pong, upstream-ack)`.

- [ ] **Step 6: Ghi kết quả và cập nhật spec**

Thêm vào cuối plan này mục `## Kết quả đo M2a`, một bảng gồm: ready local (s), stop (s), compose healthy (s), heap/session (B), lag p99 18k (ms), thời gian revoke → close trong IT (s), thời gian lease hết hạn trong drill (s). Ở spec §12, thêm một dòng dưới bảng: "M2 được hiện thực qua hai plan: M2a (infra, registry, upstream, control) và M2b (push, broadcast, kick, fake-router/fake-pushapi, smoke push → ack)."

- [ ] **Step 7: Kiểm thay đổi rồi commit**

```bash
node .gitnexus/run.cjs detect-changes --scope staged --repo .
git add -A
git commit -m "chore: M2a smoke with upstream ack, compose infra wiring, CI infra-up and exit measurements"
```

**Tiêu chí hoàn thành M2a** (phần của M2 trong spec §12 thuộc plan này):
- R1 với Kafka và Redis trên `localhost` (sau `make infra-up`); R2 với compose đầy đủ, smoke đến `UPSTREAM_ACK`; R3, R4.
- Drill kill node: lease hết hạn ≤ 15 s; khởi động lại nhận epoch mới (IT `leaseHasTtl15AndRestartTakesHigherEpoch`, `redisPauseKeepsSessionsAndReEpochsOnRecovery`).
- Revoke đóng session trong < 10 s (IT).
- IT M2a xanh; heap/session < 5 KB; lag p99 < 20 ms với 18k session.
- E2E connect → push → ack → disconnect thuộc tiêu chí của **M2b**.

## Kết quả đo M2a

Đo trong container phát triển (`ulimit -n` 20000), Docker 1 node Kafka 3.9.1 và Redis 7.4.2.

| Chỉ số | Kết quả | Ngưỡng |
|---|---|---|
| Ready ở local (jar, profile `local`, infra trên `localhost`) | 5,8 s | ≤ 30 s (R1) |
| Dừng bằng SIGTERM | 0,5 s; chạy lại ngay lấy epoch mới (3 → 4), không bị fence | ≤ 15 s (R3) |
| Compose dev từ trạng thái lạnh tới healthy | 22 s; `SMOKE OK (connected, pong, upstream-ack, resume)` | ≤ 90 s (R2) |
| Heap/session (10k idle, sau warm-up) | 4.889 B | < 5.120 B |
| Lag p99 với 18k session idle | 1,6 ms | < 20 ms |
| Revoke principal → đóng 4003 | Đạt trong `GatewayClusterIT` | < 10 s |
| Drill lease | TTL ≤ 15 s; restart nhận epoch +1; pause Redis → `/ready` 503 "node lease lost", sau khi Redis hồi phục thì có epoch mới và registry được resync | ≤ 15 s |
| Test | 261 unit + 52 IT (`GatewayLocalIT` 12, `GatewayClusterIT` 13, component IT 27) | xanh |

Ghi chú về phép đo heap: `idle-memory.sh` giờ chụp baseline sau một vòng warm-up 1000 session. Nếu không, khoảng 5–6 MB pool cố định của Kafka producer và Lettuce bị chia cho N, và kết quả phụ thuộc vào N (đo được 5.231 B với N=10k và khoảng 6.000 B với N=5k). Phần tăng thật so với M1 (4.690 B) là khoảng 200 B, chủ yếu do `Session` thêm 64 B field (rate limit, áp lực upstream, drain).
