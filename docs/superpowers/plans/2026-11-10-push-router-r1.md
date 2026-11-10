# push-router-service R1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship `push-router-service`, which consumes `c1.push.requests`, delivers to gateways over gRPC `PushStream`, applies the delivery outcome table, and emits offline / delivered / NODE_DOWN. The devenv then runs Kafka → router → gateway end to end.

**Architecture:**
- New Maven module, Spring Boot as the shell only.
- Pure-Java core: `DeliveryJob`, `RegistryLookup`, `NodeDirectory`, `GatewayLinks`, `OutcomeRecorder`, `NodeDownWatcher`.
- Driven by Confluent Parallel Consumer: `KEY` ordering, imperative `poll`, worker pool on virtual threads.
- Gateway publishes `node:addr:{id}`. Devtools and `fake-pushapi` produce `PushRecord`s to Kafka.

**Tech Stack:** Java 21, Spring Boot (platform-parent), `io.confluent.parallelconsumer:parallel-consumer-core:0.5.3.3`, grpc-netty-shaded, Lettuce, kafka-clients 3.9.x, Testcontainers (Kafka `apache/kafka:3.9.1`, Redis `redis:7.4.2`), platform-contracts-testkit.

**Spec:** `docs/superpowers/specs/2026-11-10-push-router-design.md` (cited "spec §N"). The outcome table it builds on is `docs/superpowers/specs/2026-11-10-push-contracts-design.md` §1.1a (cited "contracts §1.1a").

## Global Constraints

- **Module:** `push-router-service`, package `com.pangate.router`, parent `platform-parent` 0.1.0 (`relativePath ../platform-parent/pom.xml`), version 0.1.0.
- **Layering:** core classes import no Spring; only `config/` and `RouterApplication` do.
- **Parallel Consumer:**
  - `parallel-consumer-core` pinned to 0.5.3.3;
  - `ProcessingOrder.KEY`;
  - `CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS`;
  - worker pool from `VirtualThreadParallelConsumer.setupWorkerPool(int)` using `Thread.ofVirtual().factory()`.
- **Defaults (spec §5):**
  - `router.cell=c1`;
  - `router.kafka.group=push-router`, `router.kafka.max-concurrency=20000`;
  - `router.ack-timeout=6s`;
  - `router.retries=[100ms, 400ms]`;
  - `router.registry.cache=2s`, `router.registry.timeout=200ms`;
  - `router.directory-refresh=1s`;
  - `router.batch.max=256`, `router.batch.linger=2ms`;
  - `router.max-in-flight-per-node=10000`.
- **Management port:** :8081, or 8083 in profile `local`. `/ready` is 503 when the directory has not refreshed for 10 s.
- **gRPC header:** `pangate-push-version: 1`; keepalive 10 s.
- **Kafka:**
  - every record produced uses `PushKeys` keys and `PushHeaders.write` headers;
  - producer is idempotent, `acks=all`, zstd;
  - outcome produce blocks until acked.
- **Contract rules:** every decision about a record follows contracts §1.1a exactly (retry set, reason mapping, `attempts`, receipt statuses).
- **Logging:** never log payloads or frames. Log `topic`, `partition`, `offset`, `msg_id`.
- **Tests:** unit tests in `src/test/java`, ITs in `src/it/java` (`*IT`, failsafe), same pom layout as `auth-service`.

## Review Focus

1. The same principal holds two connections on two nodes, and one node dies mid-delivery. The record is still delivered through the other connection and is not sent offline (Task 5).
2. Expected: a gateway answers no ACK at all (the stream hangs). After `ack-timeout` the item counts as `NACK_TIMEOUT` and the record does not hang forever (Task 5).
3. Expected: a record whose `PushRecord` bytes are corrupt is skipped as invalid and does not block the partition (Task 6).
4. Expected: a gateway restarts with a new epoch and the same address. The old link closes, a new link opens, and in-flight futures complete `NACK_CLOSED` (Task 3, Task 4).
5. A burst of 20 MESSAGEs to one principal arrives in offset order (KEY ordering) (Task 6).

---

### Task 1: Gateway publishes `node:addr:{id}`

**Files:**
- Modify: `gateway-service/src/main/java/com/pangate/gateway/registry/RegistryKeys.java`: add `static String addr(String nodeId)` returning `"node:addr:{" + nodeId + "}"`.
- Modify: `gateway-service/src/main/java/com/pangate/gateway/registry/NodeIdentity.java`:
  - add `public void advertise(String hostPort)`, which stores the address and runs `SET node:addr:{id} hostPort EX 15` at once;
  - each successful `renewOnce()` / `reEpoch()` re-sets the key when an address is stored;
  - `stop()` deletes the key.
- Modify: `gateway-service/src/main/java/com/pangate/gateway/downstream/GrpcServerLifecycle.java`: after a successful bind, call `identity.advertise(advertiseHost + ":" + port())`, where `port()` is the bound port.
- Modify: `gateway-service/src/main/java/com/pangate/gateway/config/GatewayProperties.java`: add `String advertiseHost` to `Grpc`.
  - Default `${HOSTNAME:localhost}` in `application.yml`.
  - `application-dev.yml`: `gateway`.
  - `application-local.yml`: `127.0.0.1`.
  - Wire it in `PushConfig`.
- Test: `gateway-service/src/test/java/com/pangate/gateway/registry/NodeAddressTest.java` (unit, with the same Redis fake / mocks `NodeIdentity`'s existing tests use); `gateway-service/src/it/java/com/pangate/gateway/registry/NodeAddressIT.java`.

**Interfaces:**
- Produces: Redis key `node:addr:{nodeId}` = `host:port`, TTL 15 s, refreshed with the lease, deleted on stop.

- [ ] **Step 1: Write the failing tests**

```java
// NodeAddressIT (InfraContainers Redis, gateway started like GatewayEdgeIT.start, gateway.grpc.port=0)
@Test void addressIsTheBoundGrpcPortAndLivesWithTheLease() {
  // after start: GET node:addr:{nodeId} == "127.0.0.1:" + actual gRPC port (profile local); TTL in (0, 15]
  // after 6 s: TTL refreshed (> 9); after ctx.close(): key absent
}
```

- [ ] **Step 2: Run** `./mvnw -f gateway-service verify -Dtest=NONE -Dsurefire.failIfNoSpecifiedTests=false -Dit.test=NodeAddressIT`. Expected: FAIL (key absent).
- [ ] **Step 3: Implement.** Run GitNexus `impact` on `NodeIdentity`, `GrpcServerLifecycle` and `GatewayProperties` first, and record the risk.
- [ ] **Step 4: Verify.** The IT passes; `./mvnw -f gateway-service verify` is green with no existing test edited.
- [ ] **Step 5: Commit** `feat(gateway): publish node:addr:{id} with the bound gRPC port`

---

### Task 2: Router module skeleton and virtual-thread Parallel Consumer

**Files:**
- Create: `push-router-service/pom.xml`.
  - Deps: `platform-contracts`, `platform-spring-starter`, `spring-boot-starter-web` (management port only), `spring-boot-starter-actuator`, `micrometer-registry-prometheus`, `parallel-consumer-core` 0.5.3.3, `kafka-clients`, `lettuce-core`, `grpc-netty-shaded`, `grpc-stub`, `grpc-protobuf`.
  - Test deps: `spring-boot-starter-test`, `awaitility`, Testcontainers `kafka` + `junit-jupiter`, `platform-contracts-testkit`, `gateway-service` (main jar), `gateway-devtools`.
  - Plugins: build-helper (`src/it/java`), surefire, failsafe, Spring Boot repackage with classifier `exec`, Jib `pangate/push-router-service:dev`, ports 8081.
- Create: `push-router-service/src/main/java/com/pangate/router/RouterApplication.java`, `config/RouterProperties.java` (record bound to `router`, defaults per Global Constraints), `consumer/VirtualThreadParallelConsumer.java`
- Create: `src/main/resources/application.yml`, `application-local.yml`, `application-dev.yml` (spec §5 table)
- Modify: `Makefile` (`MODULES` adds `push-router-service` after `gateway-service`); `platform-devenv/scripts/release-check.sh` (version loop)
- Test: `push-router-service/src/test/java/com/pangate/router/consumer/VirtualThreadParallelConsumerTest.java`

**Interfaces:**
- Produces:
  - `RouterProperties(String cell, Kafka kafka, Duration ackTimeout, List<Duration> retries, Registry registry, Duration directoryRefresh, Batch batch, int maxInFlightPerNode, Grpc grpc)`
  - `final class VirtualThreadParallelConsumer<K, V> extends ParallelEoSStreamProcessor<K, V>`, overriding `protected ThreadPoolExecutor setupWorkerPool(int poolSize)`

- [ ] **Step 1: Write the failing test.** `workersAreVirtualThreads`: use a `MockConsumer` with one partition and three records, `ParallelConsumerOptions` with `ordering=KEY` and `maxConcurrency=4`. `poll` records `Thread.currentThread().isVirtual()`. Assert all are `true` and all 3 records are processed.
- [ ] **Step 2: Run** `./mvnw -q -f push-router-service test -Dtest=VirtualThreadParallelConsumerTest`. Expected: FAIL (no module).
- [ ] **Step 3: Implement.** `setupWorkerPool` returns `new ThreadPoolExecutor(n, n, 0, MILLISECONDS, new LinkedBlockingQueue<>(), Thread.ofVirtual().name("pc-worker-", 0).factory())`.
- [ ] **Step 4: Verify.** The test passes; `make release-check` is OK.
- [ ] **Step 5: Commit** `feat(router): module skeleton; Parallel Consumer on virtual threads`

---

### Task 3: NodeDirectory and NodeDownWatcher

**Files:**
- Create: `push-router-service/src/main/java/com/pangate/router/nodes/Node.java`, `NodeDirectory.java`, `NodeSource.java`, `RedisNodeSource.java`, `NodeDownWatcher.java`, `LifecyclePublisher.java`, `KafkaLifecyclePublisher.java`
- Test: `push-router-service/src/test/java/com/pangate/router/nodes/NodeDirectoryTest.java`, `NodeDownWatcherTest.java`; `push-router-service/src/it/java/com/pangate/router/nodes/RedisNodeSourceIT.java`

(Paths in Tasks 4–6 are relative to `push-router-service/src/{main,test,it}/java/com/pangate/router/`.)

**Interfaces:**
- Produces:
  - `record Node(String id, long epoch, String address, boolean draining)`
  - `interface NodeSource { Map<String, Node> fetch(long nowMs) throws Exception; }`. The Redis implementation runs `ZRANGEBYSCORE nodes:live (now-30000) +inf`, then pipelines `GET node:{id}`, `GET node:addr:{id}` and `EXISTS node:draining:{id}`. A node is kept only when both its epoch and its address exist.
  - `final class NodeDirectory`:
    - `NodeDirectory(NodeSource, LongSupplier wallMs, LongSupplier monoMs, Duration staleAfter=10s)`
    - `void refresh()`: on failure keeps the previous snapshot and logs a warning;
    - `Map<String, Node> live()`;
    - `Optional<Node> node(String id)`;
    - `boolean fresh()`: the last successful refresh is at most 10 s old;
    - `void onChange(BiConsumer<Map<String, Node>, Map<String, Node>> listener)`, called with (old, new) after each successful refresh.
  - `final class NodeDownWatcher implements BiConsumer<Map<String,Node>, Map<String,Node>>`: for each id in old that is missing from new, or whose epoch changed, it calls `LifecyclePublisher.nodeDown(id, oldEpoch)` once per `id:epoch`.
  - `interface LifecyclePublisher { void nodeDown(String nodeId, long epoch); }`. The Kafka implementation sends `ConnectionEvent{NODE_DOWN, node_id, epoch, cell_id, at_ms, event_id ULID}` with key `node_id` and an empty tenant header to `PushTopics.lifecycle(cell)`.

- [ ] **Step 1: Write the failing tests:**
  - `refreshKeepsTheLastSnapshotOnFailure`;
  - `freshTurnsFalseAfter10sWithoutSuccess`;
  - `drainingNodesStayLive`;
  - `lostLeaseEmitsNodeDownOnce` (two consecutive refreshes without the node → one call);
  - `epochChangeEmitsNodeDownForTheOldEpoch`;
  - `nodesNeverSeenEmitNothing`;
  - IT `RedisNodeSourceIT.readsLeaseAddressAndDraining`, which seeds keys in a Redis container.
- [ ] **Step 2: Run** `./mvnw -f push-router-service verify -Dtest='NodeDirectoryTest,NodeDownWatcherTest' -Dit.test=RedisNodeSourceIT`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Verify.** All pass.
- [ ] **Step 5: Commit** `feat(router): node directory from Redis and NODE_DOWN on lease loss`

---

### Task 4: RegistryLookup and GatewayLinks

**Files:**
- Create: `registry/RegistryLookup.java`, `registry/LiveConnection.java`, `links/GatewayLinks.java`, `links/GatewayLink.java`
- Test: `registry/RegistryLookupTest.java`, `links/GatewayLinksTest.java` (in-process gRPC server implementing `PushStreamGrpc.PushStreamImplBase`)

**Interfaces:**
- Consumes: `NodeDirectory` (Task 3); `RegistryValues` from the testkit as the reference for the parser.
- Produces:
  - `record LiveConnection(String connId, String nodeId, long epoch, Optional<String> device)`
  - `final class RegistryLookup`:
    - `RegistryLookup(Function<String, CompletionStage<Map<String,String>>> hgetall, NodeDirectory dir, Duration cache, Duration timeout, LongSupplier monoMs)`;
    - `List<LiveConnection> live(String principal)` throws `RegistryUnavailableException` on timeout or error;
    - `void invalidate(String principal)`.
    - Parsing is local to the router and agrees with `RegistryValues.cases()`.
    - An entry is kept only if `dir.node(nodeId)` exists, its epoch matches, and it is not draining.
  - `final class GatewayLinks implements AutoCloseable`:
    - `GatewayLinks(NodeDirectory, Function<Node, ManagedChannel> channels, int batchMax, Duration linger, int maxInFlight)`;
    - `CompletableFuture<AckItem.Status> send(String nodeId, PushItem item)`. The item gets a fresh `delivery_id`. For MESSAGE items of RELIABLE/ORDERED frames the future completes from the matching `AckItem`; for the others it completes `STATUS_WRITTEN` once the batch was handed to the stream. It throws `NodeUnavailableException` if the node is not live.
    - `void closeNode(String nodeId)`: every pending future → `STATUS_NACK_CLOSED`.
    - It reacts to `NodeDirectory.onChange`: when a node disappears or its epoch changes, it calls `closeNode`.

- [ ] **Step 1: Write the failing tests.**

```java
// RegistryLookupTest
@Test void parsesEveryReferenceCase() { /* for each RegistryValues.cases() with expected present: live() returns it when its node is live */ }
@Test void dropsStaleEpochsDrainingNodesAndUnknownNodes() ;
@Test void cachesForTwoSecondsAndInvalidateForcesAFetch() ;
@Test void timeoutRaisesRegistryUnavailable() ;  // hgetall never completes, 200 ms
// GatewayLinksTest (in-process server, header pangate-push-version: 1 required)
@Test void batchesUpTo256OrAfter2ms() ;          // 300 items -> batches of 256 and 44; 1 item -> sent after ~2 ms
@Test void acksCompleteTheMatchingFutures() ;     // server acks delivery_ids out of order
@Test void streamErrorCompletesPendingWithNackClosed() ;
@Test void epochChangeClosesTheLinkAndReconnects() ; // Review Focus 4
@Test void maxInFlightBlocksTheSender() ;         // limit 2: third send blocks until an ack arrives
@Test void ephemeralBroadcastAndControlCompleteWhenWritten() ;
```

- [ ] **Step 2: Run** `./mvnw -q -f push-router-service test -Dtest='RegistryLookupTest,GatewayLinksTest'`. Expected: FAIL.
- [ ] **Step 3: Implement.**
  - Reconnect backoff 100 ms → 5 s with jitter.
  - The response observer maps `AckItem.delivery_id` to its future.
  - `onCompleted` and `onError` → `closeNode`.
- [ ] **Step 4: Verify.** All pass.
- [ ] **Step 5: Commit** `feat(router): registry lookup and gRPC links to gateways`

---

### Task 5: DeliveryJob and OutcomeRecorder

**Files:**
- Create: `delivery/DeliveryJob.java`, `delivery/OutcomeRecorder.java`, `delivery/KafkaOutcomeRecorder.java`, `delivery/DeliveryResult.java`
- Test: `delivery/DeliveryJobTest.java` (fake `RegistryLookup` via an interface, fake links, fake recorder, fake clock)

**Interfaces:**
- Consumes: `RegistryLookup.live/invalidate`, `GatewayLinks.send`, `NodeDirectory.live` (Tasks 3–4).
- Produces:
  - `interface Connections { List<LiveConnection> live(String principal); void invalidate(String principal); }`, implemented by `RegistryLookup`.
  - `interface Links { CompletableFuture<AckItem.Status> send(String nodeId, PushItem item); }`, implemented by `GatewayLinks`.
  - `interface OutcomeRecorder { void offline(PushRecord r, OfflineRecord.Reason reason, int attempts); void delivered(PushRecord r, LiveConnection c, AckItem.Status s); }`. The Kafka implementation blocks until acked.
  - `enum DeliveryResult { DELIVERED, OFFLINE, DROPPED }`
  - `DeliveryJob(Connections, Links, Supplier<Collection<Node>> liveNodes, OutcomeRecorder, LongSupplier wallMs, Duration ackTimeout, List<Duration> retries, Sleeper sleeper)`. `DeliveryResult run(PushRecord r)` follows spec §2 and contracts §1.1a exactly. A `RegistryUnavailableException` propagates as is (PC retries the record).

- [ ] **Step 1: Write the failing tests**, one per row of contracts §1.1a and spec §2:
  - `expiredReliableGoesOfflineExpiredWithZeroAttempts`
  - `expiredEphemeralIsDropped`
  - `noSessionReliableGoesOfflineNoSession`
  - `noSessionEphemeralIsDropped`
  - `ackedWithReceiptRecordsDelivered`
  - `writtenCountsAsDeliveredAndRecordsWritten`
  - `timeoutRetriesTwiceThenOfflineWithThreeAttempts` (sleeps 100 ms, 400 ms)
  - `closedRetriesAndDeliversOnAnotherNode`
  - `overflowShedAndSlowConsumerAreNotRetried`
  - `oneOfTwoConnectionsDeliversSoNoOffline`
  - `nodeDiesMidDeliveryOtherConnectionDelivers` (Review Focus 1)
  - `ackNeverArrivesCountsAsTimeoutAfterAckTimeout` (Review Focus 2)
  - `deviceTargetOnlyUsesMatchingConnections`
  - `ephemeralCompletesWhenWritten`
  - `broadcastGoesOnceToEveryLiveNonDrainingNode`
  - `controlGoesOncePerNodeHoldingThePrincipal`
  - `registryUnavailablePropagates`
  - `lastFailureOnDeadNodeIsNodeGone`
- [ ] **Step 2: Run** `./mvnw -q -f push-router-service test -Dtest=DeliveryJobTest`. Expected: FAIL.
- [ ] **Step 3: Implement.** Each PushItem carries a fresh `delivery_id` from `GatewayLinks`; `enqueued_at_ms = accepted_at_ms`.
- [ ] **Step 4: Verify.** All pass.
- [ ] **Step 5: Commit** `feat(router): DeliveryJob applies the delivery outcome table`

---

### Task 6: Wiring, RouterConsumer and the end-to-end IT

**Files:**
- Create: `consumer/RouterConsumer.java`, `config/RouterConfig.java`, `config/RouterHealth.java` (readiness: `NodeDirectory.fresh()`), `config/RouterMetrics.java`
- Test:
  - `consumer/RouterConsumerTest.java` (invalid bytes are skipped);
  - `it/.../RouterIT.java`, with Testcontainers Kafka + Redis, a real gateway (`GatewayApplication`, started as in `GatewayEdgeIT.start` with profile local, `gateway.grpc.port=0` and the containers' URIs), the router application started with `SpringApplicationBuilder`, `PushPathScenario` and devtools `GatewayClient`.

**Interfaces:**
- Consumes: everything from Tasks 2–5.
- Produces: `RouterConsumer.start()` / `stop(Duration)`. `poll` parses `PushRecord`. On failure, or on `PushRecordRules` violation, it counts `router.records{result=invalid}`, logs topic/partition/offset and returns. Otherwise it runs `DeliveryJob.run`. SIGTERM stops polling and waits for in-flight work for at most 10 s.

- [ ] **Step 1: Write the failing tests.**

```java
// RouterConsumerTest
@Test void corruptRecordIsSkippedAndDoesNotBlockThePartition() ; // Review Focus 3: bytes 0xff then a valid record -> second processed
// RouterIT
@Test void clientReceivesPushAndAckRecordsDelivered() ;          // receipt=true -> awaitDelivered
@Test void offlinePrincipalProducesOfflineNoSession() ;
@Test void silentClientTimesOutToOfflineWithThreeAttempts() ;   // client paused, ack-timeout 2s in this test
@Test void deviceTargetReachesOnlyThatDevice() ;
@Test void broadcastReachesSubscribers() ;
@Test void controlKickClosesTheOlderSession4012() ;
@Test void stoppingTheGatewayEmitsNodeDown() ;
@Test void restartingTheRouterRedeliversUnfinishedRecords() ;   // stop router while client withholds ACK; restart; client sees same msg_id again
@Test void twentyMessagesToOnePrincipalArriveInOrder() ;        // Review Focus 5
```

- [ ] **Step 2: Run** `./mvnw -f push-router-service verify -Dtest=RouterConsumerTest -Dit.test=RouterIT`. Expected: FAIL.
- [ ] **Step 3: Implement.** Metrics per spec §5; `/ready` from `RouterHealth`.
- [ ] **Step 4: Verify.** Pass; `./mvnw -f push-router-service verify` is green.
- [ ] **Step 5: Commit** `feat(router): consumer wiring, health, metrics; end-to-end IT with a real gateway`

---

### Task 7: Devtools push, fake-pushapi over Kafka, devenv and smoke

**Files:**
- Create: `gateway-devtools/src/main/java/com/pangate/devtools/cmd/PushCommand.java`, `AwaitOfflineCommand.java`, `devtools/push/PushProducer.java`.
  - `PushProducer` builds `PushRecord`s:
    - ULID `msg_id`;
    - for RELIABLE/ORDERED to a principal, `INCR seq:{p}` on Redis and `seq_epoch` 0;
    - validated with `PushRecordRules`;
    - keys and headers per testkit.
- Modify: `gateway-devtools/.../router/FakePushApi.java`: a KICK from `SendStream` becomes `PushRecord{CONTROL, kick}` produced to `c1.push.requests`; it no longer dials the gateway. Answer `ok=true, code=CODE_OK` once the produce is acked.
- Modify: `gateway-devtools/.../cmd/SmokeCommand.java`:
  - new steps `PUSH via Kafka` (produce a MESSAGE to the smoke principal, client receives the PUSH and ACKs) and `OFFLINE` (MESSAGE to `t1:nobody-<random>` → `OfflineRecord REASON_NO_SESSION`);
  - new options `--kafka` and `--redis`;
  - `--no-router` keeps the old fake-router push step.
- Modify: `gateway-devtools/pom.xml` (deps `platform-contracts-testkit` at compile scope, for `PushRecordRules`/`PushKeys`/`PushHeaders`; `lettuce-core`)
- Modify: `platform-devenv/docker-compose.yml`:
  - service `router` (profile `app`, `SPRING_PROFILES_ACTIVE: dev`, healthcheck `/ready` on 8081 with the router's `HealthCheck` main, depends on kafka-init completed, redis healthy, gateway healthy);
  - `fake-pushapi` gets `--kafka kafka:29092`.
- Modify: `Makefile` (`images` builds the router; `router-local`; `smoke` passes `--kafka kafka:29092 --redis redis://redis:6379`)
- Test: `gateway-devtools/src/test/java/com/pangate/devtools/push/PushProducerTest.java`, `gateway-devtools/src/test/java/com/pangate/devtools/router/FakePushApiKafkaTest.java`; end to end via `make images up smoke down`.

**Interfaces:**
- Consumes: testkit `PushRecordRules`, `PushKeys`, `PushHeaders`, `PushTopics`.

- [ ] **Step 1: Write the failing tests.** `FakePushApiKafkaTest`: a KICK request sent over gRPC `SendStream` produces one `PushRecord{KIND_CONTROL, control.kick}` with key = principal and conventional headers to a `MockProducer`, and is answered `ok=true, code=CODE_OK`. `PushProducerTest`, using `MockProducer` and a fake seq source:
  - a principal message gets seq = INCR result and passes `PushRecordRules`;
  - a device message has no seq;
  - a broadcast has `frame.topic` set;
  - the key and headers match the testkit.
- [ ] **Step 2: Run** `./mvnw -q -f gateway-devtools test -Dtest='PushProducerTest,FakePushApiKafkaTest'`. Expected: FAIL.
- [ ] **Step 3: Implement** devtools, compose, Makefile.
- [ ] **Step 4: Verify.**
  - `./mvnw -f gateway-devtools test` is green.
  - `make images up smoke down` prints `PUSH via Kafka`, `OFFLINE` and `SMOKE OK`, and the router log shows the deliveries.
- [ ] **Step 5: Commit** `feat(devenv): router in compose; devtools push and fake-pushapi over Kafka; smoke covers the router`

---

### Task 8: Docs, ADRs, measurement

**Files:**
- Create: `docs/adr/0019-router-parallel-consumer-virtual-threads.md`, `0020-node-addr-key.md`, `0021-devenv-kafka-front-door.md`; `push-router-service/README.md` (run modes, config, metrics, failure behaviour)
- Modify: `docs/adr/README.md`, `CHANGELOG.md`, `README.md` (module list, build order), spec status line with a measurement.
  - Measurement: `devtools push` loop of 10 000 MESSAGEs to 1 000 principals on the dev machine; record the throughput and p50/p99 from `router.delivery.latency`.
- [ ] **Step 1:** Write the docs.
- [ ] **Step 2: Verify.** `make release-check` prints `release-check OK 0.1.0`; `make verify` is green.
- [ ] **Step 3: Commit** `docs: push-router README, ADR 0019-0021, changelog, measurement`

---

## Exit (spec §9) → tasks

| Criterion | Proven by |
|---|---|
| 1 Router unit + IT, `make verify` | Tasks 2–6, Task 8 Step 2 |
| 2 Gateway `node:addr`, old tests untouched | Task 1 |
| 3 Runs in local and dev | Task 7 (`router-local`, compose) |
| 4 Smoke with PUSH via Kafka and OFFLINE; KICK via Kafka + router | Task 7 (smoke, `FakePushApiKafkaTest`), Task 6 (`controlKickClosesTheOlderSession4012`) |
| 5 Restart without loss; NODE_DOWN | Task 6 RouterIT |
| 6 ADRs, release-check, CI | Task 8; push |
