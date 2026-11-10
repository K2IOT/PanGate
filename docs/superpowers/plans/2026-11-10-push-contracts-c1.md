# Push-path contracts C1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the Push API → Kafka → Push Router → gateway contracts. That means:
- new and extended Protobuf;
- Kafka conventions as code;
- registry value v2;
- a shared `platform-contracts-testkit`.

When it is done, sub-projects ③ (router) and ④ (Push API) can start without touching contracts.

**Architecture:**
- Additive `.proto` changes in `platform-contracts`.
- A new Maven module `platform-contracts-testkit`. It is plain Java (no Spring) and provides conventions, fixtures, golden files, validation and registry cases, and a JUnit 5 Kafka scenario.
- Two small gateway changes: registry value v2 and lifecycle `event_id`.
- Devenv topics, ADRs.

**Tech Stack:** Java 21, Protobuf (protobuf-maven-plugin from `platform-parent`), buf 1.47.2 (Docker image `bufbuild/buf:1.47.2`), kafka-clients, Testcontainers Kafka (`apache/kafka:3.9.1`), JUnit 5, AssertJ.

**Spec:** `docs/superpowers/specs/2026-11-10-push-contracts-design.md` (sections cited as "spec §N").

## Global Constraints

- **Additive only:**
  - `buf lint` and `buf breaking --against '.git#branch=main,subdir=platform-contracts'` must pass.
  - No existing field is renamed, renumbered or retyped.
- **Gateway 0.1.0 behaviour unchanged except spec §5:**
  - `PushStream`, `PushItem`, `AckItem`, `ControlKeys` and the KICK path are untouched.
  - Existing tests are not edited.
- **Field numbers exactly as spec §1 and §3.** `ConnectionEvent.event_id = 13`, `EVENT_TYPE_NODE_DOWN = 5`.
- **Topics:**
  - `cN.push.requests` 64 partitions, 24 h;
  - `cN.push.offline` 16 partitions, 7 d;
  - `cN.push.delivered` 16 partitions, 24 h;
  - `cN.gw.lifecycle` 32 partitions, 24 h.
- **Keys** (spec §2):
  - MESSAGE / CONTROL use `principal_id`;
  - device target uses `device.principal_id`;
  - BROADCAST uses `topic:<topic>`;
  - delivered uses `sender_principal_id`, or `msg_id` if empty;
  - NODE_DOWN uses `node_id`.
- **Headers:** via `PangateHeaders.write`, with `content-type: application/x-protobuf; schema=pangate.v1.<Message>`.
- **Registry value v2:** `nodeId|epoch|connectedAtMs|platform|device`, read with `split("\\|", 5)`. v1 has 4 parts; v1 never matches a device target (spec §4).
- **Testkit:**
  - coordinates `com.pangate:platform-contracts-testkit:0.1.0`, package `com.pangate.contracts.testkit`, parent `platform-parent` 0.1.0;
  - no Spring;
  - consumers use it at scope `test`.
- **Build order:** `MODULES` = `platform-parent platform-contracts platform-contracts-testkit platform-spring-starter gateway-devtools gateway-service auth-service`.

## Review Focus

1. A device id containing `|` round-trips through registry value v2: gateway writes it, the reference parser reads it back whole (Task 4, Task 6).
2. A v1 (4-part) registry value parses, but its device is empty and never matches a device target (Task 4).
3. A changed field number or type in any new message is caught by golden bytes even where `buf breaking` would not object, e.g. a reserved number reused after deletion (Task 1, Task 3).
4. The exact 5-field KICK request gateway 0.1.0 sends is a valid `PushRequest` (V9) (Task 4).
5. EPHEMERAL fixtures never carry `seq` / `topic_seq`; BROADCAST fixtures carry `frame.topic == topic` (Task 3).

---

### Task 1: Protobuf changes

**Files:**
- Create: `platform-contracts/proto/pangate/v1/push_record.proto` (spec §1 verbatim, including comments)
- Modify: `platform-contracts/proto/pangate/v1/lifecycle.proto`:
  - add `EVENT_TYPE_NODE_DOWN = 5;`
  - add `string event_id = 13; // ULID, consumers dedup on it`
  - update the header comment: key `node_id` for NODE_DOWN, produced by routers
- Modify: `platform-contracts/proto/pangate/v1/push.proto`:
  - fields 6–16 and the new `Kind` values exactly as spec §3;
  - the header comment now covers every caller of Push API.
- Test: `platform-contracts/src/test/java/com/pangate/contracts/v1/PushContractNumbersTest.java`

**Interfaces:**
- Produces: generated classes `com.pangate.contracts.v1.PushRecord`, `OfflineRecord`, `DeliveredRecord`; new fields on `PushRequest`, `PushResponse`, `ConnectionEvent`; `EventType.EVENT_TYPE_NODE_DOWN`.

- [ ] **Step 1: Write the failing test.** It pins every number of spec §1 and §3 through descriptors.

```java
@Test void pushRecordFieldNumbers() {
  assertNumbers(PushRecord.getDescriptor(), Map.ofEntries(entry("msg_id", 1), entry("kind", 2), entry("tenant_id", 3),
      entry("cell_id", 4), entry("principal_id", 5), entry("device", 6), entry("topic", 7), entry("class", 8),
      entry("priority", 9), entry("accepted_at_ms", 10), entry("expires_at_ms", 11), entry("collapse_key", 12),
      entry("receipt", 13), entry("sender_principal_id", 14), entry("frame", 15), entry("control", 16)));
  assertThat(PushRecord.getDescriptor().getOneofs()).extracting(o -> o.getName()).containsExactly("target");
}
@Test void offlineAndDeliveredFieldNumbers() { /* OfflineRecord record=1 reason=2 attempts=3 at_ms=4; Reason values 0..8 as spec; DeliveredRecord 1..8 */ }
@Test void pushRequestAndResponseExtensions() { /* PushRequest 1..16, Kind CONTROL=1 MESSAGE=2 BROADCAST=3; PushResponse 1..11, Code 0..6 */ }
@Test void lifecycleAdditions() { assertThat(ConnectionEvent.getDescriptor().findFieldByName("event_id").getNumber()).isEqualTo(13);
  assertThat(EventType.EVENT_TYPE_NODE_DOWN.getNumber()).isEqualTo(5); }
```

- [ ] **Step 2: Run** `./mvnw -q -f platform-contracts test -Dtest=PushContractNumbersTest`. Expected: compilation FAIL (`PushRecord` missing).
- [ ] **Step 3: Implement** the three proto changes.
- [ ] **Step 4: Verify.**
  - The test passes.
  - `docker run --rm -v $PWD:/w -w /w bufbuild/buf:1.47.2 lint platform-contracts` exits 0.
  - `... breaking platform-contracts --against '.git#branch=main,subdir=platform-contracts'` exits 0.
  - `./mvnw -q -f platform-contracts install` succeeds.
- [ ] **Step 5: Commit** `feat(contracts): PushRecord/OfflineRecord/DeliveredRecord, PushService for every target, lifecycle event_id and NODE_DOWN`

---

### Task 2: Testkit module and Kafka conventions

**Files:**
- Create: `platform-contracts-testkit/pom.xml`:
  - parent `platform-parent` 0.1.0, `relativePath ../platform-parent/pom.xml`;
  - compile-scope deps: `platform-contracts`, `platform-spring-starter` (only for `PangateHeaders`, with an `*:*` exclusion so no Spring comes along, as auth-service does for the gateway verifier jar), `kafka-clients`, `org.testcontainers:kafka`, `org.junit.jupiter:junit-jupiter-api`, `org.assertj:assertj-core`;
  - test deps: `junit-jupiter`, `awaitility`;
  - build-helper `src/it/java`, surefire excludes `*IT`, failsafe includes `*IT`, as `auth-service/pom.xml` does.
- Create: `platform-contracts-testkit/src/main/java/com/pangate/contracts/testkit/PushTopics.java`, `PushKeys.java`, `PushHeaders.java`
- Modify: `Makefile` (`MODULES` per Global Constraints); `platform-devenv/scripts/release-check.sh` (version loop adds `platform-contracts-testkit`)
- Test: `platform-contracts-testkit/src/test/java/com/pangate/contracts/testkit/PushTopicsTest.java`, `PushKeysTest.java`, `PushHeadersTest.java`

**Interfaces:**
- Consumes: Task 1 classes.
- Produces:
  - `record PushTopics.Spec(String name, int partitions, Duration retention)`
  - `PushTopics.requests(String cell) -> Spec` (`"<cell>.push.requests"`, 64, 24 h); likewise `offline(cell)` (16, 7 d), `delivered(cell)` (16, 24 h), `lifecycle(cell)` (`"<cell>.gw.lifecycle"`, 32, 24 h)
  - `PushTopics.all(String cell) -> List<Spec>`
  - `PushKeys.of(PushRecord)`, `of(OfflineRecord)`, `of(DeliveredRecord)`, `of(ConnectionEvent) -> String`; throws `IllegalArgumentException` for a record with no target
  - `PushHeaders.write(Headers h, String tenant, String cell, String traceparentOrNull, String messageName)`
  - `PushHeaders.assertConventions(ConsumerRecord<String, byte[]> r, String messageName)`: AssertJ failures if `tenant-id`, `cell-id` or `content-type` is missing or wrong

- [ ] **Step 1: Write the failing tests.**

```java
@Test void topicSpecsMatchSpecSection2() {
  assertThat(PushTopics.requests("c1")).isEqualTo(new PushTopics.Spec("c1.push.requests", 64, Duration.ofHours(24)));
  assertThat(PushTopics.offline("c1")).isEqualTo(new PushTopics.Spec("c1.push.offline", 16, Duration.ofDays(7)));
  assertThat(PushTopics.delivered("c1")).isEqualTo(new PushTopics.Spec("c1.push.delivered", 16, Duration.ofHours(24)));
  assertThat(PushTopics.lifecycle("c1")).isEqualTo(new PushTopics.Spec("c1.gw.lifecycle", 32, Duration.ofHours(24)));
}
@Test void keys() {
  // principal MESSAGE -> "t1:u1"; device MESSAGE -> "t1:u1"; BROADCAST topic "t1:news" -> "topic:t1:news"; CONTROL -> principal
  // OfflineRecord -> key of its record; Delivered sender "t1:s" -> "t1:s", empty sender -> msg_id
  // NODE_DOWN ConnectionEvent node "gw-1" -> "gw-1"; CONNECTED -> principal; PushRecord without target -> IllegalArgumentException
}
@Test void headersRoundTripAndViolationsAreReported() { /* write then assertConventions passes; missing cell-id -> AssertionError naming "cell-id" */ }
```

- [ ] **Step 2: Run** `./mvnw -q -f platform-contracts-testkit test`. Expected: FAIL (no module / classes).
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Verify.** Tests pass; `make release-check` prints `release-check OK 0.1.0`.
- [ ] **Step 5: Commit** `feat(testkit): platform-contracts-testkit with push topic, key and header conventions`

---

### Task 3: Fixtures and golden files

**Files:**
- Create: `platform-contracts-testkit/src/main/java/com/pangate/contracts/testkit/Fixtures.java`, `Golden.java`
- Create: `platform-contracts-testkit/src/main/resources/golden/v1/*.textproto` and `*.bin`, one pair per golden name below
- Test: `platform-contracts-testkit/src/test/java/com/pangate/contracts/testkit/FixturesTest.java`, `GoldenTest.java`

**Interfaces:**
- Produces:
  - `Fixtures.FIXED_MSG_ID = "01JC0000000000000000000000"`, `FIXED_NOW_MS = 1_762_732_800_000L`; every fixture is deterministic.
  - `Fixtures.message(String principal) -> PushRecord`: RELIABLE, seq 1, seq_epoch 1, tenant = prefix before `:`, cell `c1`, ttl 24 h.
  - `deviceMessage(String principal, String device)`.
  - `ephemeral(String principal)`: no seq.
  - `broadcast(String topic)`: RELIABLE, `frame.topic = topic`, `topic_seq` 1.
  - `control(String principal, ControlOp op)`.
  - `offline(PushRecord, OfflineRecord.Reason)`: attempts 3.
  - `delivered(PushRecord, String connId)`: `STATUS_ACKED`, device from target or `""`.
  - `nodeDown(String nodeId, long epoch)`: `event_id` fixed.
  - `kickRequest010()`: the exact 5-field KICK `PushRequest` that gateway 0.1.0 sends.
  - `request(PushRequest.Kind)`: a valid sample per kind.
  - `Golden.NAMES` = `push_record_message`, `push_record_device`, `push_record_ephemeral`, `push_record_broadcast`, `push_record_control_subscribe`, `push_record_control_kick`, `offline_record`, `delivered_record`, `connection_event_node_down`, `push_request_message`, `push_request_broadcast`, `push_request_control_kick_010`, `push_response_ok`.
  - `Golden.fixture(String name) -> Message`, `Golden.bytes(String name) -> byte[]`, `Golden.text(String name) -> String`.
  - `Golden.main(String[] args)`: rewrites every pair from the fixtures (run by hand only when a deliberate additive change adds a golden; never to make a red test green).

- [ ] **Step 1: Write the failing tests.**

```java
@Test void fixturesObeySpecSection11() {
  PushRecord m = Fixtures.message("t1:u1");
  assertThat(m.getFrame().getType()).isEqualTo(FrameType.FRAME_TYPE_PUSH);
  assertThat(m.getFrame().getMsgId()).isEqualTo(m.getMsgId());
  assertThat(m.getFrame().getClass_()).isEqualTo(m.getClass_());
  assertThat(m.getFrame().getSeq()).isEqualTo(1);
  assertThat(Fixtures.ephemeral("t1:u1").getFrame().getSeq()).isZero();          // Review Focus 5
  assertThat(Fixtures.ephemeral("t1:u1").getFrame().getTopicSeq()).isZero();
  PushRecord b = Fixtures.broadcast("t1:news");
  assertThat(b.getFrame().getTopic()).isEqualTo("t1:news");                     // Review Focus 5
  assertThat(b.getFrame().getTopicSeq()).isEqualTo(1);
  assertThat(Fixtures.control("t1:u1", kickOp()).hasFrame()).isFalse();
  assertThat(m.getExpiresAtMs() - m.getAcceptedAtMs()).isEqualTo(86_400_000L);
}
@Test void everyGoldenEncodesToItsStoredBytes() {
  for (String n : Golden.NAMES) assertThat(Golden.fixture(n).toByteArray()).as(n).isEqualTo(Golden.bytes(n));
}
@Test void everyGoldenTextParsesToItsFixture() { /* TextFormat.merge(Golden.text(n), builder) equals Golden.fixture(n) */ }
@Test void storedBytesStillParse() { /* parseFrom(Golden.bytes(n)) equals fixture: catches renumbering (Review Focus 3) */ }
```

- [ ] **Step 2: Run** `./mvnw -q -f platform-contracts-testkit test -Dtest='FixturesTest,GoldenTest'`. Expected: FAIL.
- [ ] **Step 3: Implement** `Fixtures` and `Golden`. Generate the golden files once by running `Golden.main` with the module's test classpath (`exec:java` or a test-scoped runner), then check them into git.
- [ ] **Step 4: Verify.**
  - Tests pass.
  - Edit one `.bin` byte by hand: `GoldenTest` goes red. Revert the edit.
- [ ] **Step 5: Commit** `feat(testkit): deterministic push fixtures and golden files`

---

### Task 4: Validation cases and registry values

**Files:**
- Create: `platform-contracts-testkit/src/main/java/com/pangate/contracts/testkit/PushRequestCases.java`, `RegistryValues.java`
- Test: `platform-contracts-testkit/src/test/java/com/pangate/contracts/testkit/PushRequestCasesTest.java`, `RegistryValuesTest.java`

**Interfaces:**
- Produces:
  - `record PushRequestCases.Case(String name, String rule, PushRequest request, PushResponse.Code expected)`; `PushRequestCases.all() -> List<Case>`. `rule` is `"V1"`…`"V9"` (spec §3.1).
  - `record RegistryValues.Parsed(String nodeId, long epoch, long connectedAtMs, String platform, Optional<String> device)`
  - `RegistryValues.parse(String value) -> Optional<Parsed>`: the reference parser, with `split("\\|", 5)` and the v1/v2 rules of spec §4. Empty means malformed.
  - `RegistryValues.v2(String nodeId, long epoch, long connectedAtMs, String platform, String device) -> String`
  - `RegistryValues.matchesDevice(Parsed p, String device) -> boolean`: false for v1 and for an empty device.
  - `record RegistryValues.Case(String name, String value, Optional<Parsed> expected)`; `RegistryValues.cases() -> List<Case>`.

- [ ] **Step 1: Write the failing tests.**

```java
@Test void everyRuleHasAPassingAndAFailingCase() {
  var byRule = PushRequestCases.all().stream().collect(groupingBy(PushRequestCases.Case::rule));
  for (String r : List.of("V1","V2","V3","V4","V5","V6","V7","V8")) {
    assertThat(byRule.get(r)).as(r).anyMatch(c -> c.expected() == Code.CODE_OK).anyMatch(c -> c.expected() == Code.CODE_INVALID);
  }
  assertThat(byRule.get("V9")).singleElement().satisfies(c -> {                 // Review Focus 4
    assertThat(c.request()).isEqualTo(Fixtures.kickRequest010());
    assertThat(c.expected()).isEqualTo(Code.CODE_OK);
  });
}
@Test void boundaryCasesUseUtf8Bytes() { /* V5: principal of exactly 256 B OK, 257 B (multi-byte char at the end) INVALID; V6 device 128 B OK, 129 B INVALID, "a/b" INVALID; V7 payload 65_536 OK, 65_537 INVALID, ttl 604_800 OK, 604_801 INVALID */ }
@Test void registryV2RoundTripsADeviceWithPipes() {                            // Review Focus 1
  String v = RegistryValues.v2("gw-1", 7, 1000, "ios", "dev|with|pipes");
  assertThat(v).isEqualTo("gw-1|7|1000|ios|dev|with|pipes");
  assertThat(RegistryValues.parse(v)).hasValue(new RegistryValues.Parsed("gw-1", 7, 1000, "ios", Optional.of("dev|with|pipes")));
}
@Test void v1ParsesButNeverMatchesADevice() {                                  // Review Focus 2
  var p = RegistryValues.parse("gw-1|7|1000|ios").orElseThrow();
  assertThat(p.device()).isEmpty();
  assertThat(RegistryValues.matchesDevice(p, "d1")).isFalse();
  assertThat(RegistryValues.matchesDevice(RegistryValues.parse("gw-1|7|1000||").orElseThrow(), "")).isFalse();
}
@Test void malformedValuesAreEmpty() { /* "gw-1|7", "gw-1|x|1000|ios", "", "gw-1|7|-1|ios" -> Optional.empty() */ }
@Test void casesAgreeWithTheParser() { /* for each RegistryValues.cases(): parse(value) equals expected */ }
```

- [ ] **Step 2: Run** `./mvnw -q -f platform-contracts-testkit test -Dtest='PushRequestCasesTest,RegistryValuesTest'`. Expected: FAIL.
- [ ] **Step 3: Implement.** The cases are data; the testkit has no validator. ④ owns validation, and its tests run `all()` against its service.
- [ ] **Step 4: Verify.** Tests pass.
- [ ] **Step 5: Commit** `feat(testkit): PushRequest validation cases and registry value v1/v2 reference cases`

---

### Task 5: Push path scenario on Kafka

**Files:**
- Create: `platform-contracts-testkit/src/main/java/com/pangate/contracts/testkit/PushPathScenario.java`
- Test: `platform-contracts-testkit/src/it/java/com/pangate/contracts/testkit/PushPathScenarioIT.java`, `platform-contracts-testkit/src/it/java/com/pangate/contracts/testkit/FakeRouter.java` (test scope only)

**Interfaces:**
- Consumes: `PushTopics`, `PushKeys`, `PushHeaders`, `Fixtures` (Tasks 2–3).
- Produces:
  - `final class PushPathScenario implements BeforeAllCallback, AfterAllCallback`, used as `@RegisterExtension static PushPathScenario scenario = new PushPathScenario("c1")`. It starts one shared `KafkaContainer("apache/kafka:3.9.1")` per JVM and creates `PushTopics.all(cell)` with their partitions.
  - `String bootstrapServers()`, `String cell()`
  - `void produce(PushRecord r)`: key `PushKeys.of(r)`, headers via `PushHeaders.write(..., "PushRecord")`, waits for the ack.
  - `PushRecord awaitRequest(String msgId)`, `OfflineRecord awaitOffline(String msgId, OfflineRecord.Reason reason)`, `DeliveredRecord awaitDelivered(String msgId)`, `ConnectionEvent awaitNodeDown(String nodeId, long epoch)`. Each takes a default timeout of 30 s and has an overload with a `Duration`. Each reads from the earliest offset and asserts `PushHeaders.assertConventions` and `key == PushKeys.of(value)` on the matching record.
  - `List<ConsumerRecord<String, byte[]>> drain(String topic)`.

- [ ] **Step 1: Write the failing IT.** `FakeRouter` consumes `requests`. For a MESSAGE it writes `Fixtures.offline(record, REASON_NO_SESSION)` to `offline`, with key and headers per convention. When `receipt` is set it also writes a delivered record. For a lifecycle trigger it writes `nodeDown`.

```java
@Test void messageGoesOfflineWithConventions() {
  PushRecord m = Fixtures.message("t1:u" + suffix).toBuilder().setMsgId(newMsgId()).build();
  scenario.produce(m);
  OfflineRecord o = scenario.awaitOffline(m.getMsgId(), OfflineRecord.Reason.REASON_NO_SESSION);
  assertThat(o.getRecord()).isEqualTo(m);
}
@Test void recordsOfOnePrincipalKeepTheirOrder() { /* produce 5 MESSAGE for one principal; drain(requests) filtered by principal yields them in produce order, all in one partition */ }
@Test void broadcastIsKeyedByTopic() { /* produce broadcast("t1:news"); awaitRequest -> its ConsumerRecord key "topic:t1:news" */ }
@Test void conventionViolationFailsTheAwait() { /* a raw record produced without cell-id header: awaitRequest(msgId) throws AssertionError naming "cell-id" */ }
```

- [ ] **Step 2: Run** `./mvnw -f platform-contracts-testkit verify -Dit.test=PushPathScenarioIT`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Verify.** The IT passes, and so does `./mvnw -f platform-contracts-testkit verify` (whole module).
- [ ] **Step 5: Commit** `feat(testkit): PushPathScenario on real Kafka`

---

### Task 6: Gateway — registry value v2 and lifecycle event_id

**Files:**
- Modify: `gateway-service/src/main/java/com/pangate/gateway/registry/RegistryWriter.java`:
  - `static String value(String nodeId, long epoch, long connectedAtMs, String platform, String device)` replaces the 4-argument version;
  - the call site passes `s.identity().deviceId()`;
  - the class javadoc gives the v2 format.
- Create: `gateway-service/src/main/java/com/pangate/gateway/upstream/EventIds.java`:
  - `static String next()`: ULID, 26 Crockford base32 chars, 48-bit ms time + 80 random bits;
  - `static String next(long nowMs, java.util.random.RandomGenerator rnd)`.
- Modify: `gateway-service/src/main/java/com/pangate/gateway/upstream/LifecyclePublisher.java` (`setEventId(EventIds.next(...))` with the publisher's `wallMillis`)
- Modify: `gateway-service/pom.xml` (test dep `platform-contracts-testkit`)
- Test: `gateway-service/src/test/java/com/pangate/gateway/registry/RegistryValueV2Test.java`, `gateway-service/src/test/java/com/pangate/gateway/upstream/EventIdsTest.java`, and a new test method in a new class `LifecycleEventIdTest.java`. Existing test files are not edited.

**Interfaces:**
- Consumes: `RegistryValues.parse`, `RegistryValues.v2`, `RegistryValues.cases` (Task 4).

- [ ] **Step 1: Write the failing tests.**

```java
// RegistryValueV2Test
@Test void writerMatchesTheContract() {
  assertThat(RegistryWriter.value("gw-1", 7, 1000, "ios", "dev|with|pipes"))
      .isEqualTo(RegistryValues.v2("gw-1", 7, 1000, "ios", "dev|with|pipes"));   // Review Focus 1
  assertThat(RegistryWriter.value("gw-1", 7, 1000, null, "d1")).isEqualTo("gw-1|7|1000||d1");
}
@Test void gatewayReadersAcceptV1AndV2() {
  for (var c : RegistryValues.cases()) c.expected().ifPresent(p ->
      assertThat(RegistryWriter.connectedAtMs(c.value())).as(c.name()).isEqualTo(p.connectedAtMs()));
}
// EventIdsTest
@Test void ulidShapeTimeOrderAndUniqueness() { /* 26 chars of [0-9A-HJKMNP-TV-Z]; next(1000, r) < next(2000, r) lexicographically; 10_000 next() distinct */ }
// LifecycleEventIdTest (same fixture style as LifecyclePublisherTest, via MockProducer)
@Test void everyEventCarriesADistinctEventId() { /* CONNECTED and DISCONNECTED events: event_id matches ULID pattern, the two differ */ }
```

- [ ] **Step 2: Run** `./mvnw -q -f gateway-service test -Dtest='RegistryValueV2Test,EventIdsTest,LifecycleEventIdTest'`. Expected: FAIL.
- [ ] **Step 3: Implement.** `SingleSessionKicker` and `RegistryWriter.connectedAtMs` read only leading parts and stay unchanged. Their v1/v2 tolerance is pinned by `gatewayReadersAcceptV1AndV2`.
- [ ] **Step 4: Verify.**
  - The three tests pass.
  - `./mvnw -f gateway-service verify` is green with no existing test edited.
  - Before committing, run GitNexus `impact` on `RegistryWriter.value` and `LifecyclePublisher.publish` and record the risk.
- [ ] **Step 5: Commit** `feat(gateway): registry value v2 with device; lifecycle events carry event_id`

---

### Task 7: Devenv topics, ADRs and docs

**Files:**
- Modify: `platform-devenv/docker-compose.yml`: `kafka-init` creates `c1.push.requests` with `--partitions 64`, `c1.push.offline` with 16 and `c1.push.delivered` with 16, each with its retention `--config retention.ms=` (86400000, 604800000, 86400000).
- Create: `docs/adr/0016-single-push-record-envelope.md`, `0017-push-api-builds-the-frame.md`, `0018-registry-value-v2.md`, with the headings `release-check.sh` requires.
- Modify: `docs/adr/README.md` (index), `CHANGELOG.md` (`## Unreleased` → contracts entry), `README.md` (module list), `docs/superpowers/specs/2026-11-10-push-contracts-design.md` (status line: implemented, with measured numbers).
- Test: `platform-contracts-testkit/src/test/java/com/pangate/contracts/testkit/DevenvTopicsTest.java`. It reads `../platform-devenv/docker-compose.yml` and asserts that for each `PushTopics.all("c1")` spec the `kafka-init` command creates that topic with that partition count.

- [ ] **Step 1: Write the failing test** `DevenvTopicsTest.composeCreatesEveryPushTopic`.
- [ ] **Step 2: Run** `./mvnw -q -f platform-contracts-testkit test -Dtest=DevenvTopicsTest`. Expected: FAIL (push topics missing).
- [ ] **Step 3: Implement** the compose change, ADRs and docs.
- [ ] **Step 4: Verify.**
  - The test passes.
  - `make infra-up`, then `docker compose -f platform-devenv/docker-compose.yml exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic c1.push.requests` shows `PartitionCount: 64`. Then `make infra-down`.
  - `make release-check` prints `release-check OK 0.1.0`.
  - `make verify` is green.
  - `make images up smoke down` is green.
- [ ] **Step 5: Commit** `docs: ADR 0016-0018, changelog; devenv creates the push topics`

---

## Exit (spec §10) → tasks

| Exit criterion | Proven by |
|---|---|
| 1 `buf lint` / `buf breaking`; protos as §1–§3 | Task 1 |
| 2 Testkit builds, unit + IT green | Tasks 2–5 |
| 3 Gateway v2 + `event_id`, old tests, smoke, local-check green | Task 6, Task 7 Step 4 |
| 4 `make up` creates `c1.push.*` | Task 7 |
| 5 Mapping table §3.2 | Already in the spec; Task 4 cases mirror V1–V9 |
| 6 ADR 0016–0018, release-check, CI | Task 7; push after Task 7 |
