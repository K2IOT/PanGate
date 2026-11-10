package com.pangate.contracts.testkit;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

class PushHeadersTest {

  private static ConsumerRecord<String, byte[]> record(RecordHeaders h) {
    return new ConsumerRecord<>("c1.push.requests", 0, 0, 0L, org.apache.kafka.common.record.TimestampType.CREATE_TIME,
        0, 0, "t1:u1", new byte[0], h, java.util.Optional.empty());
  }

  @Test
  void headersRoundTripAndViolationsAreReported() {
    RecordHeaders h = new RecordHeaders();
    PushHeaders.write(h, "t1", "c1", null, "PushRecord");
    assertThatCode(() -> PushHeaders.assertConventions(record(h), "PushRecord")).doesNotThrowAnyException();
    assertThatThrownBy(() -> PushHeaders.assertConventions(record(h), "OfflineRecord"))
        .isInstanceOf(AssertionError.class).hasMessageContaining("content-type");

    RecordHeaders missing = new RecordHeaders();
    missing.add("tenant-id", "t1".getBytes());
    missing.add("content-type", "application/x-protobuf; schema=pangate.v1.PushRecord".getBytes());
    assertThatThrownBy(() -> PushHeaders.assertConventions(record(missing), "PushRecord"))
        .isInstanceOf(AssertionError.class).hasMessageContaining("cell-id");
  }

  @Test
  void onlyNodeDownMayHaveAnEmptyTenant() {
    RecordHeaders h = new RecordHeaders();
    PushHeaders.write(h, "", "c1", null, "ConnectionEvent");
    assertThatCode(() -> PushHeaders.assertConventions(record(h), "ConnectionEvent")).doesNotThrowAnyException();
    RecordHeaders p = new RecordHeaders();
    PushHeaders.write(p, "", "c1", null, "PushRecord");
    assertThatThrownBy(() -> PushHeaders.assertConventions(record(p), "PushRecord")).hasMessageContaining("tenant-id");
  }
}
