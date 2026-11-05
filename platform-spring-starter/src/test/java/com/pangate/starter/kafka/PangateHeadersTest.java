package com.pangate.starter.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

class PangateHeadersTest {

  @Test
  void writesSchemaContentTypeAndSkipsEmptyTraceparent() {
    RecordHeaders h = new RecordHeaders();

    PangateHeaders.write(h, "", "t1", "cell-1", "UpstreamRecord");

    assertThat(PangateHeaders.read(h, PangateHeaders.CONTENT_TYPE))
        .hasValue("application/x-protobuf; schema=pangate.v1.UpstreamRecord");
    assertThat(PangateHeaders.read(h, PangateHeaders.TRACEPARENT)).isEmpty();
    assertThat(PangateHeaders.read(h, PangateHeaders.TENANT_ID)).hasValue("t1");
    assertThat(PangateHeaders.read(h, PangateHeaders.CELL_ID)).hasValue("cell-1");
  }

  @Test
  void writesTraceparentWhenPresent() {
    RecordHeaders h = new RecordHeaders();

    PangateHeaders.write(h, "00-abc-def-01", "t1", "cell-1", "ConnectionEvent");

    assertThat(PangateHeaders.read(h, "traceparent")).hasValue("00-abc-def-01");
  }

  @Test
  void readReturnsLastValue() {
    RecordHeaders h = new RecordHeaders();
    h.add("tenant-id", "a".getBytes(StandardCharsets.UTF_8));
    h.add("tenant-id", "b".getBytes(StandardCharsets.UTF_8));

    assertThat(PangateHeaders.read(h, "tenant-id")).hasValue("b");
    assertThat(PangateHeaders.read(h, "missing")).isEmpty();
  }
}
