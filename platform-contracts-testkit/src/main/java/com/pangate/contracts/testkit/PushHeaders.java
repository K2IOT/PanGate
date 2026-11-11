package com.pangate.contracts.testkit;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.contracts.v1.ConnectionEvent;
import com.pangate.contracts.v1.EventType;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;

/** Record headers of the push path (push contracts spec §2, §3.4 of the architecture document). */
public final class PushHeaders {

  private PushHeaders() {}

  public static final String TRACEPARENT = "traceparent";
  public static final String TENANT_ID = "tenant-id";
  public static final String CELL_ID = "cell-id";
  public static final String CONTENT_TYPE = "content-type";

  /** Same names, order and values as the starter's {@code PangateHeaders.write} (pinned by a test). */
  public static void write(Headers h, String tenant, String cell, String traceparentOrNull, String messageName) {
    if (traceparentOrNull != null && !traceparentOrNull.isEmpty()) {
      h.add(TRACEPARENT, traceparentOrNull.getBytes(StandardCharsets.UTF_8));
    }
    h.add(TENANT_ID, tenant.getBytes(StandardCharsets.UTF_8));
    h.add(CELL_ID, cell.getBytes(StandardCharsets.UTF_8));
    h.add(CONTENT_TYPE, contentType(messageName).getBytes(StandardCharsets.UTF_8));
  }

  static Optional<String> read(Headers h, String name) {
    var last = h.lastHeader(name);
    return last == null || last.value() == null ? Optional.empty()
        : Optional.of(new String(last.value(), StandardCharsets.UTF_8));
  }

  public static String contentType(String messageName) {
    return "application/x-protobuf; schema=pangate.v1." + messageName;
  }

  /** AssertJ failure naming the header if tenant-id, cell-id or content-type is missing or wrong. */
  public static void assertConventions(ConsumerRecord<String, byte[]> r, String messageName) {
    String where = r.topic() + "@" + r.partition() + ":" + r.offset() + " ";
    // NODE_DOWN belongs to no tenant: the header is present but may be empty there, and only there
    boolean tenantless = "ConnectionEvent".equals(messageName) && isNodeDown(r.value());
    assertThat(read(r.headers(), TENANT_ID)).as(where + TENANT_ID)
        .hasValueSatisfying(v -> {
          if (!tenantless) {
            assertThat(v).as(where + TENANT_ID).isNotEmpty();
          }
        });
    assertThat(read(r.headers(), CELL_ID)).as(where + CELL_ID)
        .hasValueSatisfying(v -> assertThat(v).as(where + CELL_ID).isNotEmpty());
    assertThat(read(r.headers(), CONTENT_TYPE)).as(where + CONTENT_TYPE)
        .hasValue(contentType(messageName));
  }

  private static boolean isNodeDown(byte[] value) {
    try {
      return value != null && ConnectionEvent.parseFrom(value).getType() == EventType.EVENT_TYPE_NODE_DOWN;
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      return false;
    }
  }
}
