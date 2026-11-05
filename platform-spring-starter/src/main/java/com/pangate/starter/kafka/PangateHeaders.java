package com.pangate.starter.kafka;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

/** Standard headers on every PanGate Kafka record (spec §2.2): trace context, tenant, cell and protobuf schema. */
public final class PangateHeaders {

  public static final String TRACEPARENT = "traceparent";
  public static final String TENANT_ID = "tenant-id";
  public static final String CELL_ID = "cell-id";
  public static final String CONTENT_TYPE = "content-type";

  private PangateHeaders() {}

  /** {@code messageName} is the simple protobuf message name, e.g. {@code UpstreamRecord}. */
  public static void write(Headers h, String traceparent, String tenantId, String cellId, String messageName) {
    if (traceparent != null && !traceparent.isEmpty()) {
      add(h, TRACEPARENT, traceparent);
    }
    add(h, TENANT_ID, tenantId);
    add(h, CELL_ID, cellId);
    add(h, CONTENT_TYPE, "application/x-protobuf; schema=pangate.v1." + messageName);
  }

  public static Optional<String> read(Headers h, String name) {
    Header last = h.lastHeader(name);
    return last == null || last.value() == null
        ? Optional.empty() : Optional.of(new String(last.value(), StandardCharsets.UTF_8));
  }

  private static void add(Headers h, String name, String value) {
    h.add(name, value.getBytes(StandardCharsets.UTF_8));
  }
}
