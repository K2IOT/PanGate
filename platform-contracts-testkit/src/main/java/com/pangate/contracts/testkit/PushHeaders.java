package com.pangate.contracts.testkit;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.starter.kafka.PangateHeaders;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;

/** Record headers of the push path (push contracts spec §2, §3.4 of the architecture document). */
public final class PushHeaders {

  private PushHeaders() {}

  public static void write(Headers h, String tenant, String cell, String traceparentOrNull, String messageName) {
    PangateHeaders.write(h, traceparentOrNull, tenant, cell, messageName);
  }

  public static String contentType(String messageName) {
    return "application/x-protobuf; schema=pangate.v1." + messageName;
  }

  /** AssertJ failure naming the header if tenant-id, cell-id or content-type is missing or wrong. */
  public static void assertConventions(ConsumerRecord<String, byte[]> r, String messageName) {
    String where = r.topic() + "@" + r.partition() + ":" + r.offset() + " ";
    // NODE_DOWN belongs to no tenant: the header is present but empty there
    boolean tenantless = "ConnectionEvent".equals(messageName);
    assertThat(PangateHeaders.read(r.headers(), PangateHeaders.TENANT_ID)).as(where + PangateHeaders.TENANT_ID)
        .hasValueSatisfying(v -> {
          if (!tenantless) {
            assertThat(v).as(where + PangateHeaders.TENANT_ID).isNotEmpty();
          }
        });
    assertThat(PangateHeaders.read(r.headers(), PangateHeaders.CELL_ID)).as(where + PangateHeaders.CELL_ID)
        .hasValueSatisfying(v -> assertThat(v).as(where + PangateHeaders.CELL_ID).isNotEmpty());
    assertThat(PangateHeaders.read(r.headers(), PangateHeaders.CONTENT_TYPE)).as(where + PangateHeaders.CONTENT_TYPE)
        .hasValue(contentType(messageName));
  }
}
