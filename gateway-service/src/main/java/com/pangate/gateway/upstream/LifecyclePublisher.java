package com.pangate.gateway.upstream;

import com.pangate.contracts.v1.ConnectionEvent;
import com.pangate.contracts.v1.EventType;
import com.pangate.gateway.lifecycle.KafkaClients;
import com.pangate.gateway.lifecycle.TopicNames;
import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.protocol.CloseCode;
import com.pangate.gateway.server.NodeState;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.session.SessionListener;
import com.pangate.starter.kafka.PangateHeaders;
import io.micrometer.core.instrument.Counter;
import java.util.function.LongSupplier;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Lifecycle events on {@code <prefix>.gw.lifecycle} (spec §3.2, §4.6 step 5, §5.10), key = principal, through the
 * reliable producer. Fire-and-forget: a failure is counted, never retried and never affects the session.
 */
public final class LifecyclePublisher implements SessionListener {

  private final KafkaClients kafka;
  private final TopicNames topics;
  private final NodeState node;
  private final String cell;
  private final LongSupplier wallMillis;
  private final Counter errors;

  public LifecyclePublisher(KafkaClients kafka, TopicNames topics, NodeState node, String cell,
      GatewayMetrics metrics, LongSupplier wallMillis) {
    this.kafka = kafka;
    this.topics = topics;
    this.node = node;
    this.cell = cell;
    this.wallMillis = wallMillis;
    this.errors = metrics.counter("gw.lifecycle.publish.errors");
  }

  @Override
  public void activated(Session s) {
    publish(s, EventType.EVENT_TYPE_CONNECTED);
  }

  @Override
  public void closed(Session s) {
    EventType type;
    if (s.closeCode() == CloseCode.DUPLICATE_SESSION) {
      type = EventType.EVENT_TYPE_KICKED;
    } else if ("HEARTBEAT_TIMEOUT".equals(s.closeReason())) {
      type = EventType.EVENT_TYPE_HEARTBEAT_TIMEOUT;
    } else {
      type = EventType.EVENT_TYPE_DISCONNECTED;
    }
    publish(s, type);
  }

  public void publish(Session s, EventType type) {
    Session.Identity id = s.identity();
    ConnectionEvent.Builder e = ConnectionEvent.newBuilder()
        .setType(type)
        .setConnId(s.connId())
        .setTenantId(id.tenantId())
        .setPrincipalId(id.principalId())
        .setDeviceId(id.deviceId())
        .setNodeId(node.nodeId())
        .setEpoch(node.epoch())
        .setCellId(cell)
        .setPlatform(id.platform())
        .setSdkVersion(id.sdkVersion())
        .setAtMs(wallMillis.getAsLong())
        .setEventId(EventIds.next(wallMillis.getAsLong(), java.util.concurrent.ThreadLocalRandom.current()));
    if (type != EventType.EVENT_TYPE_CONNECTED && s.closeCode() != null) {
      e.setCloseCode(s.closeCode().code());
    }
    ProducerRecord<String, byte[]> r = new ProducerRecord<>(topics.lifecycle(), id.principalId(),
        e.build().toByteArray());
    PangateHeaders.write(r.headers(), null, id.tenantId(), cell, "ConnectionEvent");
    try {
      kafka.reliable().send(r, (md, ex) -> {
        if (ex != null) {
          errors.increment();
        }
      });
    } catch (RuntimeException ex) {
      errors.increment();
    }
  }
}
