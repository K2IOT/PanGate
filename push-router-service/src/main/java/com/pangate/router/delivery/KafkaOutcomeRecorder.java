package com.pangate.router.delivery;

import com.google.protobuf.MessageLite;
import com.pangate.contracts.v1.AckItem;
import com.pangate.contracts.v1.DeliveredRecord;
import com.pangate.contracts.v1.OfflineRecord;
import com.pangate.contracts.v1.PushRecord;
import com.pangate.router.registry.LiveConnection;
import com.pangate.starter.kafka.PangateHeaders;
import java.util.concurrent.ExecutionException;
import java.util.function.LongSupplier;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Writes outcomes to {@code cN.push.offline} / {@code cN.push.delivered} and blocks until the broker acknowledged
 * them, so the source offset is committed only after the outcome is durable. Keys follow push contracts §2
 * (mirrored by the testkit's {@code PushKeys}). A failed send throws: Parallel Consumer then retries the record.
 */
public final class KafkaOutcomeRecorder implements OutcomeRecorder {

  private final Producer<String, byte[]> producer;
  private final String offlineTopic;
  private final String deliveredTopic;
  private final String cell;
  private final LongSupplier wallMs;

  public KafkaOutcomeRecorder(Producer<String, byte[]> producer, String cell, LongSupplier wallMs) {
    this.producer = producer;
    this.cell = cell;
    this.offlineTopic = cell + ".push.offline";
    this.deliveredTopic = cell + ".push.delivered";
    this.wallMs = wallMs;
  }

  @Override
  public void offline(PushRecord r, OfflineRecord.Reason reason, int attempts) {
    OfflineRecord o = OfflineRecord.newBuilder().setRecord(r).setReason(reason).setAttempts(attempts)
        .setAtMs(wallMs.getAsLong()).build();
    send(offlineTopic, targetKey(r), r.getTenantId(), o, "OfflineRecord");
  }

  @Override
  public void delivered(PushRecord r, LiveConnection c, AckItem.Status status) {
    boolean device = r.getTargetCase() == PushRecord.TargetCase.DEVICE;
    DeliveredRecord d = DeliveredRecord.newBuilder().setMsgId(r.getMsgId()).setTenantId(r.getTenantId())
        .setSenderPrincipalId(r.getSenderPrincipalId())
        .setPrincipalId(device ? r.getDevice().getPrincipalId() : r.getPrincipalId())
        .setDeviceId(c.device().orElse("")).setConnId(c.connId()).setStatus(status)
        .setAtMs(wallMs.getAsLong()).build();
    String key = d.getSenderPrincipalId().isEmpty() ? d.getMsgId() : d.getSenderPrincipalId();
    send(deliveredTopic, key, r.getTenantId(), d, "DeliveredRecord");
  }

  static String targetKey(PushRecord r) {
    return switch (r.getTargetCase()) {
      case PRINCIPAL_ID -> r.getPrincipalId();
      case DEVICE -> r.getDevice().getPrincipalId();
      case TOPIC -> "topic:" + r.getTopic();
      case TARGET_NOT_SET -> throw new IllegalArgumentException("PushRecord " + r.getMsgId() + " has no target");
    };
  }

  private void send(String topic, String key, String tenant, MessageLite value, String name) {
    ProducerRecord<String, byte[]> pr = new ProducerRecord<>(topic, key, value.toByteArray());
    PangateHeaders.write(pr.headers(), null, tenant, cell, name);
    try {
      producer.send(pr).get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted writing " + topic, e);
    } catch (ExecutionException e) {
      throw new IllegalStateException("could not write " + topic, e.getCause());
    }
  }
}
