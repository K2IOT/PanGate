package com.pangate.router.nodes;

import com.pangate.contracts.v1.ConnectionEvent;
import com.pangate.contracts.v1.EventType;
import com.pangate.router.kafka.Ulids;
import com.pangate.starter.kafka.PangateHeaders;
import java.util.function.LongSupplier;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** NODE_DOWN on {@code <cell>.gw.lifecycle}, key node_id, empty tenant header (push-router spec §3.4). */
public final class KafkaLifecyclePublisher implements LifecyclePublisher {

  private static final Logger log = LoggerFactory.getLogger(KafkaLifecyclePublisher.class);

  private final Producer<String, byte[]> producer;
  private final String cell;
  private final LongSupplier wallMs;

  public KafkaLifecyclePublisher(Producer<String, byte[]> producer, String cell, LongSupplier wallMs) {
    this.producer = producer;
    this.cell = cell;
    this.wallMs = wallMs;
  }

  @Override
  public void nodeDown(String nodeId, long epoch) {
    long now = wallMs.getAsLong();
    ConnectionEvent e = ConnectionEvent.newBuilder().setType(EventType.EVENT_TYPE_NODE_DOWN).setNodeId(nodeId)
        .setEpoch(epoch).setCellId(cell).setAtMs(now).setEventId(Ulids.next(now)).build();
    ProducerRecord<String, byte[]> r = new ProducerRecord<>(cell + ".gw.lifecycle", nodeId, e.toByteArray());
    PangateHeaders.write(r.headers(), null, "", cell, "ConnectionEvent");
    producer.send(r, (md, ex) -> {
      if (ex != null) {
        log.warn("NODE_DOWN {}:{} not published: {}", nodeId, epoch, ex.toString());
      }
    });
    log.info("node {} epoch {} is down", nodeId, epoch);
  }
}
