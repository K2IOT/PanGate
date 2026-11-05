package com.pangate.gateway.lifecycle;

import org.apache.kafka.clients.producer.Producer;

/** KafkaClients backed by test producers (e.g. MockProducer) for tests in other packages. */
public final class TestKafka {

  private TestKafka() {}

  public static KafkaClients of(Producer<String, byte[]> reliable, Producer<String, byte[]> telemetry) {
    return new KafkaClients(null, "test", reliable, telemetry);
  }
}
