package com.pangate.gateway.diagnostics;

import java.util.List;

/** Kafka topics the gateway needs are missing and {@code gateway.kafka.create-topics=false} (prod). */
public final class MissingTopicsException extends RuntimeException {

  private final List<String> topics;

  public MissingTopicsException(List<String> topics) {
    super("Kafka topics missing: " + String.join(", ", topics));
    this.topics = List.copyOf(topics);
  }

  public List<String> topics() {
    return topics;
  }
}
