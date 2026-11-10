package com.pangate.pushapi.core;

/** The record did not reach Kafka within the produce timeout (push-api spec §2 step 9). */
public final class ProduceFailedException extends RuntimeException {

  public ProduceFailedException(String what, Throwable cause) {
    super(what, cause);
  }
}
