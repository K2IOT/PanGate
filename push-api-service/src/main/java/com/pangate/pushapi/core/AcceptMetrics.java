package com.pangate.pushapi.core;

import com.pangate.contracts.v1.PushRequest;

/**
 * Accept-path measurements (push-api spec §6) without Micrometer in the core: {@code pushapi.produce{result}},
 * {@code pushapi.idempotency{result}}, {@code pushapi.ratelimited{kind}}, {@code pushapi.seq.latency}.
 */
public interface AcceptMetrics {

  AcceptMetrics NONE = new AcceptMetrics() { };

  /** {@code ok} or {@code failed}. */
  default void produce(String result) { }

  /** {@code new}, {@code duplicate}, {@code pending_wait} or {@code pending_timeout}. */
  default void idempotency(String result) { }

  default void rateLimited(PushRequest.Kind kind) { }

  default void seqLatency(long nanos) { }
}
