package com.pangate.pushapi.config;

import com.pangate.contracts.v1.PushRequest;
import com.pangate.pushapi.core.AcceptMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/** {@link AcceptMetrics} on Micrometer, with the meter names of push-api spec §6. */
final class MicrometerAcceptMetrics implements AcceptMetrics {

  private final MeterRegistry meters;
  private final Timer seqLatency;

  MicrometerAcceptMetrics(MeterRegistry meters) {
    this.meters = meters;
    this.seqLatency = Timer.builder("pushapi.seq.latency").register(meters);
  }

  @Override
  public void produce(String result) {
    meters.counter("pushapi.produce", "result", result).increment();
  }

  @Override
  public void idempotency(String result) {
    meters.counter("pushapi.idempotency", "result", result).increment();
  }

  @Override
  public void rateLimited(PushRequest.Kind kind) {
    meters.counter("pushapi.ratelimited", "kind", kind.name().substring(5).toLowerCase(Locale.ROOT)).increment();
  }

  @Override
  public void seqLatency(long nanos) {
    seqLatency.record(nanos, TimeUnit.NANOSECONDS);
  }
}
