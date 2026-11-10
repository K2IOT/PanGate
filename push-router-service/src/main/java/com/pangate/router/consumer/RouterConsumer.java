package com.pangate.router.consumer;

import com.google.protobuf.InvalidProtocolBufferException;
import com.pangate.contracts.v1.PushRecord;
import com.pangate.router.config.RouterMetrics;
import com.pangate.router.delivery.DeliveryResult;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.LongSupplier;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code cN.push.requests} → {@link com.pangate.router.delivery.DeliveryJob}, one record per virtual thread, ordered per
 * key (push-router spec §1). A record that does not parse or breaks the push rules is skipped and counted (spec §4);
 * any other exception propagates so Parallel Consumer retries the record.
 */
public final class RouterConsumer {

  private static final Logger log = LoggerFactory.getLogger(RouterConsumer.class);

  private final Function<PushRecord, DeliveryResult> job;
  private final RouterMetrics metrics;
  private final String topic;
  private final LongSupplier wallMs;
  private ParallelEoSStreamProcessor<String, byte[]> processor;

  public RouterConsumer(Function<PushRecord, DeliveryResult> job, RouterMetrics metrics, String topic,
      LongSupplier wallMs) {
    this.job = job;
    this.metrics = metrics;
    this.topic = topic;
    this.wallMs = wallMs;
  }

  /** Subscribes and starts polling with the given Kafka consumer. */
  public synchronized void start(Consumer<String, byte[]> consumer, int maxConcurrency) {
    ParallelConsumerOptions<String, byte[]> options = ParallelConsumerOptions.<String, byte[]>builder()
        .consumer(consumer).ordering(ParallelConsumerOptions.ProcessingOrder.KEY).maxConcurrency(maxConcurrency)
        .commitMode(ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS)
        .defaultMessageRetryDelay(Duration.ofMillis(100))
        .retryDelayProvider(ctx -> Duration.ofMillis(Math.min(5_000, 100L << Math.min(6, ctx.getNumberOfFailedAttempts()))))
        .build();
    processor = new VirtualThreadParallelConsumer<>(options);
    processor.subscribe(List.of(topic));
    processor.poll(ctx -> handle(ctx.getSingleConsumerRecord()));
  }

  /** Stops polling and waits up to {@code timeout} for records in flight; unfinished ones are not committed. */
  public synchronized void stop(Duration timeout) {
    if (processor != null) {
      processor.closeDontDrainFirst(timeout);
      processor = null;
    }
  }

  public boolean running() {
    return processor != null && !processor.isClosedOrFailed();
  }

  void handle(ConsumerRecord<String, byte[]> rec) {
    Optional<PushRecord> parsed = parse(rec);
    if (parsed.isEmpty()) {
      metrics.invalid();
      return;
    }
    PushRecord r = parsed.get();
    metrics.inflight().incrementAndGet();
    try {
      DeliveryResult result = job.apply(r);
      metrics.record(r.getKind(), result);
      if (result == DeliveryResult.DELIVERED && r.getKind() == PushRecord.Kind.KIND_MESSAGE) {
        metrics.latency(r.getClass_(), wallMs.getAsLong() - r.getAcceptedAtMs());
      }
    } finally {
      metrics.inflight().decrementAndGet();
    }
  }

  private static Optional<PushRecord> parse(ConsumerRecord<String, byte[]> rec) {
    String where = rec.topic() + "@" + rec.partition() + ":" + rec.offset();
    if (rec.value() == null) {
      log.warn("skipping {}: empty value", where);
      return Optional.empty();
    }
    PushRecord r;
    try {
      r = PushRecord.parseFrom(rec.value());
    } catch (InvalidProtocolBufferException e) {
      log.warn("skipping {}: not a PushRecord", where);
      return Optional.empty();
    }
    Optional<String> broken = PushRecordCheck.violation(r);
    if (broken.isPresent()) {
      log.warn("skipping {}: breaks push rules ({})", where, broken.get());
      return Optional.empty();
    }
    return Optional.of(r);
  }
}
