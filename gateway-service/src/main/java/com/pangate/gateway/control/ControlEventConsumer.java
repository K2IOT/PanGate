package com.pangate.gateway.control;

import com.google.protobuf.InvalidProtocolBufferException;
import com.pangate.contracts.v1.ControlEvent;
import com.pangate.gateway.lifecycle.ProcessExit;
import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.server.NodeState;
import io.micrometer.core.instrument.Counter;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads the compacted gw.control topic on the dedicated gw-ctl thread (spec §6.8): assign every partition, seek to
 * the beginning, never commit; the node is "caught up" (a readiness condition) once every partition reached the end
 * offsets snapshotted at boot. Bad records are skipped and counted; poll errors back off 1, 5, 30 s while the state
 * already built stays in force. Not caught up within the limit (5 min) exits the process (spec §10).
 */
public final class ControlEventConsumer {

  private static final Logger log = LoggerFactory.getLogger(ControlEventConsumer.class);
  private static final long[] BACKOFF_MS = {1_000, 5_000, 30_000};

  private final Supplier<Consumer<String, byte[]>> factory;
  private final String topic;
  private final NodeState node;
  private final List<ControlEventHandler> handlers;
  private final ProcessExit exit;
  private final Duration catchUpLimit;
  private final LongSupplier monoMillis;
  private final GatewayMetrics metrics;
  private final Counter badRecords;
  private final Counter pollErrors;
  private final Counter handlerErrors;
  private volatile boolean running;
  private volatile boolean caughtUp;
  private volatile Consumer<String, byte[]> consumer;
  private Thread thread;
  private volatile long startedAt;

  public ControlEventConsumer(Supplier<Consumer<String, byte[]>> factory, String topic, NodeState node,
      List<ControlEventHandler> handlers, GatewayMetrics metrics, ProcessExit exit, Duration catchUpLimit,
      LongSupplier monoMillis) {
    this.factory = factory;
    this.topic = topic;
    this.node = node;
    this.handlers = List.copyOf(handlers);
    this.metrics = metrics;
    this.exit = exit;
    this.catchUpLimit = catchUpLimit;
    this.monoMillis = monoMillis;
    this.badRecords = metrics.counter("gw.control.consumer.errors", "reason", "bad_record");
    this.pollErrors = metrics.counter("gw.control.consumer.errors", "reason", "poll");
    this.handlerErrors = metrics.counter("gw.control.consumer.errors", "reason", "handler");
  }

  public synchronized void start() {
    if (running) {
      return;
    }
    running = true;
    startedAt = monoMillis.getAsLong();
    thread = new Thread(this::run, "gw-ctl");
    thread.setDaemon(true);
    thread.start();
  }

  public boolean caughtUp() {
    return caughtUp;
  }

  private void run() {
    Consumer<String, byte[]> c;
    try {
      c = factory.get();
    } catch (RuntimeException e) {
      // Without a consumer the node would stay unready forever: exit like a missed catch-up (spec §10).
      log.error("cannot create the gw.control consumer", e);
      exit.exit(1, "gw.control consumer cannot be created: " + e.getMessage());
      return;
    }
    consumer = c;
    Map<TopicPartition, Long> target = null;
    int errorsInRow = 0;
    try {
      while (running) {
        try {
          if (target == null) {
            target = assign(c);
          }
          if (target != null) {
            for (ConsumerRecord<String, byte[]> r : c.poll(Duration.ofMillis(500))) {
              dispatch(r);
            }
            if (!caughtUp && reached(c, target)) {
              caughtUp = true;
              node.controlCaughtUp(true);
              log.info("gw.control caught up ({} partitions)", target.size());
            }
          }
          errorsInRow = 0;
        } catch (WakeupException e) {
          if (!running) {
            break;
          }
        } catch (RuntimeException e) {
          pollErrors.increment();
          long backoff = BACKOFF_MS[Math.min(errorsInRow++, BACKOFF_MS.length - 1)];
          log.warn("gw.control consumer error, retrying in {} ms: {}", backoff, e.toString());
          sleep(backoff);
        }
        if (!caughtUp && monoMillis.getAsLong() - startedAt > catchUpLimit.toMillis()) {
          exit.exit(1, "gw.control not caught up within " + catchUpLimit.toMinutes() + "m");
          break;
        }
        if (target == null) {
          sleep(500);
        }
      }
    } finally {
      try {
        c.close(Duration.ofSeconds(2));
      } catch (RuntimeException e) {
        log.debug("closing gw.control consumer", e);
      }
    }
  }

  /** Assigns every partition and snapshots end offsets; null while the topic has no partitions yet. */
  private Map<TopicPartition, Long> assign(Consumer<String, byte[]> c) {
    List<PartitionInfo> infos = c.partitionsFor(topic, Duration.ofSeconds(5));
    if (infos == null || infos.isEmpty()) {
      return null;
    }
    List<TopicPartition> parts = infos.stream().map(i -> new TopicPartition(topic, i.partition())).toList();
    c.assign(parts);
    c.seekToBeginning(parts);
    return Map.copyOf(c.endOffsets(parts, Duration.ofSeconds(5)));
  }

  private static boolean reached(Consumer<String, byte[]> c, Map<TopicPartition, Long> target) {
    for (Map.Entry<TopicPartition, Long> e : target.entrySet()) {
      if (c.position(e.getKey(), Duration.ofSeconds(5)) < e.getValue()) {
        return false;
      }
    }
    return true;
  }

  private void dispatch(ConsumerRecord<String, byte[]> r) {
    try {
      if (r.value() == null) {
        tombstone(r.key());
      } else {
        event(ControlEvent.parseFrom(r.value()));
      }
    } catch (InvalidProtocolBufferException e) {
      badRecords.increment();
    } catch (RuntimeException e) {
      handlerErrors.increment();
      log.warn("gw.control handler failed on {}@{}: {}", r.partition(), r.offset(), e.toString());
    }
  }

  private void tombstone(String key) {
    switch (ControlKeys.parse(key)) {
      case ControlKeys.RevokeKey k -> each(h -> h.onRevokeRemoved(k.revocationKey()));
      case ControlKeys.CellMapKey k -> each(h -> h.onCellMapRemoved(k.tenant()));
      case ControlKeys.TenantCfgKey k -> each(h -> h.onTenantCfgRemoved(k.scope(), k.id()));
      case ControlKeys.TraceKey k -> each(h -> h.onTraceRemoved(k.principal()));
      case ControlKeys.Unknown u -> badRecords.increment();
    }
    count("tombstone");
  }

  private void event(ControlEvent e) {
    switch (e.getEventCase()) {
      case REVOKE -> {
        var key = RevocationList.keyOf(e.getRevoke());
        if (key.isEmpty()) {
          badRecords.increment();
          return;
        }
        each(h -> h.onRevoke(key.get(), e.getRevoke().getExpiresAtMs()));
        count("revoke");
      }
      case CELL_MAP -> {
        if (e.getCellMap().getTenantId().isEmpty() || e.getCellMap().getCell().isEmpty()) {
          badRecords.increment();
          return;
        }
        each(h -> h.onCellMap(e.getCellMap().getTenantId(), e.getCellMap().getCell()));
        count("cellmap");
      }
      case TENANT_CFG -> {
        each(h -> h.onTenantCfg(e.getTenantCfg()));
        count("tenantcfg");
      }
      case TRACE -> {
        if (e.getTrace().getPrincipalId().isEmpty()) {
          badRecords.increment();
          return;
        }
        each(h -> h.onTrace(e.getTrace().getPrincipalId(), e.getIssuedAtMs(), e.getTrace().getExpiresAtMs()));
        count("trace");
      }
      case EVENT_NOT_SET -> badRecords.increment();
    }
  }

  private void each(java.util.function.Consumer<ControlEventHandler> action) {
    handlers.forEach(action);
  }

  private void count(String type) {
    metrics.counter("gw.control.events", "type", type).increment();
  }

  private static void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public void stop() {
    running = false;
    Consumer<String, byte[]> c = consumer;
    if (c != null) {
      try {
        c.wakeup();
      } catch (RuntimeException ignored) {
        // MockConsumer and closed consumers
      }
    }
    Thread t = thread;
    if (t != null) {
      try {
        t.join(5_000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
