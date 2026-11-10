package com.pangate.devtools.cmd;

import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.PushRecord;
import com.pangate.devtools.push.PushProducer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Callable;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "push", description = "Produce one PushRecord to cN.push.requests (as Push API will), print its msg_id.")
public final class PushCommand implements Callable<Integer> {

  @Option(names = "--kafka", required = true, description = "Kafka bootstrap, e.g. localhost:9092")
  String kafka;

  @Option(names = "--redis", description = "Redis state URI for seq:{p} (RELIABLE / ORDERED to a principal)")
  String redis;

  @Option(names = "--cell", defaultValue = "c1")
  String cell;

  static final class Target {
    @Option(names = "--principal", required = true, description = "<tenant>:<principal>")
    String principal;

    @Option(names = "--topic", required = true, description = "<tenant>:<topic>, BROADCAST")
    String topic;
  }

  @ArgGroup(exclusive = true, multiplicity = "1")
  Target target;

  @Option(names = "--device", description = "with --principal: only this device")
  String device;

  @Option(names = "--class", defaultValue = "RELIABLE", description = "EPHEMERAL, RELIABLE or ORDERED")
  String deliveryClass;

  @Option(names = "--payload", defaultValue = "{\"hello\":\"pangate\"}")
  String payload;

  @Option(names = "--receipt", description = "ask for a DeliveredRecord (RELIABLE / ORDERED MESSAGE)")
  boolean receipt;

  @Option(names = "--count", defaultValue = "1", description = "records to produce (a load loop when > 1)")
  int count;

  @Option(names = "--principals", defaultValue = "0",
      description = "with --principal P: target P<i mod N>, e.g. --principal t1:load- --principals 1000")
  int principals;

  static String principalFor(String base, int i, int principals) {
    return principals > 0 ? base + (i % principals) : base;
  }

  @Override
  public Integer call() {
    DeliveryClass c = DeliveryClass.valueOf("DELIVERY_CLASS_" + deliveryClass.toUpperCase(java.util.Locale.ROOT));
    byte[] body = payload.getBytes(StandardCharsets.UTF_8);
    try (PushProducer p = PushProducer.connect(kafka, redis, cell)) {
      if (count <= 1) {
        PushRecord r = target.topic != null ? p.broadcast(target.topic, c, body)
            : p.message(principalFor(target.principal, 0, principals), device, c, body, receipt);
        System.out.println(r.getMsgId());
        return 0;
      }
      return loop(p, c, body);
    }
  }

  /** {@code count} records, 256 in flight on virtual threads; prints the produce rate. */
  private int loop(PushProducer p, DeliveryClass c, byte[] body) {
    java.util.concurrent.Semaphore inFlight = new java.util.concurrent.Semaphore(256);
    java.util.concurrent.atomic.AtomicInteger failed = new java.util.concurrent.atomic.AtomicInteger();
    long t0 = System.nanoTime();
    try (var pool = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
      for (int i = 0; i < count; i++) {
        int n = i;
        inFlight.acquireUninterruptibly();
        pool.execute(() -> {
          try {
            if (target.topic != null) {
              p.broadcast(target.topic, c, body);
            } else {
              p.message(principalFor(target.principal, n, principals), device, c, body, receipt);
            }
          } catch (RuntimeException e) {
            failed.incrementAndGet();
          } finally {
            inFlight.release();
          }
        });
      }
    }
    double s = (System.nanoTime() - t0) / 1e9;
    System.out.printf("produced %d (failed %d) in %.2f s = %.0f records/s%n", count - failed.get(), failed.get(), s,
        (count - failed.get()) / s);
    return failed.get() == 0 ? 0 : 1;
  }
}
