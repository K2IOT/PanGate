package com.pangate.devtools.cmd;

import com.pangate.contracts.v1.AckItem;
import com.pangate.contracts.v1.ControlOp;
import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.Kick;
import com.pangate.contracts.v1.PushItem;
import com.pangate.contracts.v1.RefreshConfig;
import com.pangate.contracts.v1.Topics;
import com.pangate.devtools.router.FakeRouter;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

@Command(name = "fake-router", description = "Push Router stand-in: send PushBatch items to a gateway's :9090.",
    subcommands = {FakeRouterCommand.Unicast.class, FakeRouterCommand.Broadcast.class,
        FakeRouterCommand.Control.class, FakeRouterCommand.Load.class})
public final class FakeRouterCommand {

  @Option(names = "--target", required = true, description = "Gateway gRPC address, e.g. localhost:9090")
  String target;

  static DeliveryClass deliveryClass(String s) {
    return DeliveryClass.valueOf("DELIVERY_CLASS_" + s.toUpperCase(Locale.ROOT));
  }

  @Command(name = "unicast", description = "One PUSH to one connection; prints the DeliveryAck.")
  static final class Unicast implements Callable<Integer> {
    @ParentCommand FakeRouterCommand parent;

    @Option(names = "--conn-id", required = true)
    String connId;

    @Option(names = "--class", defaultValue = "reliable", description = "reliable | ordered | ephemeral")
    String cls;

    @Option(names = "--payload", defaultValue = "hello")
    String payload;

    @Option(names = "--wait-ack", defaultValue = "PT10S", description = "How long to wait for the ack (RELIABLE).")
    Duration waitAck;

    @Override
    public Integer call() throws Exception {
      DeliveryClass c = deliveryClass(cls);
      try (FakeRouter r = FakeRouter.connect(parent.target)) {
        long id = r.unicast(connId, c, UUID.randomUUID().toString(), payload.getBytes(StandardCharsets.UTF_8));
        if (c == DeliveryClass.DELIVERY_CLASS_EPHEMERAL) {
          System.out.println("SENT delivery_id=" + id + " (ephemeral: no ack)");
          TimeUnit.MILLISECONDS.sleep(200);  // let the batch leave before the stream closes
          return 0;
        }
        try {
          AckItem a = r.awaitAck(id, waitAck);
          System.out.println("ACK delivery_id=" + id + " status=" + a.getStatus());
          return 0;
        } catch (TimeoutException e) {
          System.out.println("NO ACK delivery_id=" + id + " within " + waitAck);
          return 1;
        }
      }
    }
  }

  @Command(name = "broadcast", description = "One broadcast PUSH to every subscriber of a topic (no ack).")
  static final class Broadcast implements Callable<Integer> {
    @ParentCommand FakeRouterCommand parent;

    @Option(names = "--tenant", required = true)
    String tenant;

    @Option(names = "--topic", required = true)
    String topic;

    @Option(names = "--priority", defaultValue = "normal", description = "critical | normal | bulk")
    String priority;

    @Option(names = "--payload", defaultValue = "hello")
    String payload;

    @Override
    public Integer call() throws Exception {
      try (FakeRouter r = FakeRouter.connect(parent.target)) {
        r.broadcast(tenant, topic, PushItem.Priority.valueOf("PRIORITY_" + priority.toUpperCase(Locale.ROOT)),
            payload.getBytes(StandardCharsets.UTF_8));
        TimeUnit.MILLISECONDS.sleep(200);
        System.out.println("SENT broadcast " + topic);
        return 0;
      }
    }
  }

  @Command(name = "control", description = "Control op for every session of a principal (no ack).")
  static final class Control implements Callable<Integer> {
    @ParentCommand FakeRouterCommand parent;

    @Option(names = "--tenant", required = true)
    String tenant;

    @Option(names = "--principal", required = true)
    String principal;

    @ArgGroup(exclusive = true, multiplicity = "1")
    Op op;

    static final class Op {
      @Option(names = "--subscribe", split = ",")
      List<String> subscribe;

      @Option(names = "--unsubscribe", split = ",")
      List<String> unsubscribe;

      @Option(names = "--kick-new-conn")
      String kickNewConn;

      @Option(names = "--refresh-config")
      boolean refresh;
    }

    @Option(names = "--connected-at-ms", defaultValue = "0", description = "With --kick-new-conn.")
    long connectedAtMs;

    @Override
    public Integer call() throws Exception {
      ControlOp.Builder b = ControlOp.newBuilder();
      if (op.subscribe != null) {
        b.setSubscribe(Topics.newBuilder().addAllTopics(op.subscribe));
      } else if (op.unsubscribe != null) {
        b.setUnsubscribe(Topics.newBuilder().addAllTopics(op.unsubscribe));
      } else if (op.kickNewConn != null) {
        b.setKick(Kick.newBuilder().setNewConnId(op.kickNewConn).setConnectedAtMs(connectedAtMs));
      } else {
        b.setRefreshConfig(RefreshConfig.getDefaultInstance());
      }
      try (FakeRouter r = FakeRouter.connect(parent.target)) {
        r.control(tenant, principal, b.build());
        TimeUnit.MILLISECONDS.sleep(200);
        System.out.println("SENT control " + b.getOpCase());
        return 0;
      }
    }
  }

  @Command(name = "load", description = "N unicast pushes at a rate; prints acks by status.")
  static final class Load implements Callable<Integer> {
    @ParentCommand FakeRouterCommand parent;

    @Option(names = "--conn-id", required = true)
    String connId;

    @Option(names = "--class", defaultValue = "reliable")
    String cls;

    @Option(names = "--rate", defaultValue = "100", description = "Pushes per second.")
    int rate;

    @Option(names = "--count", defaultValue = "1000")
    int count;

    @Override
    public Integer call() throws Exception {
      DeliveryClass c = deliveryClass(cls);
      long periodNanos = 1_000_000_000L / Math.max(1, rate);
      try (FakeRouter r = FakeRouter.connect(parent.target)) {
        long next = System.nanoTime();
        for (int i = 0; i < count; i++) {
          r.unicast(connId, c, UUID.randomUUID().toString(), new byte[16]);
          next += periodNanos;
          long wait = next - System.nanoTime();
          if (wait > 0) {
            TimeUnit.NANOSECONDS.sleep(wait);
          }
        }
        TimeUnit.SECONDS.sleep(c == DeliveryClass.DELIVERY_CLASS_EPHEMERAL ? 1 : 7);  // pending-ack timeout is 5 s
        Map<AckItem.Status, Integer> byStatus = new TreeMap<>();
        r.acks().forEach(a -> byStatus.merge(a.getStatus(), 1, Integer::sum));
        System.out.println("SENT " + count + " acks " + byStatus);
        return 0;
      }
    }
  }
}
