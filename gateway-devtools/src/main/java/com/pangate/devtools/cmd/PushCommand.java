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

  @Override
  public Integer call() {
    DeliveryClass c = DeliveryClass.valueOf("DELIVERY_CLASS_" + deliveryClass.toUpperCase(java.util.Locale.ROOT));
    byte[] body = payload.getBytes(StandardCharsets.UTF_8);
    try (PushProducer p = PushProducer.connect(kafka, redis, cell)) {
      PushRecord r = target.topic != null ? p.broadcast(target.topic, c, body)
          : p.message(target.principal, device, c, body, receipt);
      System.out.println(r.getMsgId());
      return 0;
    }
  }
}
