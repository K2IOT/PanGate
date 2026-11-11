package com.pangate.devtools.cmd;

import com.pangate.devtools.router.FakePushApi;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "fake-pushapi",
    description = "Push API stand-in: receives gateway KICKs and produces them as PushRecord CONTROL to Kafka "
        + "(--kafka, for the push-router) or forwards them as CONTROL items to the gateways (--router).")
public final class FakePushApiCommand implements Callable<Integer> {

  @Option(names = "--port", defaultValue = "9443")
  int port;

  static final class Sink {
    @Option(names = "--kafka", required = true, description = "Kafka bootstrap; KICK → cN.push.requests")
    String kafka;

    @Option(names = "--router", split = ",", required = true,
        description = "Gateway gRPC addresses to forward KICK to directly, e.g. gateway:9090")
    List<String> routers;
  }

  @picocli.CommandLine.ArgGroup(exclusive = true, multiplicity = "1")
  Sink sink;

  @Option(names = "--cell", defaultValue = "c1")
  String cell;

  @Override
  public Integer call() throws Exception {
    FakePushApi api = sink.kafka != null
        ? FakePushApi.start(port, com.pangate.devtools.push.PushProducer.connect(sink.kafka, null, cell))
        : FakePushApi.start(port, sink.routers);
    CountDownLatch stop = new CountDownLatch(1);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      api.close();
      stop.countDown();
    }));
    System.out.println("fake-pushapi listening on " + api.port() + ", forwarding to "
        + (sink.kafka != null ? "kafka " + sink.kafka : sink.routers));
    stop.await();
    return 0;
  }
}
