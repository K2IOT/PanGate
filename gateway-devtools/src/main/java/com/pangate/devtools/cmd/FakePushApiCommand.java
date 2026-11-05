package com.pangate.devtools.cmd;

import com.pangate.devtools.router.FakePushApi;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "fake-pushapi",
    description = "Push API stand-in: receives gateway KICKs and forwards them as CONTROL items to the gateways.")
public final class FakePushApiCommand implements Callable<Integer> {

  @Option(names = "--port", defaultValue = "9443")
  int port;

  @Option(names = "--router", split = ",", required = true,
      description = "Gateway gRPC addresses to forward KICK to, e.g. gateway:9090")
  List<String> routers;

  @Override
  public Integer call() throws Exception {
    FakePushApi api = FakePushApi.start(port, routers);
    CountDownLatch stop = new CountDownLatch(1);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      api.close();
      stop.countDown();
    }));
    System.out.println("fake-pushapi listening on " + api.port() + ", forwarding to " + routers);
    stop.await();
    return 0;
  }
}
