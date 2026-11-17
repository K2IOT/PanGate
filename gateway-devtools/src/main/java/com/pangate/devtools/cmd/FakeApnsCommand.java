package com.pangate.devtools.cmd;

import com.pangate.devtools.notify.FakeApns;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "fake-apns", description = "APNs stand-in for notify-service; GET /received?token=<t> lists what it got.")
public final class FakeApnsCommand implements Callable<Integer> {

  @Option(names = "--port", defaultValue = "9550")
  int port;

  @Override
  public Integer call() throws Exception {
    FakeApns f = FakeApns.start(port);
    CountDownLatch stop = new CountDownLatch(1);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      f.close();
      stop.countDown();
    }));
    System.out.println("fake-apns listening on " + f.port());
    stop.await();
    return 0;
  }
}
