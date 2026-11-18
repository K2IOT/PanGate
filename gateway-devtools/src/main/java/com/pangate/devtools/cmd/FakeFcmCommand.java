package com.pangate.devtools.cmd;

import com.pangate.devtools.notify.FakeFcm;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "fake-fcm", description = "FCM HTTP v1 stand-in for notify-service; GET /received?token=<t> lists what it got.")
public final class FakeFcmCommand implements Callable<Integer> {

  @Option(names = "--port", defaultValue = "9551")
  int port;

  @Override
  public Integer call() throws Exception {
    FakeFcm f = FakeFcm.start(port);
    CountDownLatch stop = new CountDownLatch(1);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      f.close();
      stop.countDown();
    }));
    System.out.println("fake-fcm listening on " + f.port());
    stop.await();
    return 0;
  }
}
