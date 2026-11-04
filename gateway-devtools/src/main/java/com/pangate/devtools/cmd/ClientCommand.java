package com.pangate.devtools.cmd;

import com.nimbusds.jose.jwk.ECKey;
import com.pangate.contracts.v1.Connected;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.contracts.v1.Hello;
import com.pangate.devtools.client.ClientTls;
import com.pangate.devtools.client.GatewayClient;
import com.pangate.devtools.client.Hellos;
import com.pangate.devtools.jwt.JwtMinter;
import com.pangate.devtools.jwt.TokenRequest;
import com.pangate.devtools.keys.DevKeys;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "client", description = "Connect to the gateway: one session (--once) or many idle sessions (--load).")
public final class ClientCommand implements Callable<Integer> {

  private static final Duration T = Duration.ofSeconds(10);

  @Option(names = "--url", required = true, description = "wss://host:8443/v1/ws or tcps://host:8883")
  URI url;

  @ArgGroup(exclusive = true, multiplicity = "1")
  Trust trust;

  static final class Trust {
    @Option(names = "--ca", description = "Dev CA certificate to trust (hostname verified).")
    Path ca;

    @Option(names = "--insecure", description = "Trust any certificate.")
    boolean insecure;
  }

  @Option(names = "--token", description = "Connect JWT (single session).")
  String token;

  @Option(names = "--ticket-file", description = "Resume ticket file: used for HELLO if present (and no --token); updated after CONNECTED.")
  Path ticketFile;

  @Option(names = "--key", description = "ES256 key: mint tokens locally (required for --load).")
  Path key;

  @Option(names = "--tenant", defaultValue = "t1")
  String tenant;

  @Option(names = "--principal", description = "Principal for minted single-session tokens (default <tenant>:dev).")
  String principal;

  @Option(names = "--device", defaultValue = "dev-1")
  String device;

  @Option(names = "--once", description = "HELLO, PING, close, exit.")
  boolean once;

  @Option(names = "--load", description = "Open N idle sessions, each with its own principal and device.")
  int load;

  @Option(names = "--ramp", defaultValue = "1000", description = "New sessions per second in --load mode.")
  int ramp;

  @Option(names = "--bind", split = ",", description = "Local addresses to spread --load connections over.")
  List<String> bind = new ArrayList<>();

  @Option(names = "--subscribe", split = ",",
      description = "Topics to SUBSCRIBE after CONNECTED (minted tokens get them as topic patterns).")
  List<String> subscribe = List.of();

  @Option(names = "--ping-interval", defaultValue = "PT30S")
  Duration pingInterval;

  @Override
  public Integer call() throws Exception {
    ClientTls tls = trust.insecure ? ClientTls.insecure() : ClientTls.trust(trust.ca);
    return load > 0 ? runLoad(tls) : runSingle(tls);
  }

  private int runSingle(ClientTls tls) throws Exception {
    try (GatewayClient c = GatewayClient.connect(url, tls, T)) {
      // Print every PUSH / RPC_REQ and ACK the reliable ones (spec §2.3).
      c.onPush(f -> {
        System.out.println(f.getType().name().substring("FRAME_TYPE_".length()) + " msg_id=" + f.getMsgId()
            + " class=" + f.getClass_() + (f.getTopic().isEmpty() ? "" : " topic=" + f.getTopic())
            + " bytes=" + f.getPayload().size());
        if (f.getClass_() == com.pangate.contracts.v1.DeliveryClass.DELIVERY_CLASS_RELIABLE
            || f.getClass_() == com.pangate.contracts.v1.DeliveryClass.DELIVERY_CLASS_ORDERED) {
          c.ack(f.getMsgId());
        }
      });
      Connected connected = c.hello(singleHello(), T);
      System.out.println("CONNECTED conn_id=" + connected.getConnId() + " expires_at_ms=" + connected.getSessionExpiresAtMs());
      if (ticketFile != null) {
        Files.writeString(ticketFile, connected.getResumeTicket());
      }
      if (!subscribe.isEmpty()) {
        c.subscribe(subscribe.toArray(String[]::new));
      }
      c.ping(T);
      System.out.println("PONG");
      if (once) {
        return 0;
      }
      while (c.isOpen()) {
        Thread.sleep(pingInterval.toMillis());
        c.ping(T);
      }
      System.out.println("closed: " + c.awaitClose(T));
      return 0;
    }
  }

  private Hello singleHello() throws Exception {
    if (token != null) {
      return Hellos.jwt(token, device);
    }
    if (ticketFile != null && Files.exists(ticketFile)) {
      return Hellos.ticket(Files.readString(ticketFile).trim(), device);
    }
    if (key != null) {
      String p = principal != null ? principal : tenant + ":dev";
      return Hellos.jwt(minter().mint(new TokenRequest(tenant, p, device, subscribe, Duration.ofMinutes(10)), Instant.now()), device);
    }
    throw new IllegalArgumentException("one of --token, --ticket-file (existing) or --key is required");
  }

  private JwtMinter minter() {
    ECKey k = DevKeys.load(key);
    return new JwtMinter(k, "pangate-devtools");
  }

  private int runLoad(ClientTls tls) throws Exception {
    if (key == null) {
      throw new IllegalArgumentException("--load needs --key to mint one token per session");
    }
    JwtMinter minter = minter();
    AtomicInteger connected = new AtomicInteger();
    AtomicInteger failed = new AtomicInteger();
    ConcurrentLinkedQueue<GatewayClient> clients = new ConcurrentLinkedQueue<>();
    Semaphore inFlight = new Semaphore(512);
    ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
    Thread reporter = Thread.ofPlatform().daemon().start(() -> {
      while (true) {
        System.out.println("connected=" + connected.get() + " failed=" + failed.get());
        try {
          Thread.sleep(10_000);
        } catch (InterruptedException e) {
          return;
        }
      }
    });
    long intervalNanos = TimeUnit.SECONDS.toNanos(1) / Math.max(1, ramp);
    long next = System.nanoTime();
    for (int i = 0; i < load; i++) {
      long wait = next - System.nanoTime();
      if (wait > 0) {
        TimeUnit.NANOSECONDS.sleep(wait);
      }
      next += intervalNanos;
      int n = i;
      inFlight.acquire();
      pool.execute(() -> {
        try {
          InetSocketAddress local = bind.isEmpty() ? null : new InetSocketAddress(bind.get(n % bind.size()), 0);
          GatewayClient c = GatewayClient.connect(url, tls, T, local);
          String dev = "load-" + n;
          c.hello(Hellos.jwt(minter.mint(new TokenRequest(tenant, tenant + ":load-" + n, dev, List.of(),
              Duration.ofMinutes(10)), Instant.now()), dev), T);
          clients.add(c);
          connected.incrementAndGet();
        } catch (RuntimeException e) {
          failed.incrementAndGet();
        } finally {
          inFlight.release();
        }
      });
    }
    inFlight.acquire(512);
    System.out.println("connected=" + connected.get() + " failed=" + failed.get());
    Frame ping = Frame.newBuilder().setType(FrameType.FRAME_TYPE_PING).build();
    while (true) {
      Thread.sleep(pingInterval.toMillis());
      clients.removeIf(c -> !c.isOpen());
      clients.forEach(c -> c.send(ping));
      connected.set(clients.size());
      if (reporter.isInterrupted()) {
        return 0;
      }
    }
  }
}
