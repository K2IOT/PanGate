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

  @Option(names = "--stop-reading",
      description = "After the first PONG stop reading (and pinging) until the gateway closes: a stuck client.")
  boolean stopReading;

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
      if (stopReading) {
        c.pauseReading();
        System.out.println("READING STOPPED");
        while (c.isOpen()) {
          Thread.sleep(200);
        }
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
    Failures failed = new Failures();
    ConcurrentLinkedQueue<GatewayClient> clients = new ConcurrentLinkedQueue<>();
    Semaphore inFlight = new Semaphore(512);
    ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
    Thread reporter = Thread.ofPlatform().daemon().start(() -> {
      while (true) {
        System.out.println("connected=" + connected.get() + " failed=" + failed.total() + " " + failed.summary());
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
          failed.record(e);
        } finally {
          inFlight.release();
        }
      });
    }
    inFlight.acquire(512);
    System.out.println("connected=" + connected.get() + " failed=" + failed.total() + " " + failed.summary());
    Frame ping = Frame.newBuilder().setType(FrameType.FRAME_TYPE_PING).build();
    // Each client pings once per interval, spread evenly over it: real clients connect at different times, and one
    // burst of N pings would stall the gateway's loops every interval (and hold the shed ladder at L1).
    long round = System.nanoTime() + pingInterval.toNanos();
    while (true) {
      TimeUnit.NANOSECONDS.sleep(Math.max(0, round - System.nanoTime()));
      clients.removeIf(c -> !c.isOpen());
      List<GatewayClient> batch = List.copyOf(clients);
      for (int i = 0; i < batch.size(); i++) {
        long wait = round + pingOffsetNanos(i, batch.size(), pingInterval) - System.nanoTime();
        if (wait > 0) {
          TimeUnit.NANOSECONDS.sleep(wait);
        }
        batch.get(i).send(ping);
      }
      round += pingInterval.toNanos();
      connected.set(clients.size());
      if (reporter.isInterrupted()) {
        return 0;
      }
    }
  }

  /** When, from the start of a ping round, client {@code i} of {@code n} sends its PING. */
  static long pingOffsetNanos(int i, int n, Duration interval) {
    return interval.toNanos() / n * i;
  }

  /** Failed load sessions counted by reason (exception type and root message), for the load check's report. */
  static final class Failures {

    private final java.util.concurrent.ConcurrentHashMap<String, java.util.concurrent.atomic.LongAdder> byReason =
        new java.util.concurrent.ConcurrentHashMap<>();
    private final java.util.concurrent.atomic.LongAdder total = new java.util.concurrent.atomic.LongAdder();

    int total() {
      return total.intValue();
    }

    void record(Throwable t) {
      Throwable root = t;
      while (root.getCause() != null && root.getCause() != root) {
        root = root.getCause();
      }
      String msg = String.valueOf(root.getMessage());
      String key = t.getClass().getSimpleName() + ": " + (msg.length() > 80 ? msg.substring(0, 80) : msg);
      byReason.computeIfAbsent(key, k -> new java.util.concurrent.atomic.LongAdder()).increment();
      total.increment();
    }

    /** {@code reasons={type: message=count, ...}}, most frequent first, at most five; empty when none failed. */
    String summary() {
      if (byReason.isEmpty()) {
        return "";
      }
      return byReason.entrySet().stream()
          .sorted((a, b) -> Long.compare(b.getValue().sum(), a.getValue().sum()))
          .limit(5).map(e -> e.getKey() + "=" + e.getValue().sum())
          .collect(java.util.stream.Collectors.joining(", ", "reasons={", "}"));
    }
  }
}
