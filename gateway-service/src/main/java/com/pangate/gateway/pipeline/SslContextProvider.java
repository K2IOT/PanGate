package com.pangate.gateway.pipeline;

import com.pangate.gateway.config.GatewayProperties;
import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.starter.secrets.SecretNotFoundException;
import io.micrometer.core.instrument.Counter;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolConfig.Protocol;
import io.netty.handler.ssl.ApplicationProtocolConfig.SelectedListenerFailureBehavior;
import io.netty.handler.ssl.ApplicationProtocolConfig.SelectorFailureBehavior;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Server TLS contexts (spec §7.7): TLS 1.3 only, BoringSSL; WS negotiates http/1.1, TCP requires ALPN pangate.v1.
 * {@link #reloadIfChanged()} swaps both contexts when the cert or key file changes (new connections only); a broken
 * replacement keeps the current contexts. Replaced contexts are released after {@code releaseDelay}, by which time
 * handshakes that started on them are over.
 */
public final class SslContextProvider {

  private static final Logger log = LoggerFactory.getLogger(SslContextProvider.class);

  private record Contexts(SslContext ws, SslContext tcp, long certMtime, long certSize, long keyMtime, long keySize) {}

  private final GatewayProperties.Tls tls;
  private final SslProvider provider;
  private final ScheduledExecutorService releaser;
  private final Duration releaseDelay;
  private final AtomicReference<Contexts> current = new AtomicReference<>();
  private final List<Consumer<SslContext>> hooks = new CopyOnWriteArrayList<>();
  private final Counter reloadOk;
  private final Counter reloadFailed;

  public SslContextProvider(GatewayProperties.Tls tls, ScheduledExecutorService releaser, Duration releaseDelay,
      GatewayMetrics m) {
    require(tls.certFile());
    require(tls.keyFile());
    this.tls = tls;
    this.releaser = releaser;
    this.releaseDelay = releaseDelay;
    this.reloadOk = m.counter("gw.tls.reloads", "result", "ok");
    this.reloadFailed = m.counter("gw.tls.reloads", "result", "failed");
    SslProvider p = SslProvider.OPENSSL_REFCNT;
    if (!OpenSsl.isAvailable()) {
      log.warn("BoringSSL (netty-tcnative) unavailable, falling back to the JDK TLS provider: {}",
          String.valueOf(OpenSsl.unavailabilityCause()));
      p = SslProvider.JDK;
    }
    this.provider = p;
    current.set(build());
    log.info("TLS contexts loaded from {} using {}", tls.certFile(), provider);
  }

  private static void require(Path file) {
    if (!Files.isReadable(file)) {
      throw new SecretNotFoundException(file.getFileName().toString(), file.toString(), null);
    }
  }

  private Contexts build() {
    long[] st = stat();
    SslContext ws = build(tls, provider, "http/1.1");
    SslContext tcp;
    try {
      tcp = build(tls, provider, HelloAuthHandler.ALPN_PANGATE);
    } catch (RuntimeException e) {
      ReferenceCountUtil.release(ws);
      throw e;
    }
    return new Contexts(ws, tcp, st[0], st[1], st[2], st[3]);
  }

  private long[] stat() {
    try {
      return new long[] {Files.getLastModifiedTime(tls.certFile()).toMillis(), Files.size(tls.certFile()),
          Files.getLastModifiedTime(tls.keyFile()).toMillis(), Files.size(tls.keyFile())};
    } catch (IOException e) {
      throw new IllegalStateException("Cannot stat TLS files " + tls.certFile() + ", " + tls.keyFile(), e);
    }
  }

  private static SslContext build(GatewayProperties.Tls tls, SslProvider provider, String alpn) {
    try {
      return SslContextBuilder.forServer(tls.certFile().toFile(), tls.keyFile().toFile())
          .sslProvider(provider)
          .protocols("TLSv1.3")
          .applicationProtocolConfig(new ApplicationProtocolConfig(Protocol.ALPN,
              SelectorFailureBehavior.NO_ADVERTISE, SelectedListenerFailureBehavior.ACCEPT, alpn))
          .build();
    } catch (Exception e) {
      throw new IllegalStateException("Cannot build TLS context from " + tls.certFile() + ": " + e.getMessage(), e);
    }
  }

  /** Called with every context built from now on (both of each reload), e.g. to install TLS ticket keys. */
  public void onContextsBuilt(Consumer<SslContext> hook) {
    hooks.add(hook);
  }

  /**
   * Blocking file I/O: runs on the offload executor. Rebuilds both contexts when the cert or key file's (mtime,
   * size) changed; returns whether the contexts were swapped.
   */
  public synchronized boolean reloadIfChanged() {
    Contexts old = current.get();
    long[] st;
    try {
      st = stat();
    } catch (RuntimeException e) {
      reloadFailed.increment();
      log.warn("TLS reload skipped, keeping current certificate: {}", e.getMessage());
      return false;
    }
    if (st[0] == old.certMtime() && st[1] == old.certSize() && st[2] == old.keyMtime() && st[3] == old.keySize()) {
      return false;
    }
    Contexts next;
    try {
      next = build();
    } catch (RuntimeException e) {
      reloadFailed.increment();
      log.warn("TLS reload failed, keeping current certificate (retried in {}): {}", tls.reloadInterval(),
          e.getMessage());
      return false;
    }
    for (Consumer<SslContext> h : hooks) {
      h.accept(next.ws());
      h.accept(next.tcp());
    }
    current.set(next);
    reloadOk.increment();
    log.info("TLS certificate reloaded from {}", tls.certFile());
    releaser.schedule(() -> {
      ReferenceCountUtil.release(old.ws());
      ReferenceCountUtil.release(old.tcp());
    }, releaseDelay.toMillis(), TimeUnit.MILLISECONDS);
    return true;
  }

  public SslContext ws() {
    return current.get().ws();
  }

  public SslContext tcp() {
    return current.get().tcp();
  }
}
