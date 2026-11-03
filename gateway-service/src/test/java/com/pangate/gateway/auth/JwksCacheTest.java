package com.pangate.gateway.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.gen.ECKeyGenerator;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.pangate.gateway.config.GatewayProperties;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class JwksCacheTest {

  /** Scripted JWKS endpoint: each response is (status, body, cacheControl). */
  private HttpServer http;
  private final ArrayDeque<String[]> responses = new ArrayDeque<>();
  private final AtomicInteger hits = new AtomicInteger();
  private final AtomicLong clock = new AtomicLong(1_000_000L);
  private final ScheduledExecutorService tick = Executors.newSingleThreadScheduledExecutor();
  private final ArrayDeque<Runnable> offloadQueue = new ArrayDeque<>();

  @BeforeEach
  void start() throws Exception {
    http = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    http.createContext("/jwks", ex -> {
      hits.incrementAndGet();
      String[] r = responses.isEmpty() ? new String[] {"500", "", null} : responses.poll();
      if ("304".equals(r[0])) {
        ex.sendResponseHeaders(304, -1);
        ex.close();
        return;
      }
      byte[] body = r[1].getBytes(StandardCharsets.UTF_8);
      ex.getResponseHeaders().set("ETag", "\"v" + hits.get() + "\"");
      if (r[2] != null) {
        ex.getResponseHeaders().set("Cache-Control", r[2]);
      }
      ex.sendResponseHeaders(Integer.parseInt(r[0]), body.length == 0 ? -1 : body.length);
      if (body.length > 0) {
        ex.getResponseBody().write(body);
      }
      ex.close();
    });
    http.start();
  }

  @AfterEach
  void stop() {
    http.stop(0);
    tick.shutdownNow();
  }

  private JwksSource httpSource() {
    return new HttpJwksSource(URI.create("http://127.0.0.1:" + http.getAddress().getPort() + "/jwks"),
        HttpClient.newHttpClient(), Duration.ofSeconds(2));
  }

  private JwksCache cache(JwksSource src) {
    return new JwksCache(src, tick, Runnable::run, Duration.ofMinutes(5), clock::get);
  }

  private static ECKey ecKey() throws Exception {
    return new ECKeyGenerator(Curve.P_256).keyIDFromThumbprint(true).generate();
  }

  private static String jwks(JWK... keys) {
    return new JWKSet(List.of(keys)).toString(true);
  }

  @Test
  void initialLoadPopulatesKeys() throws Exception {
    ECKey k = ecKey();
    responses.add(new String[] {"200", jwks(k.toPublicJWK()), null});
    JwksCache cache = cache(httpSource());

    cache.refresh().join();

    assertThat(cache.key(k.getKeyID())).isPresent();
    assertThat(cache.size()).isEqualTo(1);
    assertThat(cache.ageSeconds()).isZero();
    assertThat(cache.refreshCount("updated")).isEqualTo(1);
  }

  @Test
  void refreshReplacesWholeSetAndDropsRemovedKid() throws Exception {
    ECKey a = ecKey();
    ECKey b = ecKey();
    responses.add(new String[] {"200", jwks(a.toPublicJWK()), null});
    responses.add(new String[] {"200", jwks(b.toPublicJWK()), null});
    JwksCache cache = cache(httpSource());

    cache.refresh().join();
    cache.refresh().join();

    assertThat(cache.key(a.getKeyID())).isEmpty();
    assertThat(cache.key(b.getKeyID())).isPresent();
  }

  @Test
  void notModifiedKeepsKeys() throws Exception {
    ECKey k = ecKey();
    responses.add(new String[] {"200", jwks(k.toPublicJWK()), null});
    responses.add(new String[] {"304", "", null});
    JwksCache cache = cache(httpSource());

    cache.refresh().join();
    cache.refresh().join();

    assertThat(cache.size()).isEqualTo(1);
    assertThat(cache.refreshCount("not_modified")).isEqualTo(1);
  }

  @Test
  void errorKeepsLastGoodSet() throws Exception {
    ECKey k = ecKey();
    responses.add(new String[] {"200", jwks(k.toPublicJWK()), null});
    responses.add(new String[] {"500", "boom", null});
    JwksCache cache = cache(httpSource());

    cache.refresh().join();
    clock.addAndGet(120_000);
    cache.refresh().join();

    assertThat(cache.key(k.getKeyID())).isPresent();
    assertThat(cache.refreshCount("error")).isEqualTo(1);
    assertThat(cache.ageSeconds()).isEqualTo(120);
  }

  @Test
  void neverLoadedReportsMinusOneAndRetriesIn10s() {
    responses.add(new String[] {"500", "x", null});
    JwksCache cache = cache(httpSource());

    cache.refresh().join();

    assertThat(cache.ageSeconds()).isEqualTo(-1);
    assertThat(cache.nextDelay()).isEqualTo(Duration.ofSeconds(10));
  }

  @Test
  void ignoresNonEs256Keys() throws Exception {
    ECKey good = ecKey();
    JWK rsa = new RSAKeyGenerator(2048).keyID("rsa").generate().toPublicJWK();
    JWK p384 = new ECKeyGenerator(Curve.P_384).keyID("p384").generate().toPublicJWK();
    responses.add(new String[] {"200", jwks(good.toPublicJWK(), rsa, p384), null});
    JwksCache cache = cache(httpSource());

    cache.refresh().join();

    assertThat(cache.size()).isEqualTo(1);
    assertThat(cache.key("rsa")).isEmpty();
  }

  @Test
  void emergencyRefreshIsSingleflightAndRateLimited() throws Exception {
    ECKey k = ecKey();
    responses.add(new String[] {"200", jwks(k.toPublicJWK()), null});
    JwksCache cache = new JwksCache(httpSource(), tick, offloadQueue::add, Duration.ofMinutes(5), clock::get);

    CompletableFuture<Boolean> first = cache.refreshFor(k.getKeyID());
    CompletableFuture<Boolean> second = cache.refreshFor(k.getKeyID());
    assertThat(offloadQueue).hasSize(1);
    offloadQueue.poll().run();

    assertThat(first.join()).isTrue();
    assertThat(second.join()).isTrue();
    assertThat(hits.get()).isEqualTo(1);

    clock.addAndGet(5_000);
    assertThat(cache.refreshFor("other-kid").join()).isFalse();
    assertThat(offloadQueue).isEmpty();
    assertThat(hits.get()).isEqualTo(1);
  }

  @Test
  void maxAgeClampedToBounds() throws Exception {
    ECKey k = ecKey();
    responses.add(new String[] {"200", jwks(k.toPublicJWK()), "max-age=5"});
    responses.add(new String[] {"200", jwks(k.toPublicJWK()), "public, max-age=99999"});
    responses.add(new String[] {"200", jwks(k.toPublicJWK()), null});
    JwksCache cache = cache(httpSource());

    cache.refresh().join();
    assertThat(cache.nextDelay()).isEqualTo(Duration.ofSeconds(60));
    cache.refresh().join();
    assertThat(cache.nextDelay()).isEqualTo(Duration.ofSeconds(3600));
    cache.refresh().join();
    assertThat(cache.nextDelay()).isEqualTo(Duration.ofMinutes(5));
  }

  @Test
  void fileSourceDetectsRewrite(@TempDir Path d) throws Exception {
    ECKey a = ecKey();
    ECKey b = ecKey();
    Path file = d.resolve("jwks.json");
    Files.writeString(file, jwks(a.toPublicJWK()));
    JwksCache cache = cache(new FileJwksSource(file));

    cache.refresh().join();
    assertThat(cache.key(a.getKeyID())).isPresent();
    assertThat(cache.nextDelay()).isEqualTo(Duration.ofSeconds(30));

    Files.writeString(file, jwks(b.toPublicJWK()));
    Files.setLastModifiedTime(file, java.nio.file.attribute.FileTime.fromMillis(System.currentTimeMillis() + 5_000));
    cache.refresh().join();

    assertThat(cache.key(b.getKeyID())).isPresent();
    assertThat(cache.key(a.getKeyID())).isEmpty();
  }

  @Test
  void bothSourcesConfiguredIsConfigError(@TempDir Path d) {
    GatewayProperties.Auth both = new GatewayProperties.Auth("http://x/jwks", d.resolve("j.json"),
        Duration.ofMinutes(5), "pangate-gw", "", Duration.ofSeconds(30));
    GatewayProperties.Auth none = new GatewayProperties.Auth("", null,
        Duration.ofMinutes(5), "pangate-gw", "", Duration.ofSeconds(30));

    assertThatThrownBy(() -> JwksSources.from(both, HttpClient.newHttpClient())).isInstanceOf(JwksConfigException.class);
    assertThatThrownBy(() -> JwksSources.from(none, HttpClient.newHttpClient())).isInstanceOf(JwksConfigException.class);
  }
}
