package com.pangate.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.nimbusds.jose.jwk.JWKSet;
import com.pangate.auth.application.KeyRing;
import com.pangate.auth.application.RotateSigningKeys;
import com.pangate.auth.application.port.TokenSigner;
import com.pangate.auth.config.AuthProperties;
import com.pangate.auth.config.JobsConfig.KeyJobs;
import com.pangate.auth.domain.ApiClaims;
import com.pangate.auth.domain.Scope;
import com.pangate.auth.infrastructure.postgres.AdvisoryLock;
import com.pangate.auth.infrastructure.postgres.JdbcSigningKeyStore;
import com.pangate.auth.support.AuthApp;
import com.pangate.auth.support.Containers;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.simple.JdbcClient;

/** Two Auth "pods" on one database (spec §3, §5, §12 item 5). */
class KeyRotationIT {

  private final List<ConfigurableApplicationContext> pods = new ArrayList<>();
  private final HttpClient http = HttpClient.newHttpClient();

  @AfterEach
  void close() {
    pods.forEach(ConfigurableApplicationContext::close);
  }

  private ConfigurableApplicationContext pod(String url, String... props) {
    var ctx = AuthApp.start(url, props);
    pods.add(ctx);
    return ctx;
  }

  private Set<String> jwksKids(ConfigurableApplicationContext ctx) throws Exception {
    var res = http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + AuthApp.port(ctx)
        + "/.well-known/jwks.json")).build(), HttpResponse.BodyHandlers.ofString());
    assertThat(res.statusCode()).isEqualTo(200);
    assertThat(res.headers().firstValue("Cache-Control")).hasValueSatisfying(v -> assertThat(v)
        .contains("max-age=300").contains("public"));
    return JWKSet.parse(res.body()).getKeys().stream().map(k -> k.getKeyID()).collect(Collectors.toSet());
  }

  /** RotateSigningKeys of {@code ctx} driven by a fake clock, under the real advisory lock. */
  private static final class Driver {
    final AtomicReference<Instant> now;
    final RotateSigningKeys rotate;
    final AdvisoryLock lock;
    final KeyRing ring;

    Driver(ConfigurableApplicationContext ctx, Instant start) {
      now = new AtomicReference<>(start);
      rotate = new RotateSigningKeys(ctx.getBean(JdbcSigningKeyStore.class), now::get,
          ctx.getBean(AuthProperties.class).keys());
      lock = ctx.getBean(AdvisoryLock.class);
      ring = ctx.getBean(KeyRing.class);
    }

    void pass(Duration sinceStart, Instant start) {
      now.set(start.plus(sinceStart));
      // the pod's own scheduled pass may hold the lock for a moment: retry until this pass really ran
      await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(50))
          .until(() -> lock.runExclusive("auth.rotate-keys", rotate::runOnce));
      ring.reload();
    }
  }

  private static List<String> states(ConfigurableApplicationContext ctx) {
    return ctx.getBean(JdbcClient.class).sql("SELECT state FROM auth.signing_keys ORDER BY state").query(String.class)
        .list();
  }

  @Test
  void onlyOneContextRotatesAtATime() throws Exception {
    String url = Containers.freshDatabase("rot_lock");
    var a = pod(url);
    var b = pod(url);
    assertThat(states(a)).containsExactly("active");  // b found a's key at startup instead of creating one

    for (int round = 0; round < 10; round++) {
      a.getBean(JdbcClient.class).sql("DELETE FROM auth.signing_keys").update();
      var go = new CountDownLatch(1);
      Instant t = Instant.now();
      var da = new Driver(a, t);
      var db = new Driver(b, t);
      var fa = CompletableFuture.runAsync(() -> awaitThen(go, () -> da.pass(Duration.ZERO, t)));
      var fb = CompletableFuture.runAsync(() -> awaitThen(go, () -> db.pass(Duration.ZERO, t)));
      go.countDown();
      CompletableFuture.allOf(fa, fb).get();  // no exception escapes
      assertThat(states(a)).as("round " + round).containsExactly("active");
    }
  }

  private static void awaitThen(CountDownLatch l, Runnable r) {
    try {
      l.await();
    } catch (InterruptedException e) {
      throw new IllegalStateException(e);
    }
    r.run();
  }

  @Test
  void otherPodSeesPendingKeyWithinCacheRefresh() throws Exception {
    String url = Containers.freshDatabase("rot_refresh");
    var a = pod(url);
    var b = pod(url, "auth.keys.cache-refresh=1s");
    Set<String> before = jwksKids(b);
    assertThat(before).hasSize(1);

    assertThat(a.getBean(KeyJobs.class).forcePending()).isTrue();

    Set<String> onA = jwksKids(a);
    assertThat(onA).hasSize(2).containsAll(before);
    await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(jwksKids(b)).isEqualTo(onA));
  }

  @Test
  void jwksListsPendingActiveAndRetired() throws Exception {
    String url = Containers.freshDatabase("rot_states");
    var a = pod(url);
    Instant t = Instant.now();
    var d = new Driver(a, t);
    d.pass(Duration.ofDays(29), t);                 // pending
    d.pass(Duration.ofDays(29).plusHours(1), t);    // pending -> active, old -> retired
    d.rotate.forcePending();                        // a new pending
    d.ring.reload();

    assertThat(states(a)).containsExactly("active", "pending", "retired");
    assertThat(jwksKids(a)).hasSize(3);
  }

  @Test
  void tokenSignedWithOldKeyVerifiesThroughTheOverlap() throws Exception {
    String url = Containers.freshDatabase("rot_overlap");
    var a = pod(url);
    var signer = a.getBean(TokenSigner.class);
    var ring = a.getBean(KeyRing.class);
    String iss = a.getBean(AuthProperties.class).issuer();
    Instant t = Instant.now().truncatedTo(ChronoUnit.SECONDS);
    String oldKid = ring.active().kid();
    String oldToken = signer.sign(ring.active(), new ApiClaims(iss, "t1-backend", "t1", EnumSet.of(Scope.PUSH_SEND),
        "j-old", t, t.plusSeconds(900)));

    Instant clock0 = Instant.now();  // not truncated: never earlier than the first key's activate_at
    var d = new Driver(a, clock0);
    d.pass(Duration.ofDays(29), clock0);
    d.pass(Duration.ofDays(29).plusHours(1), clock0);
    String newKid = ring.active().kid();
    assertThat(newKid).isNotEqualTo(oldKid);

    // throughout the overlap the old key stays published and verifies its tokens; new tokens use the new key
    for (Duration step : List.of(Duration.ofDays(30), Duration.ofDays(33), Duration.ofDays(36))) {
      d.pass(step, clock0);
      assertThat(jwksKids(a)).as(step.toString()).contains(oldKid, newKid);
      assertThat(signer.verifyApi(oldToken, ring::publicKey, t)).as(step.toString()).isPresent();
      String fresh = signer.sign(ring.active(), new ApiClaims(iss, "t1-backend", "t1", EnumSet.of(Scope.PUSH_SEND),
          "j-new", t, t.plusSeconds(900)));
      assertThat(com.nimbusds.jwt.SignedJWT.parse(fresh).getHeader().getKeyID()).isEqualTo(newKid);
    }

    d.pass(Duration.ofDays(36).plusHours(1), clock0);    // retire + 7 d: removed
    assertThat(jwksKids(a)).containsExactly(newKid);
    assertThat(signer.verifyApi(oldToken, ring::publicKey, t)).isEmpty();
  }
}
