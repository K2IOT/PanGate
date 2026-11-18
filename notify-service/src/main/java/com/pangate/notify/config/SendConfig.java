package com.pangate.notify.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pangate.notify.application.NotifyDispatcher;
import com.pangate.notify.application.port.Dedup;
import com.pangate.notify.application.port.Delayer;
import com.pangate.notify.application.port.PushProvider;
import com.pangate.notify.application.port.TokenStore;
import com.pangate.notify.domain.Platform;
import com.pangate.notify.infrastructure.Pem;
import com.pangate.notify.infrastructure.apns.ApnsJwt;
import com.pangate.notify.infrastructure.apns.ApnsProvider;
import com.pangate.notify.infrastructure.fcm.FcmAccessToken;
import com.pangate.notify.infrastructure.fcm.FcmProvider;
import com.pangate.notify.infrastructure.kafka.NotifyRequestLoop;
import com.pangate.notify.infrastructure.redis.LazyRedis;
import com.pangate.notify.infrastructure.redis.RedisDedup;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.http.HttpClient;
import java.nio.file.Files;
import java.security.interfaces.ECPrivateKey;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Providers per tenant, the dispatcher and the request consumer (offline notifications spec §5.4, §5.5). */
@Configuration(proxyBeanMethods = false)
public class SendConfig {

  private static final Logger log = LoggerFactory.getLogger(SendConfig.class);

  /** Providers keyed {@code <tenant>/<platform>}; a tenant whose key file cannot be read is left out and logged. */
  @Bean
  Map<String, PushProvider> pushProviders(NotifyProperties p) {
    HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_2)
        .connectTimeout(Duration.ofSeconds(5)).build();
    Map<String, PushProvider> out = new HashMap<>();
    p.tenants().forEach((tenant, cfg) -> {
      if (cfg.apns() != null && cfg.apns().keyFile() != null) {
        try {
          if (p.devCredentials() && !Files.exists(cfg.apns().keyFile())) {
            DevCredentials.apnsKey(cfg.apns().keyFile());
          }
          ECPrivateKey key = (ECPrivateKey) Pem.privateKey(Files.readString(cfg.apns().keyFile()), "EC");
          out.put(tenant + "/" + Platform.APNS, new ApnsProvider(http, cfg.apns().baseUrl(),
              cfg.apns().sandboxBaseUrl(), cfg.apns().bundleId(),
              new ApnsJwt(key, cfg.apns().keyId(), cfg.apns().teamId(), System::currentTimeMillis)));
        } catch (Exception e) {
          log.error("APNs of tenant {} not configured: {}", tenant, e.toString());
        }
      }
      if (cfg.fcm() != null && cfg.fcm().serviceAccountFile() != null) {
        try {
          if (p.devCredentials() && !Files.exists(cfg.fcm().serviceAccountFile())) {
            DevCredentials.fcmServiceAccount(cfg.fcm().serviceAccountFile(), cfg.fcm().tokenUri());
          }
          JsonNode sa = new ObjectMapper().readTree(Files.readString(cfg.fcm().serviceAccountFile()));
          String tokenUri = cfg.fcm().tokenUri() != null ? cfg.fcm().tokenUri() : sa.get("token_uri").asText();
          FcmAccessToken auth = new FcmAccessToken(http, sa.get("client_email").asText(),
              Pem.privateKey(sa.get("private_key").asText(), "RSA"), tokenUri, System::currentTimeMillis);
          out.put(tenant + "/" + Platform.FCM, new FcmProvider(http, cfg.fcm().baseUrl(), cfg.fcm().projectId(), auth,
              System::currentTimeMillis));
        } catch (Exception e) {
          log.error("FCM of tenant {} not configured: {}", tenant, e.toString());
        }
      }
    });
    log.info("push providers: {}", out.keySet());
    return out;
  }

  @Bean
  Dedup dedup(LazyRedis redis) {
    return new RedisDedup(redis);
  }

  @Bean
  NotifyDispatcher notifyDispatcher(TokenStore store, Dedup dedup, Map<String, PushProvider> pushProviders,
      MeterRegistry meters, NotifyProperties p) {
    Delayer delayer = ms -> CompletableFuture.runAsync(() -> { },
        CompletableFuture.delayedExecutor(ms, TimeUnit.MILLISECONDS));
    return new NotifyDispatcher(store, dedup, (tenant, platform) -> pushProviders.get(tenant + "/" + platform),
        delayer, meters, p.attempts());
  }

  @Bean(destroyMethod = "")
  NotifyRequestLoop notifyRequestLoop(NotifyProperties p, NotifyDispatcher dispatcher, MeterRegistry meters) {
    return new NotifyRequestLoop(NotifyRequestLoop.consumer(p.kafka().bootstrap()), p.kafka().requestsTopic(),
        dispatcher, p.maxInFlight(), meters, System::currentTimeMillis);
  }

  @Bean
  SmartLifecycle notifyRequestLoopLifecycle(NotifyRequestLoop loop) {
    return new SmartLifecycle() {
      private volatile boolean running;

      @Override
      public void start() {
        loop.start();
        running = true;
      }

      @Override
      public void stop() {
        running = false;
        loop.close();
      }

      @Override
      public boolean isRunning() {
        return running;
      }
    };
  }
}
