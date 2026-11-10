package com.pangate.auth.config;

import com.pangate.auth.api.ApiAuthFilter;
import com.pangate.auth.api.BodyLimitFilter;
import com.pangate.auth.api.RateLimitFilter;
import com.pangate.auth.infrastructure.AuthMetrics;
import com.pangate.auth.infrastructure.ratelimit.FailedAuthTracker;
import com.pangate.auth.infrastructure.ratelimit.TokenBuckets;
import java.util.Map;
import com.pangate.auth.api.PortRoutingFilter;
import com.pangate.auth.application.IssueClientToken;
import com.pangate.auth.application.KeyRing;
import com.pangate.auth.application.ManageClients;
import com.pangate.auth.application.ManageTenants;
import com.pangate.auth.application.MintConnectToken;
import com.pangate.auth.application.Revoke;
import com.pangate.auth.application.port.ClientStore;
import com.pangate.auth.application.port.Clock;
import com.pangate.auth.application.port.RevocationStore;
import com.pangate.auth.application.port.SecretHasher;
import com.pangate.auth.application.port.TenantStore;
import com.pangate.auth.application.port.TokenSigner;
import com.pangate.auth.infrastructure.postgres.JdbcClientStore;
import com.pangate.auth.infrastructure.postgres.JdbcTenantStore;
import java.time.Duration;
import java.util.Set;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.jdbc.core.simple.JdbcClient;

/** Stores, use cases and the request filters. */
@Configuration(proxyBeanMethods = false)
public class WebConfig {

  static final Duration AUTH_CACHE_TTL = Duration.ofSeconds(10);

  @Bean
  TenantStore tenantStore(JdbcClient jdbc) {
    return new JdbcTenantStore(jdbc);
  }

  @Bean
  ClientStore clientStore(JdbcClient jdbc) {
    return new JdbcClientStore(jdbc);
  }

  @Bean
  ManageTenants manageTenants(TenantStore tenants) {
    return new ManageTenants(tenants);
  }

  @Bean
  ManageClients manageClients(ClientStore clients, TenantStore tenants, SecretHasher hasher, Revoke revoke,
      com.pangate.auth.application.port.Transactions tx) {
    return new ManageClients(clients, tenants, hasher, revoke, tx);
  }

  @Bean
  IssueClientToken issueClientToken(ClientStore clients, TenantStore tenants, SecretHasher hasher, TokenSigner signer,
      KeyRing keys, Clock clock, AuthProperties props) {
    return new IssueClientToken(clients, tenants, hasher, signer, keys, clock, props.issuer(), props.apiTokenTtl());
  }

  @Bean
  MintConnectToken mintConnectToken(TenantStore tenants, TokenSigner signer, KeyRing keys, Clock clock,
      AuthProperties props) {
    return new MintConnectToken(tenants, signer, keys, clock, props.issuer(), props.connectTokenMaxTtl());
  }

  @Bean
  TtlCache<Set<String>> disabledClientsCache(ClientStore clients) {
    return new TtlCache<>(clients::disabledIds, AUTH_CACHE_TTL, Set.of(), System::nanoTime);
  }

  @Bean
  JtiRevocationCache jtiRevocationCache(RevocationStore revocations) {
    return new JtiRevocationCache(revocations::isJtiRevoked, AUTH_CACHE_TTL, 100_000, System::nanoTime);
  }

  @Bean
  FilterRegistrationBean<PortRoutingFilter> portRoutingFilter(AdminConnector admin) {
    var r = new FilterRegistrationBean<>(new PortRoutingFilter(admin));
    r.setOrder(Ordered.HIGHEST_PRECEDENCE);
    r.addUrlPatterns("/*");
    return r;
  }

  @Bean
  AuthMetrics authMetrics(io.micrometer.core.instrument.MeterRegistry registry, KeyRing keys) {
    return new AuthMetrics(registry, keys);
  }

  @Bean
  TokenBuckets tokenBuckets(AuthProperties props) {
    var l = props.rateLimits();
    return new TokenBuckets(Map.of(
        "token", new TokenBuckets.Limit(l.token().rate(), l.token().burst()),
        "mint", new TokenBuckets.Limit(l.mint().rate(), l.mint().burst()),
        "revoke", new TokenBuckets.Limit(l.revoke().rate(), l.revoke().burst()),
        "admin", new TokenBuckets.Limit(l.admin().rate(), l.admin().burst())), System::nanoTime);
  }

  @Bean
  FailedAuthTracker failedAuthTracker(AuthProperties props) {
    var f = props.rateLimits().authFailures();
    return new FailedAuthTracker(f.perMinute(), f.block(), System::nanoTime);
  }

  @Bean
  FilterRegistrationBean<BodyLimitFilter> bodyLimitFilter() {
    var r = new FilterRegistrationBean<>(new BodyLimitFilter());
    r.setOrder(Ordered.HIGHEST_PRECEDENCE + 5);
    r.addUrlPatterns("/*");
    return r;
  }

  @Bean
  FilterRegistrationBean<RateLimitFilter> rateLimitFilter(TokenBuckets buckets, AuthMetrics metrics) {
    var r = new FilterRegistrationBean<>(new RateLimitFilter(buckets, metrics));
    r.setOrder(Ordered.HIGHEST_PRECEDENCE + 20);
    r.addUrlPatterns("/v1/*", "/admin/v1/*");
    return r;
  }

  @Bean
  FilterRegistrationBean<ApiAuthFilter> apiAuthFilter(TokenSigner signer, KeyRing keys, Clock clock,
      TtlCache<Set<String>> disabledClientsCache, JtiRevocationCache jtiRevocationCache) {
    var r = new FilterRegistrationBean<>(new ApiAuthFilter(signer, keys, clock, disabledClientsCache,
        jtiRevocationCache::isRevoked));
    r.setOrder(Ordered.HIGHEST_PRECEDENCE + 10);
    r.addUrlPatterns("/v1/*", "/admin/v1/*");
    return r;
  }
}
