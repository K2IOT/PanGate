package com.pangate.auth.config;

import com.pangate.auth.api.ApiAuthFilter;
import com.pangate.auth.api.PortRoutingFilter;
import com.pangate.auth.application.KeyRing;
import com.pangate.auth.application.ManageClients;
import com.pangate.auth.application.ManageTenants;
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
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
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

  /** Replaced by the JDBC store with the revocations work; no jti is revoked until then. */
  @Bean
  @ConditionalOnMissingBean
  RevocationStore revocationStore() {
    return Set::of;
  }

  @Bean
  ManageTenants manageTenants(TenantStore tenants) {
    return new ManageTenants(tenants);
  }

  @Bean
  ManageClients manageClients(ClientStore clients, TenantStore tenants, SecretHasher hasher) {
    return new ManageClients(clients, tenants, hasher);
  }

  @Bean
  TtlCache<Set<String>> disabledClientsCache(ClientStore clients) {
    return new TtlCache<>(clients::disabledIds, AUTH_CACHE_TTL, Set.of(), System::nanoTime);
  }

  @Bean
  TtlCache<Set<String>> revokedJtisCache(RevocationStore revocations) {
    return new TtlCache<>(revocations::activeJtis, AUTH_CACHE_TTL, Set.of(), System::nanoTime);
  }

  @Bean
  FilterRegistrationBean<PortRoutingFilter> portRoutingFilter(AdminConnector admin) {
    var r = new FilterRegistrationBean<>(new PortRoutingFilter(admin));
    r.setOrder(Ordered.HIGHEST_PRECEDENCE);
    r.addUrlPatterns("/*");
    return r;
  }

  @Bean
  FilterRegistrationBean<ApiAuthFilter> apiAuthFilter(TokenSigner signer, KeyRing keys, Clock clock,
      TtlCache<Set<String>> disabledClientsCache, TtlCache<Set<String>> revokedJtisCache) {
    var r = new FilterRegistrationBean<>(new ApiAuthFilter(signer, keys, clock, disabledClientsCache,
        revokedJtisCache));
    r.setOrder(Ordered.HIGHEST_PRECEDENCE + 10);
    r.addUrlPatterns("/v1/*", "/admin/v1/*");
    return r;
  }
}
