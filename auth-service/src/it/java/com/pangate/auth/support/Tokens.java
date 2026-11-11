package com.pangate.auth.support;

import com.pangate.auth.application.KeyRing;
import com.pangate.auth.application.port.TokenSigner;
import com.pangate.auth.config.AuthProperties;
import com.pangate.auth.domain.ApiClaims;
import com.pangate.auth.domain.Scope;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import org.springframework.context.ApplicationContext;

/** API tokens signed directly with the active key (before / besides the token endpoint). */
public final class Tokens {

  private Tokens() {}

  public static String api(ApplicationContext ctx, String clientId, String tenant, Set<Scope> scopes) {
    Instant now = Instant.now();
    return ctx.getBean(TokenSigner.class).sign(ctx.getBean(KeyRing.class).active(), new ApiClaims(
        ctx.getBean(AuthProperties.class).issuer(), clientId, tenant, scopes, UUID.randomUUID().toString(), now,
        now.plusSeconds(900)));
  }

  public static String admin(ApplicationContext ctx) {
    return api(ctx, "pangate-admin", "pangate", Set.of(Scope.ADMIN));
  }
}
