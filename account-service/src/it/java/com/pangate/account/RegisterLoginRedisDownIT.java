package com.pangate.account;

import static com.pangate.account.support.Http.call;
import static com.pangate.account.support.Http.login;
import static com.pangate.account.support.Http.registration;
import static com.pangate.account.support.Http.unique;
import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.account.support.AccountIT;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.TestPropertySource;

/** account spec §7: without Redis the limits still hold, per pod. */
@AccountIT
@TestPropertySource(properties = "account.redis.uri=redis://127.0.0.1:1")
class RegisterLoginRedisDownIT {

  @LocalServerPort int port;

  @Test
  void redisDownStillLimits() {
    String username = unique("nored");
    assertThat(call(port, "POST", "/v1/accounts", null, registration(username, unique("n") + "@x.vn", null)).status())
        .isEqualTo(201);
    for (int i = 0; i < 10; i++) {
      assertThat(call(port, "POST", "/v1/sessions", null, login(username, "wrong-pw-1")).status()).isEqualTo(401);
    }
    var blocked = call(port, "POST", "/v1/sessions", null, login(username, "secret-pw-1"));
    assertThat(blocked.status()).isEqualTo(429);
    assertThat(blocked.body().get("code").asText()).isEqualTo("too_many_attempts");
    assertThat(blocked.raw().headers().firstValue("Retry-After")).isPresent();
  }
}
