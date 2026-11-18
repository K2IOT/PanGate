package com.pangate.account;

import static com.pangate.account.support.Http.call;
import static com.pangate.account.support.Http.unique;
import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.account.support.AccountIT;
import com.pangate.account.support.Accounts;
import com.pangate.account.support.Accounts.Device;
import com.pangate.account.support.Http.Res;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.web.server.LocalServerPort;

@AccountIT
class ProfileIT {

  @LocalServerPort int port;

  @Test
  void renameFreesTheOldUsername() {
    String old = unique("al");
    String next = unique("Al");
    Device a = Accounts.register(port, old);
    String ta = a.connectToken(port);
    assertThat(call(port, "PUT", "/v1/me/username", ta, Map.of("username", next)).status()).isEqualTo(204);
    Device b = Accounts.register(port, old);
    String tb = b.connectToken(port);
    Res found = call(port, "GET", "/v1/users?username=" + next.toUpperCase(), tb, null);
    assertThat(found.status()).isEqualTo(200);
    assertThat(found.body().get("userId").asText()).isEqualTo(a.userId());
    assertThat(found.body().get("username").asText()).isEqualTo(next);
    assertThat(call(port, "GET", "/v1/users?username=" + unique("nobody"), tb, null).status()).isEqualTo(404);
    Res batch = call(port, "POST", "/v1/users:batch", tb, Map.of("userIds", List.of(a.userId(), b.userId())));
    assertThat(batch.body().get("users")).hasSize(2);
    Res me = call(port, "GET", "/v1/me", ta, null);
    assertThat(me.body().get("username").asText()).isEqualTo(next);
    assertThat(me.body().get("email").asText()).startsWith(old.toLowerCase() + ".").endsWith("@x.vn");
    assertThat(me.body().get("emailVerified").asBoolean()).isFalse();
    assertThat(call(port, "PATCH", "/v1/me", ta, Map.of("displayName", "Alice", "bio", "hello")).status())
        .isEqualTo(204);
    Res pub = call(port, "GET", "/v1/users/" + a.userId(), tb, null);
    assertThat(pub.body().get("displayName").asText()).isEqualTo("Alice");
    assertThat(pub.body().has("email")).isFalse();
  }

  @Test
  void passwordChangeLogsOutOtherDevices() {
    String name = unique("pc");
    Device d1 = Accounts.register(port, name);
    Device d2 = Accounts.login(port, name, "secret-pw-1");
    String t2 = d2.connectToken(port);
    Res wrong = call(port, "POST", "/v1/me/password", t2, Map.of("currentPassword", "nope-nope-1",
        "newPassword", "secret-pw-2"));
    assertThat(wrong.status()).isEqualTo(401);
    assertThat(call(port, "POST", "/v1/me/password", t2, Map.of("currentPassword", "secret-pw-1",
        "newPassword", "secret-pw-2")).status()).isEqualTo(204);
    assertThat(call(port, "POST", "/v1/sessions/token", d1.refreshToken(), null).status()).isEqualTo(401);
    assertThat(call(port, "POST", "/v1/sessions/token", d2.refreshToken(), null).status()).isEqualTo(200);
    assertThat(call(port, "POST", "/v1/sessions", null, com.pangate.account.support.Http.login(name, "secret-pw-2"))
        .status()).isEqualTo(201);
  }

  /** Offline notifications spec §2.2: account.users carries the display name after a rename. */
  @Test
  void renameReachesAccountUsersWithTheName() throws Exception {
    Device a = Accounts.register(port, unique("dn"));
    String ta = a.connectToken(port);
    assertThat(call(port, "PATCH", "/v1/me", ta, Map.of("displayName", "Lan Anh")).status()).isEqualTo(204);
    var recs = com.pangate.account.support.ControlTopic.await("account.users", rs -> rs.stream()
        .filter(r -> a.userId().equals(r.key())).anyMatch(r -> {
          try {
            return com.pangate.contracts.v1.AccountState.parseFrom(r.value()).getDisplayName().equals("Lan Anh");
          } catch (Exception e) {
            return false;
          }
        }), java.time.Duration.ofSeconds(20));
    assertThat(recs.stream().filter(r -> a.userId().equals(r.key())).map(r -> {
      try {
        return com.pangate.contracts.v1.AccountState.parseFrom(r.value()).getDisplayName();
      } catch (Exception e) {
        return "";
      }
    })).contains("Lan Anh");
  }
}
