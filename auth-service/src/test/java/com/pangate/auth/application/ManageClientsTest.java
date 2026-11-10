package com.pangate.auth.application;

import static com.pangate.auth.domain.ClaimLimitsTest.assertCode;
import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.auth.domain.Client;
import com.pangate.auth.domain.Scope;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ManageClientsTest {

  private final InMemoryStores.Tenants tenants = new InMemoryStores.Tenants();
  private final InMemoryStores.Clients clients = new InMemoryStores.Clients();
  private final FakeHasher hasher = new FakeHasher();
  private final ManageTenants manageTenants = new ManageTenants(tenants);
  private final ManageClients manage = new ManageClients(clients, tenants, hasher);

  @BeforeEach
  void tenants() {
    manageTenants.create("t1");
    manageTenants.create("pangate");
  }

  @Test
  void createReturnsSecretOnceAndStoresOnlyAHash() {
    var created = manage.create("t1", "backend", Set.of(Scope.TOKENS_MINT));
    assertThat(created.clientId()).isEqualTo("t1-backend");
    assertThat(java.util.Base64.getUrlDecoder().decode(created.plainSecret())).hasSize(32);
    Client stored = clients.rows.get("t1-backend");
    assertThat(stored.secretHash()).isEqualTo("h:" + created.plainSecret()).doesNotContain("backend");
    assertThat(stored.scopes()).containsExactly(Scope.TOKENS_MINT);
    assertThat(stored.active()).isTrue();
    assertCode(() -> manage.create("t1", "backend", Set.of(Scope.TOKENS_MINT)), "client_exists");
  }

  @Test
  void rotationAcceptsBothSecretsUntilFinished() {
    var created = manage.create("t1", "backend", Set.of(Scope.TOKENS_MINT));
    String next = manage.rotateSecret("t1-backend");
    assertThat(manage.authenticates("t1-backend", created.plainSecret().toCharArray())).isTrue();
    assertThat(manage.authenticates("t1-backend", next.toCharArray())).isTrue();
    manage.finishRotation("t1-backend");
    assertThat(manage.authenticates("t1-backend", created.plainSecret().toCharArray())).isFalse();
    assertThat(manage.authenticates("t1-backend", next.toCharArray())).isTrue();
    assertCode(() -> manage.finishRotation("t1-backend"), "no_rotation_in_progress");
  }

  @Test
  void adminScopeRefusedForCustomerTenant() {
    assertCode(() -> manage.create("t1", "ops", Set.of(Scope.ADMIN)), "scope_not_allowed");
    assertThat(manage.create("pangate", "admin", Set.of(Scope.ADMIN)).clientId()).isEqualTo("pangate-admin");
  }

  @Test
  void unknownTenantOrClientIs404AndBadNamesAre400() {
    assertThat(assertCode(() -> manage.create("t9", "x", Set.of()), "tenant_not_found").httpStatus()).isEqualTo(404);
    assertThat(assertCode(() -> manage.rotateSecret("t1-nope"), "client_not_found").httpStatus()).isEqualTo(404);
    assertCode(() -> manage.create("t1", "Bad Name", Set.of()), "client_name_invalid");
    assertCode(() -> manageTenants.create("__warmup"), "tenant_invalid");
    assertCode(() -> manageTenants.create("T1"), "tenant_invalid");
    assertCode(() -> manageTenants.create("t1"), "tenant_exists");
  }

  @Test
  void disableMarksTheClient() {
    manage.create("t1", "backend", Set.of(Scope.TOKENS_MINT));
    manage.disable("t1-backend");
    assertThat(clients.disabledIds()).containsExactly("t1-backend");
  }

  @Test
  void clientNamesCannotContainTheIdSeparator() {
    manageTenants.create("t1-a");
    assertCode(() -> manage.create("t1", "a-b", Set.of()), "client_name_invalid");
    assertThat(manage.create("t1-a", "b", Set.of()).clientId()).isEqualTo("t1-a-b");
    assertThat(manage.create("t1", "a_b", Set.of()).clientId()).isEqualTo("t1-a_b");
  }

  @Test
  void disableAndItsRevocationAreOneTransaction() {
    manage.create("t1", "backend", Set.of(Scope.TOKENS_MINT));
    // a transaction that restores the rows when the work throws
    com.pangate.auth.application.port.Transactions tx = work -> {
      var before = new java.util.TreeMap<>(clients.rows);
      try {
        work.run();
      } catch (RuntimeException e) {
        clients.rows.clear();
        clients.rows.putAll(before);
        throw e;
      }
    };
    var failing = new ManageClients(clients, tenants, hasher, (actor, id, t) -> {
      throw new IllegalStateException("db down");
    }, tx);
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> failing.disable("admin", "t1-backend"))
        .hasMessageContaining("db down");
    assertThat(clients.rows.get("t1-backend").active()).isTrue();
  }
}
