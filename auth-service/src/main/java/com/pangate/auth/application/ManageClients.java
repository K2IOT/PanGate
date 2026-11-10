package com.pangate.auth.application;

import com.pangate.auth.application.port.ClientStore;
import com.pangate.auth.application.port.SecretHasher;
import com.pangate.auth.application.port.TenantStore;
import com.pangate.auth.domain.Client;
import com.pangate.auth.domain.DomainException;
import com.pangate.auth.domain.Scope;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/** Admin use cases on API clients (spec §4.5). Secrets are returned once and stored only as hashes. */
public final class ManageClients {

  static final Pattern NAME = Pattern.compile("^[a-z0-9][a-z0-9_-]{0,62}$");
  private static final SecureRandom RANDOM = new SecureRandom();

  public record CreatedClient(String clientId, String plainSecret, Set<Scope> scopes) {}

  /** Publishes {@code revoke:c:<client_id>} so Push API stops accepting the client's tokens (spec §4.5). */
  @FunctionalInterface
  public interface ClientRevoker {
    void revokeClient(String actor, String clientId, String tenantId);
  }

  private final ClientStore clients;
  private final TenantStore tenants;
  private final SecretHasher hasher;
  private final ClientRevoker revoker;

  public ManageClients(ClientStore clients, TenantStore tenants, SecretHasher hasher) {
    this(clients, tenants, hasher, (actor, id, tenant) -> { });
  }

  public ManageClients(ClientStore clients, TenantStore tenants, SecretHasher hasher, ClientRevoker revoker) {
    this.clients = clients;
    this.tenants = tenants;
    this.hasher = hasher;
    this.revoker = revoker;
  }

  public static String newSecret() {
    byte[] b = new byte[32];
    RANDOM.nextBytes(b);
    return Base64.getUrlEncoder().withoutPadding().encodeToString(b);
  }

  public CreatedClient create(String tenantId, String name, Set<Scope> scopes) {
    String secret = newSecret();
    String id = insert(tenantId, name, scopes, secret);
    return new CreatedClient(id, secret, Set.copyOf(scopes));
  }

  private String insert(String tenantId, String name, Set<Scope> scopes, String secret) {
    tenants.find(tenantId).orElseThrow(() -> ManageTenants.notFound(tenantId));
    if (name == null || !NAME.matcher(name).matches()) {
      throw DomainException.badRequest("client_name_invalid", "client name must match " + NAME.pattern());
    }
    Client.checkScopes(tenantId, scopes);
    String id = tenantId + "-" + name;
    clients.insert(new Client(id, tenantId, hasher.hash(secret.toCharArray()), null, Set.copyOf(scopes), true));
    return id;
  }

  /**
   * Dev seed: makes client {@code tenantId-name} exist with these scopes and accept {@code secret} (the seed file
   * wins over a stale hash). @return true if anything was written
   */
  public boolean ensure(String tenantId, String name, Set<Scope> scopes, String secret) {
    String id = tenantId + "-" + name;
    var existing = clients.find(id);
    if (existing.isEmpty()) {
      insert(tenantId, name, scopes, secret);
      return true;
    }
    Client c = existing.get();
    if (hasher.matches(secret.toCharArray(), c.secretHash()) && c.scopes().equals(scopes) && c.active()) {
      return false;
    }
    clients.update(new Client(id, tenantId, hasher.hash(secret.toCharArray()), null, Set.copyOf(scopes), true));
    return true;
  }

  public List<Client> list(String tenantId) {
    tenants.find(tenantId).orElseThrow(() -> ManageTenants.notFound(tenantId));
    return clients.listByTenant(tenantId);
  }

  /** New secret in {@code secret_hash_next}; both secrets are valid until {@link #finishRotation}. */
  public String rotateSecret(String clientId) {
    Client c = get(clientId);
    String secret = newSecret();
    clients.update(new Client(c.id(), c.tenantId(), c.secretHash(), hasher.hash(secret.toCharArray()), c.scopes(),
        c.active()));
    return secret;
  }

  public void finishRotation(String clientId) {
    Client c = get(clientId);
    if (c.secretHashNext() == null) {
      throw new DomainException("no_rotation_in_progress", 409, "client " + clientId + " has no pending secret");
    }
    clients.update(new Client(c.id(), c.tenantId(), c.secretHashNext(), null, c.scopes(), c.active()));
  }

  public void disable(String clientId) {
    disable("system", clientId);
  }

  /** {@code status = disabled} and a revocation of kind {@code client}. */
  public void disable(String actor, String clientId) {
    Client c = get(clientId);
    clients.update(new Client(c.id(), c.tenantId(), c.secretHash(), c.secretHashNext(), c.scopes(), false));
    revoker.revokeClient(actor, c.id(), c.tenantId());
  }

  /** Secret matches the current or the next hash (constant-time comparison inside the hasher). */
  public boolean authenticates(String clientId, char[] secret) {
    return clients.find(clientId).map(c -> hasher.matches(secret, c.secretHash())
        || hasher.matches(secret, c.secretHashNext())).orElse(false);
  }

  public Client get(String clientId) {
    return clients.find(clientId).orElseThrow(() -> new DomainException("client_not_found", 404,
        "no client " + clientId));
  }
}
