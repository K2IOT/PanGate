package com.pangate.auth.api;

import com.pangate.auth.application.ManageClients;
import com.pangate.auth.application.ManageTenants;
import com.pangate.auth.config.JobsConfig.KeyJobs;
import com.pangate.auth.domain.Caller;
import com.pangate.auth.domain.Client;
import com.pangate.auth.domain.Scope;
import com.pangate.auth.domain.Tenant;
import jakarta.servlet.http.HttpServletRequest;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Internal Admin API on the management port, scope {@code admin} (spec §4.5). Every action is audit-logged. */
@RestController
@RequestMapping("/admin/v1")
public class AdminController {

  private static final Logger audit = LoggerFactory.getLogger("pangate.audit");

  public record TenantBody(String id) {}

  public record ClientBody(String name, List<String> scopes) {}

  public record TenantView(String id, String status) {
    static TenantView of(Tenant t) {
      return new TenantView(t.id(), t.active() ? "active" : "disabled");
    }
  }

  public record ClientView(String clientId, String tenant, List<String> scopes, String status, boolean rotating) {
    static ClientView of(Client c) {
      return new ClientView(c.id(), c.tenantId(), c.scopes().stream().sorted().map(Scope::value).toList(),
          c.active() ? "active" : "disabled", c.secretHashNext() != null);
    }
  }

  private final ManageTenants tenants;
  private final ManageClients clients;
  private final KeyJobs keys;

  public AdminController(ManageTenants tenants, ManageClients clients, KeyJobs keys) {
    this.tenants = tenants;
    this.clients = clients;
    this.keys = keys;
  }

  private <T> T act(HttpServletRequest req, String action, String target, Supplier<T> body) {
    Caller p = ApiPrincipal.of(req).require(Scope.ADMIN);
    try {
      T out = body.get();
      audit.atInfo().addKeyValue("admin_action", action).addKeyValue("actor", p.clientId())
          .addKeyValue("target", target).addKeyValue("result", "ok").log("admin action");
      return out;
    } catch (RuntimeException e) {
      audit.atInfo().addKeyValue("admin_action", action).addKeyValue("actor", p.clientId())
          .addKeyValue("target", target).addKeyValue("result", "error").log("admin action failed");
      throw e;
    }
  }

  @PostMapping("/tenants")
  ResponseEntity<TenantView> createTenant(HttpServletRequest req, @RequestBody TenantBody b) {
    return ResponseEntity.status(HttpStatus.CREATED)
        .body(act(req, "tenant.create", b.id(), () -> TenantView.of(tenants.create(b.id()))));
  }

  @GetMapping("/tenants")
  List<TenantView> listTenants(HttpServletRequest req) {
    return act(req, "tenant.list", "*", () -> tenants.list().stream().map(TenantView::of).toList());
  }

  @GetMapping("/tenants/{t}")
  TenantView getTenant(HttpServletRequest req, @PathVariable String t) {
    return act(req, "tenant.get", t, () -> TenantView.of(tenants.get(t)));
  }

  @PostMapping("/tenants/{t}:disable")
  TenantView disableTenant(HttpServletRequest req, @PathVariable String t) {
    return act(req, "tenant.disable", t, () -> TenantView.of(tenants.disable(t)));
  }

  @PostMapping("/tenants/{t}:enable")
  TenantView enableTenant(HttpServletRequest req, @PathVariable String t) {
    return act(req, "tenant.enable", t, () -> TenantView.of(tenants.enable(t)));
  }

  @PostMapping("/tenants/{t}/clients")
  ResponseEntity<Map<String, Object>> createClient(HttpServletRequest req, @PathVariable String t,
      @RequestBody ClientBody b) {
    Set<Scope> scopes = EnumSet.noneOf(Scope.class);
    if (b.scopes() != null) {
      b.scopes().forEach(s -> scopes.add(Scope.of(s)));
    }
    var created = act(req, "client.create", t + "-" + b.name(), () -> clients.create(t, b.name(), scopes));
    return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("clientId", created.clientId(), "secret",
        created.plainSecret(), "scopes", scopes.stream().map(Scope::value).toList()));
  }

  @GetMapping("/tenants/{t}/clients")
  List<ClientView> listClients(HttpServletRequest req, @PathVariable String t) {
    return act(req, "client.list", t, () -> clients.list(t).stream().map(ClientView::of).toList());
  }

  @PostMapping("/clients/{id}:rotate-secret")
  Map<String, String> rotateSecret(HttpServletRequest req, @PathVariable String id) {
    return Map.of("clientId", id, "secret", act(req, "client.rotate-secret", id, () -> clients.rotateSecret(id)));
  }

  @PostMapping("/clients/{id}:finish-rotation")
  ClientView finishRotation(HttpServletRequest req, @PathVariable String id) {
    return act(req, "client.finish-rotation", id, () -> {
      clients.finishRotation(id);
      return ClientView.of(clients.get(id));
    });
  }

  @PostMapping("/clients/{id}:disable")
  ClientView disableClient(HttpServletRequest req, @PathVariable String id) {
    return act(req, "client.disable", id, () -> {
      clients.disable(id);
      return ClientView.of(clients.get(id));
    });
  }

  @PostMapping("/keys:rotate-now")
  ResponseEntity<Map<String, Boolean>> rotateNow(HttpServletRequest req) {
    boolean created = act(req, "keys.rotate-now", "signing-keys", keys::forcePending);
    return ResponseEntity.status(HttpStatus.ACCEPTED).body(Map.of("created", created));
  }
}
