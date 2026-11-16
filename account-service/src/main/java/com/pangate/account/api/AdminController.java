package com.pangate.account.api;

import com.pangate.account.application.AdminAccounts;
import com.pangate.account.domain.Account;
import com.pangate.account.domain.DomainException;
import com.pangate.account.domain.LoginKind;
import jakarta.servlet.http.HttpServletRequest;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** {@code /admin/v1/accounts*} on the management port (account spec §5). Bodies: {@code {reason}}. */
@RestController
public class AdminController {

  private final AdminAccounts admin;

  public AdminController(AdminAccounts admin) {
    this.admin = admin;
  }

  static String actor(HttpServletRequest req) {
    return (String) req.getAttribute(AdminAuthFilter.ACTOR);
  }

  static Map<String, Object> json(AdminAccounts.AdminView v) {
    Account a = v.account();
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("userId", a.userId());
    m.put("status", a.status().name().toLowerCase(Locale.ROOT));
    m.put("username", a.username());
    m.put("displayName", a.displayName());
    m.put("email", a.email());
    m.put("phone", a.phone());
    m.put("mustChangePassword", a.mustChangePassword());
    m.put("createdAt", a.createdAt().toString());
    m.put("deletedAt", a.deletedAt() == null ? null : a.deletedAt().toString());
    m.put("liveSessions", v.liveSessions());
    m.put("actions", v.actions().stream().map(e -> Map.of("actor", e.actor(), "action", e.action(),
        "reason", e.reason(), "at", e.at().toString())).toList());
    return m;
  }

  static String reason(Map<String, Object> body) {
    return body == null ? null : Bodies.string(body, "reason");
  }

  @GetMapping("/admin/v1/accounts/{userId}")
  public Map<String, Object> get(@PathVariable("userId") String userId) {
    return json(admin.get(userId));
  }

  @GetMapping("/admin/v1/accounts")
  public Map<String, Object> find(@RequestParam(name = "username", required = false) String username,
      @RequestParam(name = "email", required = false) String email,
      @RequestParam(name = "phone", required = false) String phone) {
    if (username != null) {
      return json(admin.find(LoginKind.USERNAME, username));
    }
    if (email != null) {
      return json(admin.find(LoginKind.EMAIL, email));
    }
    if (phone != null) {
      return json(admin.find(LoginKind.PHONE, phone));
    }
    throw new DomainException("invalid_request", "one of username, email, phone is required");
  }

  @PostMapping("/admin/v1/accounts/{userId}:lock")
  public ResponseEntity<Void> lock(@PathVariable("userId") String userId,
      @RequestBody(required = false) Map<String, Object> body, HttpServletRequest req) {
    admin.lock(actor(req), userId, reason(body));
    return ResponseEntity.noContent().build();
  }

  @PostMapping("/admin/v1/accounts/{userId}:unlock")
  public ResponseEntity<Void> unlock(@PathVariable("userId") String userId,
      @RequestBody(required = false) Map<String, Object> body, HttpServletRequest req) {
    admin.unlock(actor(req), userId, reason(body));
    return ResponseEntity.noContent().build();
  }

  @PostMapping("/admin/v1/accounts/{userId}:reset-password")
  public Map<String, Object> reset(@PathVariable("userId") String userId,
      @RequestBody(required = false) Map<String, Object> body, HttpServletRequest req) {
    return Map.of("temporaryPassword", admin.resetPassword(actor(req), userId, reason(body)));
  }

  @DeleteMapping("/admin/v1/accounts/{userId}")
  public ResponseEntity<Void> delete(@PathVariable("userId") String userId,
      @RequestBody(required = false) Map<String, Object> body, HttpServletRequest req) {
    admin.delete(actor(req), userId, reason(body));
    return ResponseEntity.noContent().build();
  }
}
