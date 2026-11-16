package com.pangate.account.api;

import static com.pangate.account.api.Bodies.chars;
import static com.pangate.account.api.Bodies.required;
import static com.pangate.account.api.Bodies.string;

import com.pangate.account.application.ChangeIdentifier;
import com.pangate.account.application.ChangePassword;
import com.pangate.account.application.Profiles;
import com.pangate.account.domain.LoginKind;
import jakarta.servlet.http.HttpServletRequest;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/** {@code /v1/me*} (account spec §4.2). */
@RestController
public class MeController {

  private final Profiles profiles;
  private final ChangePassword password;
  private final ChangeIdentifier identifier;

  public MeController(Profiles profiles, ChangePassword password, ChangeIdentifier identifier) {
    this.profiles = profiles;
    this.password = password;
    this.identifier = identifier;
  }

  @GetMapping("/v1/me")
  public Map<String, Object> me(HttpServletRequest req) {
    Profiles.MeView m = profiles.me(ConnectAuthFilter.caller(req));
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("userId", m.userId());
    out.put("username", m.username());
    out.put("displayName", m.displayName());
    out.put("bio", m.bio());
    out.put("email", m.email());
    out.put("emailVerified", false);
    out.put("phone", m.phone());
    out.put("phoneVerified", false);
    out.put("createdAt", m.createdAt().toString());
    return out;
  }

  @PatchMapping(path = "/v1/me", consumes = "application/json")
  public ResponseEntity<Void> update(@RequestBody Map<String, Object> body, HttpServletRequest req) {
    profiles.update(ConnectAuthFilter.caller(req), string(body, "displayName"), string(body, "bio"));
    return ResponseEntity.noContent().build();
  }

  @PutMapping(path = "/v1/me/username", consumes = "application/json")
  public ResponseEntity<Void> username(@RequestBody Map<String, Object> body, HttpServletRequest req) {
    profiles.changeUsername(ConnectAuthFilter.caller(req), required(body, "username"));
    return ResponseEntity.noContent().build();
  }

  @PutMapping(path = "/v1/me/email", consumes = "application/json")
  public ResponseEntity<Void> email(@RequestBody Map<String, Object> body, HttpServletRequest req) {
    identifier.set(ConnectAuthFilter.caller(req), LoginKind.EMAIL, string(body, "value"),
        chars(required(body, "password")));
    return ResponseEntity.noContent().build();
  }

  @PutMapping(path = "/v1/me/phone", consumes = "application/json")
  public ResponseEntity<Void> phone(@RequestBody Map<String, Object> body, HttpServletRequest req) {
    identifier.set(ConnectAuthFilter.caller(req), LoginKind.PHONE, string(body, "value"),
        chars(required(body, "password")));
    return ResponseEntity.noContent().build();
  }

  @PostMapping(path = "/v1/me/password", consumes = "application/json")
  public ResponseEntity<Void> password(@RequestBody Map<String, Object> body, HttpServletRequest req) {
    password.change(ConnectAuthFilter.caller(req), chars(required(body, "currentPassword")),
        chars(required(body, "newPassword")));
    return ResponseEntity.noContent().build();
  }
}
