package com.pangate.account.api;

import static com.pangate.account.api.Bodies.chars;
import static com.pangate.account.api.Bodies.required;
import static com.pangate.account.api.Bodies.string;

import com.pangate.account.application.Caller;
import com.pangate.account.application.ExchangeToken;
import com.pangate.account.application.LoginUseCase;
import com.pangate.account.application.ManageSessions;
import com.pangate.account.application.port.AuthApi;
import com.pangate.account.domain.DeviceInfo;
import com.pangate.account.domain.DomainException;
import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** {@code /v1/sessions*} (account spec §4.2). */
@RestController
public class SessionsController {

  private final LoginUseCase login;
  private final ExchangeToken exchange;
  private final ManageSessions sessions;
  private final ConnectAuthFilter auth;

  public SessionsController(LoginUseCase login, ExchangeToken exchange, ManageSessions sessions,
      ConnectAuthFilter auth) {
    this.login = login;
    this.exchange = exchange;
    this.sessions = sessions;
    this.auth = auth;
  }

  @PostMapping(path = "/v1/sessions", consumes = "application/json")
  public ResponseEntity<Map<String, Object>> login(@RequestBody Map<String, Object> body, HttpServletRequest req) {
    DeviceInfo d = DeviceInfo.of(string(body, "deviceName"), string(body, "platform"));
    return ResponseEntity.status(201).body(AccountsController.session(login.login(required(body, "login"),
        chars(required(body, "password")), chars(string(body, "newPassword")), d, ClientIp.of(req))));
  }

  /** {@code Authorization: Bearer rt_…}. */
  @PostMapping("/v1/sessions/token")
  public Map<String, Object> token(@RequestHeader(name = "Authorization", required = false) String h) {
    if (h == null || !h.regionMatches(true, 0, "Bearer ", 0, 7)) {
      throw new DomainException("invalid_session", "a Bearer refresh token is required");
    }
    AuthApi.ConnectToken t = exchange.exchange(h.substring(7).trim());
    return Map.of("connectToken", t.token(), "expiresAt", t.expiresAt().toString());
  }

  @GetMapping("/v1/sessions")
  public Map<String, List<ManageSessions.SessionView>> list(HttpServletRequest req) {
    return Map.of("sessions", sessions.list(ConnectAuthFilter.caller(req)));
  }

  @DeleteMapping("/v1/sessions/{deviceId}")
  public ResponseEntity<Void> logout(@PathVariable("deviceId") String deviceId, HttpServletRequest req) {
    sessions.logout(ConnectAuthFilter.caller(req), deviceId);
    auth.forget(deviceId);
    return ResponseEntity.noContent().build();
  }

  @DeleteMapping("/v1/sessions")
  public ResponseEntity<Void> logoutAll(@RequestParam(name = "others", defaultValue = "false") boolean others,
      HttpServletRequest req) {
    Caller c = ConnectAuthFilter.caller(req);
    sessions.logoutAll(c.userId(), others ? c.deviceId() : null).forEach(auth::forget);
    return ResponseEntity.noContent().build();
  }
}
