package com.pangate.account.api;

import static com.pangate.account.api.Bodies.chars;
import static com.pangate.account.api.Bodies.required;
import static com.pangate.account.api.Bodies.string;

import com.pangate.account.application.NewSession;
import com.pangate.account.application.Register;
import com.pangate.account.domain.DeviceInfo;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Map;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/** {@code POST /v1/accounts} (account spec §4.2). */
@RestController
public class AccountsController {

  private final Register register;

  public AccountsController(Register register) {
    this.register = register;
  }

  static Map<String, Object> session(NewSession s) {
    return Map.of("userId", s.userId(), "deviceId", s.deviceId(), "refreshToken", s.refreshToken());
  }

  @PostMapping(path = "/v1/accounts", consumes = "application/json")
  public ResponseEntity<Map<String, Object>> register(@RequestBody Map<String, Object> body, HttpServletRequest req) {
    DeviceInfo d = DeviceInfo.of(string(body, "deviceName"), string(body, "platform"));
    NewSession s = register.register(required(body, "username"), chars(required(body, "password")),
        string(body, "email"), string(body, "phone"), string(body, "displayName"), d, ClientIp.of(req));
    return ResponseEntity.status(201).body(session(s));
  }
}
