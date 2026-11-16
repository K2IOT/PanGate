package com.pangate.account.api;

import static com.pangate.account.api.Bodies.chars;
import static com.pangate.account.api.Bodies.required;
import static com.pangate.account.api.Bodies.string;

import com.pangate.account.application.LoginUseCase;
import com.pangate.account.domain.DeviceInfo;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Map;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/** {@code /v1/sessions} (account spec §4.2). */
@RestController
public class SessionsController {

  private final LoginUseCase login;

  public SessionsController(LoginUseCase login) {
    this.login = login;
  }

  @PostMapping(path = "/v1/sessions", consumes = "application/json")
  public ResponseEntity<Map<String, Object>> login(@RequestBody Map<String, Object> body, HttpServletRequest req) {
    DeviceInfo d = DeviceInfo.of(string(body, "deviceName"), string(body, "platform"));
    return ResponseEntity.status(201).body(AccountsController.session(login.login(required(body, "login"),
        chars(required(body, "password")), chars(string(body, "newPassword")), d, ClientIp.of(req))));
  }
}
