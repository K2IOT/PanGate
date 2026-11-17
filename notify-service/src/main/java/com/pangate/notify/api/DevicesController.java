package com.pangate.notify.api;

import com.pangate.notify.application.Registrations;
import com.pangate.notify.domain.DomainException;
import com.pangate.notify.domain.Platform;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/** Offline notifications spec §5.1: the calling device sets or removes its push token. */
@RestController
public class DevicesController {

  /** {@code environment} is for APNs only; absent means production. */
  public record PushBody(String platform, String token, String environment) {}

  private final Registrations registrations;

  public DevicesController(Registrations registrations) {
    this.registrations = registrations;
  }

  @PutMapping(path = "/v1/devices/self/push", consumes = "application/json")
  public ResponseEntity<Void> register(@RequestBody PushBody body, HttpServletRequest req) {
    if (body == null) {
      throw new DomainException("invalid_request", "a JSON object is required");
    }
    registrations.register(ConnectAuthFilter.caller(req), Platform.parse(body.platform()), body.token(),
        body.environment());
    return ResponseEntity.noContent().build();
  }

  @DeleteMapping("/v1/devices/self/push")
  public ResponseEntity<Void> unregister(HttpServletRequest req) {
    registrations.unregister(ConnectAuthFilter.caller(req));
    return ResponseEntity.noContent().build();
  }
}
