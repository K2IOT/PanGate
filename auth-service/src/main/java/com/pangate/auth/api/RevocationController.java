package com.pangate.auth.api;

import com.pangate.auth.application.Revoke;
import com.pangate.auth.domain.DomainException;
import com.pangate.auth.domain.Revocation;
import com.pangate.auth.infrastructure.AuthMetrics;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/** {@code POST /v1/revocations} (scope {@code sessions:revoke}, spec §4.4). */
@RestController
public class RevocationController {

  public record Body(String kind, String principal, String device, String jti) {}

  private final Revoke revoke;
  private final AuthMetrics metrics;

  public RevocationController(Revoke revoke, AuthMetrics metrics) {
    this.revoke = revoke;
    this.metrics = metrics;
  }

  @PostMapping(value = "/v1/revocations", consumes = MediaType.APPLICATION_JSON_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  ResponseEntity<Map<String, Object>> revoke(HttpServletRequest req, @RequestBody Body b) {
    var caller = ApiPrincipal.of(req);
    Revocation r = switch (b.kind() == null ? "" : b.kind()) {
      case "principal" -> revoke.principal(caller, b.principal());
      case "device" -> revoke.device(caller, b.principal(), b.device());
      case "jti" -> revoke.jti(caller, b.jti());
      default -> throw DomainException.badRequest("kind_invalid", "kind must be principal, device or jti");
    };
    metrics.revocation(r.kind().db());
    return ResponseEntity.status(HttpStatus.ACCEPTED).body(Map.of("id", r.id(), "controlKey", r.controlKey(),
        "expiresAt", r.expiresAt().toString()));
  }
}
