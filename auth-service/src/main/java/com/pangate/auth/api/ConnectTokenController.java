package com.pangate.auth.api;

import com.pangate.auth.application.MintConnectToken;
import com.pangate.auth.domain.DomainException;
import com.pangate.auth.infrastructure.AuthMetrics;
import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;
import org.springframework.http.CacheControl;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/** {@code POST /v1/tokens} (scope {@code tokens:mint}). */
@RestController
public class ConnectTokenController {

  public record Body(String principal, String device, List<String> topics, Integer ttlSec) {}

  private final MintConnectToken mint;
  private final AuthMetrics metrics;

  public ConnectTokenController(MintConnectToken mint, AuthMetrics metrics) {
    this.mint = mint;
    this.metrics = metrics;
  }

  @PostMapping(value = "/v1/tokens", consumes = MediaType.APPLICATION_JSON_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  ResponseEntity<Map<String, String>> mint(HttpServletRequest req, @RequestBody Body b) {
    long start = System.nanoTime();
    MintConnectToken.MintedToken t;
    try {
      t = mint.mint(ApiPrincipal.of(req), b.principal(), b.device(), b.topics(), b.ttlSec());
    } catch (DomainException e) {
      metrics.issued("connect", e.code());
      throw e;
    }
    metrics.issued("connect", "ok");
    metrics.latency("connect", start);
    return ResponseEntity.ok().cacheControl(CacheControl.noStore())
        .body(Map.of("token", t.token(), "expiresAt", t.expiresAt().toString()));
  }
}
