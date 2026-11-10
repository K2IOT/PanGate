package com.pangate.auth.api;

import com.pangate.auth.application.MintConnectToken;
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

  public ConnectTokenController(MintConnectToken mint) {
    this.mint = mint;
  }

  @PostMapping(value = "/v1/tokens", consumes = MediaType.APPLICATION_JSON_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  ResponseEntity<Map<String, String>> mint(HttpServletRequest req, @RequestBody Body b) {
    var t = mint.mint(ApiPrincipal.of(req), b.principal(), b.device(), b.topics(), b.ttlSec());
    return ResponseEntity.ok().cacheControl(CacheControl.noStore())
        .body(Map.of("token", t.token(), "expiresAt", t.expiresAt().toString()));
  }
}
