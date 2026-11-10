package com.pangate.auth.api;

import com.pangate.auth.application.KeyRing;
import org.springframework.http.CacheControl;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** {@code GET /.well-known/jwks.json} from memory, never from the database (spec §3). */
@RestController
public class JwksController {

  private final KeyRing keys;

  public JwksController(KeyRing keys) {
    this.keys = keys;
  }

  @GetMapping(value = "/.well-known/jwks.json", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> jwks() {
    return ResponseEntity.ok().cacheControl(CacheControl.maxAge(java.time.Duration.ofSeconds(300)).cachePublic())
        .body(keys.jwksJson());
  }
}
