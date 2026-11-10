package com.pangate.auth.api;

import com.pangate.auth.application.IssueClientToken;
import com.pangate.auth.application.OAuthError;
import jakarta.servlet.http.HttpServletRequest;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

/** {@code POST /oauth2/token}, client credentials only (RFC 6749 §4.4, spec §4.1). */
@RestController
public class TokenEndpoint {

  private final IssueClientToken issue;

  public TokenEndpoint(IssueClientToken issue) {
    this.issue = issue;
  }

  private record Credentials(String id, String secret) {}

  @PostMapping(value = "/oauth2/token", consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  ResponseEntity<Map<String, Object>> token(HttpServletRequest req) {
    Credentials cred = credentials(req);
    String grant = req.getParameter("grant_type");
    if (grant == null) {
      throw new OAuthError("invalid_request", 400, "grant_type is required");
    }
    if (!"client_credentials".equals(grant)) {
      throw new OAuthError("unsupported_grant_type", 400, "only client_credentials is supported");
    }
    var t = issue.issue(cred.id(), cred.secret().toCharArray(), req.getParameter("scope"));
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("access_token", t.accessToken());
    body.put("token_type", "Bearer");
    body.put("expires_in", t.expiresIn());
    body.put("scope", t.scope());
    return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(body);
  }

  /** Exactly one of HTTP Basic (form-urlencoded parts, RFC 6749 §2.3.1) or form parameters. */
  static Credentials credentials(HttpServletRequest req) {
    String h = req.getHeader(HttpHeaders.AUTHORIZATION);
    boolean basic = h != null && h.regionMatches(true, 0, "Basic ", 0, 6);
    String formId = req.getParameter("client_id");
    String formSecret = req.getParameter("client_secret");
    boolean form = formId != null || formSecret != null;
    if (basic == form) {
      throw new OAuthError("invalid_request", 400, "use exactly one of HTTP Basic or client_id/client_secret");
    }
    if (form) {
      if (formId == null || formSecret == null) {
        throw new OAuthError("invalid_request", 400, "client_id and client_secret are both required");
      }
      return new Credentials(formId, formSecret);
    }
    try {
      String decoded = new String(Base64.getDecoder().decode(h.substring(6).trim()), StandardCharsets.UTF_8);
      int colon = decoded.indexOf(':');
      if (colon < 0) {
        throw OAuthError.invalidClient();
      }
      return new Credentials(URLDecoder.decode(decoded.substring(0, colon), StandardCharsets.UTF_8),
          URLDecoder.decode(decoded.substring(colon + 1), StandardCharsets.UTF_8));
    } catch (IllegalArgumentException e) {
      throw OAuthError.invalidClient();
    }
  }

  @ExceptionHandler(OAuthError.class)
  ResponseEntity<Map<String, String>> error(OAuthError e) {
    var b = ResponseEntity.status(e.status()).cacheControl(CacheControl.noStore())
        .contentType(MediaType.APPLICATION_JSON);
    if (e.status() == 401) {
      b.header(HttpHeaders.WWW_AUTHENTICATE, "Basic realm=\"pangate\"");
    }
    return b.body(Map.of("error", e.error(), "error_description", e.getMessage()));
  }
}
