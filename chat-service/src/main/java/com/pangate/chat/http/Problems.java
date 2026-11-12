package com.pangate.chat.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;

/**
 * RFC 9457 problem documents with a stable {@code code} (chat spec §7): invalid_request, not_found, forbidden,
 * rate_limited, unavailable, invalid_token.
 */
public final class Problems {

  public static final String MEDIA_TYPE = "application/problem+json";
  private static final ObjectMapper JSON = new ObjectMapper();

  private Problems() {}

  public static ProblemDetail of(int status, String code, String detail) {
    ProblemDetail p = ProblemDetail.forStatusAndDetail(HttpStatus.valueOf(status), detail);
    p.setType(java.net.URI.create("urn:pangate:problem:" + code));
    p.setProperty("code", code);
    return p;
  }

  /** For filters, which run outside Spring MVC's exception handling. */
  public static void write(HttpServletResponse res, int status, String code, String detail) throws IOException {
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("type", "urn:pangate:problem:" + code);
    body.put("title", HttpStatus.valueOf(status).getReasonPhrase());
    body.put("status", status);
    body.put("detail", detail);
    body.put("code", code);
    res.setStatus(status);
    res.setContentType(MEDIA_TYPE);
    res.setCharacterEncoding("UTF-8");
    JSON.writeValue(res.getOutputStream(), body);
  }
}
