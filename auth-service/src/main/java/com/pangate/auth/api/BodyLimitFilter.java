package com.pangate.auth.api;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Request bodies above 16 KB are refused with 413 (spec §4). Requests without Content-Length (chunked) are not
 * buffered here; Tomcat's max-swallow/post limits and the JSON parser bound them.
 */
public class BodyLimitFilter extends OncePerRequestFilter {

  public static final long MAX_BODY = 16 * 1024;

  @Override
  protected void doFilterInternal(HttpServletRequest req, HttpServletResponse res, FilterChain chain)
      throws ServletException, IOException {
    if (req.getContentLengthLong() > MAX_BODY) {
      Problems.write(res, 413, "payload_too_large", "request body exceeds 16 KB");
      return;
    }
    chain.doFilter(req, res);
  }
}
