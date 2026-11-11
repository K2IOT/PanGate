package com.pangate.auth.api;

import com.pangate.auth.config.AdminConnector;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.springframework.web.filter.OncePerRequestFilter;

/** Public paths only on the public port, admin and actuator paths only on the management port (spec §1, §7). */
public class PortRoutingFilter extends OncePerRequestFilter {

  private final AdminConnector admin;

  public PortRoutingFilter(AdminConnector admin) {
    this.admin = admin;
  }

  static boolean isAdminPath(String p) {
    return p.startsWith("/admin/v1/") || p.startsWith("/actuator/") || p.equals("/actuator") || p.equals("/ready")
        || p.equals("/live");
  }

  static boolean isPublicPath(String p) {
    return p.equals("/oauth2/token") || p.startsWith("/v1/") || p.equals("/.well-known/jwks.json");
  }

  @Override
  protected void doFilterInternal(HttpServletRequest req, HttpServletResponse res, FilterChain chain)
      throws ServletException, IOException {
    String path = req.getRequestURI().substring(req.getContextPath().length());
    boolean onAdmin = req.getLocalPort() == admin.port();
    if (onAdmin ? isAdminPath(path) : isPublicPath(path)) {
      chain.doFilter(req, res);
    } else {
      Problems.write(res, 404, "not_found", "no such endpoint");
    }
  }
}
