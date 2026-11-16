package com.pangate.account.api;

import jakarta.servlet.http.HttpServletRequest;

/** The caller's address: Tomcat's RemoteIpValve already resolved X-Forwarded-For from trusted proxies. */
public final class ClientIp {

  private ClientIp() {}

  public static String of(HttpServletRequest req) {
    return req.getRemoteAddr();
  }
}
