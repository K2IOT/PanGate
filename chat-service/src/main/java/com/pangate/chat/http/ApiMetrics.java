package com.pangate.chat.http;

import io.micrometer.core.instrument.MeterRegistry;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.HandlerMapping;

/** {@code chat.api.requests{route, code}} for every request that reached a controller. */
public final class ApiMetrics implements HandlerInterceptor {

  private final MeterRegistry meters;

  public ApiMetrics(MeterRegistry meters) {
    this.meters = meters;
  }

  @Override
  public void afterCompletion(HttpServletRequest req, HttpServletResponse res, Object handler, Exception ex) {
    Object route = req.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE);
    if (route != null) {
      meters.counter("chat.api.requests", "route", req.getMethod() + " " + route, "code",
          String.valueOf(res.getStatus())).increment();
    }
  }
}
