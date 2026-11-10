package com.pangate.pushapi.http;

import com.pangate.contracts.v1.PushRequest;
import com.pangate.contracts.v1.PushResponse;
import com.pangate.pushapi.core.BadRequestException;
import com.pangate.pushapi.core.Caller;
import com.pangate.pushapi.core.HttpMapping;
import com.pangate.pushapi.core.KeyedSerialExecutor;
import com.pangate.pushapi.core.MessageStatus;
import com.pangate.pushapi.core.PushAcceptor;
import com.pangate.pushapi.core.RateLimiter;
import com.pangate.pushapi.core.StoreUnavailableException;
import com.pangate.pushapi.core.TenantPolicy;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

/** {@code POST /v1/push}, {@code POST /v1/push:batch}, {@code GET /v1/messages/{msgId}} (push-api spec §2, §4.2). */
@RestController
public class PushController {

  private final PushAcceptor acceptor;
  private final KeyedSerialExecutor serial;
  private final MessageStatus status;
  private final RateLimiter statusLimiter;
  private final int statusRate;
  private final int maxBatch;
  private final MeterRegistry meters;

  public PushController(PushAcceptor acceptor, KeyedSerialExecutor serial, MessageStatus status,
      com.pangate.pushapi.config.PushApiProperties props, MeterRegistry meters) {
    this.acceptor = acceptor;
    this.serial = serial;
    this.status = status;
    this.statusLimiter = new RateLimiter(System::nanoTime);
    this.statusRate = props.limits().statusRate();
    this.maxBatch = props.http().maxBatch();
    this.meters = meters;
  }

  static Caller.ApiCaller caller(HttpServletRequest req) {
    return (Caller.ApiCaller) req.getAttribute(ApiAuthFilter.CALLER);
  }

  @PostMapping(path = "/v1/push", consumes = "application/json")
  public ResponseEntity<?> push(@RequestBody Map<String, Object> body,
      @RequestHeader(name = "Idempotency-Key", required = false) String idempotencyKey, HttpServletRequest req) {
    Caller.ApiCaller c = caller(req);
    PushResponse r = acceptor.accept(c, HttpMapping.toRequest(body, c.tenant(), idempotencyKey));
    count(r);
    return toHttp(r);
  }

  @PostMapping(path = "/v1/push:batch", consumes = "application/json")
  public ResponseEntity<?> batch(@RequestBody Map<String, Object> body, HttpServletRequest req) {
    Caller.ApiCaller c = caller(req);
    if (!(body.get("items") instanceof List<?> items) || items.isEmpty()) {
      throw new BadRequestException("invalid_request", "items must be a non-empty array");
    }
    if (items.size() > maxBatch) {
      throw new BadRequestException("batch_too_large", "at most " + maxBatch + " items");
    }
    List<CompletableFuture<Map<String, Object>>> results = new ArrayList<>();
    for (int i = 0; i < items.size(); i++) {
      int index = i;
      if (!(items.get(i) instanceof Map<?, ?> raw)) {
        results.add(CompletableFuture.completedFuture(itemError(index, 400, "invalid_request", "item must be an object")));
        continue;
      }
      @SuppressWarnings("unchecked")
      Map<String, Object> item = (Map<String, Object>) raw;
      PushRequest r;
      try {
        Object k = item.get("idempotencyKey");
        r = HttpMapping.toRequest(item, c.tenant(), k instanceof String s ? s : null);
      } catch (BadRequestException e) {
        results.add(CompletableFuture.completedFuture(itemError(index, 400, e.code(), e.getMessage())));
        continue;
      }
      String key = r.getKind() == PushRequest.Kind.KIND_BROADCAST ? "topic:" + r.getTopic() : r.getPrincipalId();
      results.add(serial.submit(key, () -> {
        PushResponse res = acceptor.accept(c, r);
        count(res);
        return item(index, res);
      }));
    }
    List<Map<String, Object>> out = new ArrayList<>(results.size());
    for (CompletableFuture<Map<String, Object>> f : results) {
      out.add(f.join());
    }
    return ResponseEntity.ok(Map.of("results", out));
  }

  @GetMapping("/v1/messages/{msgId}")
  public ResponseEntity<?> message(@PathVariable String msgId, HttpServletRequest req) {
    Caller.ApiCaller c = caller(req);
    if (!c.scopes().contains("push:send") && !c.scopes().contains("push:broadcast")) {
      return problem(403, "forbidden", "scope push:send or push:broadcast required", 0);
    }
    long wait = statusLimiter.tryAcquire("status:" + c.tenant(), PushRequest.Kind.KIND_MESSAGE,
        new TenantPolicy(false, true, statusRate, statusRate, 0));
    if (wait > 0) {
      return problem(429, "rate_limited", "too many status reads", (int) Math.max(1, wait / 1_000_000));
    }
    Optional<Map<String, String>> m;
    try {
      m = status.get(c.tenant(), msgId);
    } catch (StoreUnavailableException e) {
      return problem(503, "unavailable", "state store unavailable", 200);
    }
    if (m.isEmpty()) {
      return problem(404, "not_found", "unknown or expired message", 0);
    }
    return ResponseEntity.ok(statusBody(msgId, m.get()));
  }

  static Map<String, Object> statusBody(String msgId, Map<String, String> h) {
    Map<String, Object> b = new LinkedHashMap<>();
    b.put("msgId", msgId);
    b.put("state", h.get("state"));
    b.put("kind", h.get("kind"));
    b.put("class", h.get("class"));
    b.put("receipt", Boolean.parseBoolean(h.get("receipt")));
    putLong(b, "acceptedAt", h.get("acceptedAt"));
    putLong(b, "expiresAt", h.get("expiresAt"));
    putLong(b, "deliveredConnections", h.get("deliveredConns"));
    if (h.get("lastStatus") != null) {
      b.put("lastStatus", h.get("lastStatus"));
    }
    if (h.get("offlineReason") != null) {
      b.put("offlineReason", h.get("offlineReason"));
    }
    putLong(b, "attempts", h.get("attempts"));
    return b;
  }

  private static void putLong(Map<String, Object> b, String name, String v) {
    if (v != null) {
      b.put(name, Long.parseLong(v));
    }
  }

  private void count(PushResponse r) {
    meters.counter("pushapi.requests", "surface", "http", "code", r.getCode().name()).increment();
  }

  static int httpStatus(PushResponse.Code c) {
    return switch (c) {
      case CODE_OK -> 202;
      case CODE_INVALID -> 400;
      case CODE_FORBIDDEN -> 403;
      case CODE_RATE_LIMITED, CODE_QUOTA_EXCEEDED -> 429;
      default -> 503;
    };
  }

  /** {@code error} is "<code>: detail" or "V<n>: detail" (the latter → {@code rule_V<n>}). */
  static String problemCode(PushResponse r) {
    String e = r.getError();
    int colon = e.indexOf(':');
    String head = colon > 0 ? e.substring(0, colon) : e;
    if (r.getCode() == PushResponse.Code.CODE_INVALID && head.matches("V\\d+")) {
      return "rule_" + head;
    }
    return head.isEmpty() ? r.getCode().name().substring(5).toLowerCase(java.util.Locale.ROOT) : head;
  }

  private static ResponseEntity<?> toHttp(PushResponse r) {
    if (r.getCode() == PushResponse.Code.CODE_OK) {
      return ResponseEntity.status(202).body(HttpMapping.toBody(r));
    }
    return problem(httpStatus(r.getCode()), problemCode(r), r.getError(), r.getRetryAfterMs());
  }

  private static ResponseEntity<?> problem(int status, String code, String detail, int retryAfterMs) {
    ResponseEntity.BodyBuilder b = ResponseEntity.status(status).contentType(
        org.springframework.http.MediaType.parseMediaType(Problems.MEDIA_TYPE));
    if (retryAfterMs > 0) {
      b.header(HttpHeaders.RETRY_AFTER, Long.toString((retryAfterMs + 999) / 1000));
    }
    return b.body(Problems.of(status, code, detail));
  }

  private static Map<String, Object> item(int index, PushResponse r) {
    if (r.getCode() != PushResponse.Code.CODE_OK) {
      Map<String, Object> m = itemError(index, httpStatus(r.getCode()), problemCode(r), r.getError());
      if (r.getRetryAfterMs() > 0) {
        m.put("retryAfterMs", r.getRetryAfterMs());
      }
      return m;
    }
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("index", index);
    m.put("status", 202);
    m.putAll(HttpMapping.toBody(r));
    return m;
  }

  private static Map<String, Object> itemError(int index, int status, String code, String error) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("index", index);
    m.put("status", status);
    m.put("code", code);
    m.put("error", error);
    return m;
  }
}
